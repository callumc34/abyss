# ADP-004: Cold Consumer and Compaction Buffer

**Status:** Accepted
**Created:** 2026-04-09

## Context

The cold consumer is the most architecturally significant component in Abyss. It reads from the queue, maintains an in-memory compaction buffer, and flushes to the cold store only when it is smart to do so.

The key insight: if a key has an `eviction` of 4 hours, we have up to 3.99 hours before we need to persist it to cold. During that window, the key may be updated hundreds of times. Writing every intermediate state to disk is wasteful. Instead, the cold consumer searches for windows where keys are not being actively written, then flushes. If no quiet window appears, it flushes before the eviction deadline as a safety net.

## Design

### Data Flow

The cold consumer does not read from the raw queue and write directly to cold. It interposes a compaction buffer:

```
Raw Queue ──▶ Cold Consumer ──▶ Compaction Buffer ──▶ (flush decisions) ──▶ Cold Store
```

### Compaction Buffer

Each entry in the compaction buffer tracks the key, its compacted state, the time it first entered the buffer, the time of its last modification, and a count of absorbed writes. This provides the flush strategy with everything it needs to decide when to persist.

The buffer is protected by a shared mutex. The cold consumer thread holds an exclusive lock when absorbing new entries or removing flushed entries. I/O threads performing reads hold a shared lock. This allows concurrent buffer reads with minimal contention.

The buffer is also part of the read path: reads that miss the hot store check the compaction buffer before falling through to cold. See [ADP-006](006-read-write-paths.md).

### Compaction Semantics

Different Redis data structures require different merge strategies:

**Scalar commands (SET, DEL, EXPIRE):** Last-write-wins. A `SET` replaces any prior state. A `DEL` cancels all preceding writes (tombstone). A `SET` after `DEL` replaces the tombstone.

**Set commands (SADD, SREM):** Merge-accumulate. Consecutive `SADD` commands merge members. `SREM` cancels specific previously-added members. The buffer maintains net-add and net-remove sets. On flush, it emits a single `SADD` for net additions and a single `SREM` for net removals.

**Sorted set commands (ZADD, ZREM, ZREMRANGEBYSCORE):** Same merge-accumulate approach. `ZADD` entries merge (latest score wins for duplicate members). `ZREM` cancels specific members. `ZREMRANGEBYSCORE` applies against accumulated entries by score range.

**A `DEL` for any key type resets the buffer entry entirely.** All accumulated state is discarded and replaced with a tombstone.

`CompactedState` is type-aware: it tracks which data type the key holds (string, hash, set, sorted set, or none) and maintains per-type storage internally. Absorption takes a typed write operation, not a raw RESP command. Emission produces the minimal set of typed write operations needed to materialise the compacted state in cold. Type conflicts (e.g. SADD on a string key) are handled per Redis semantics — SET and DEL override any type, other commands require a matching type or are skipped.

See `include/abyss/consumer/compacted_state.h` for the current interface.

### Flush Strategy

Each key has two potential flush triggers:

1. **Quiet window:** The key hasn't been modified for `quiet_threshold`. The burst of writes has likely ended — flush now avoids a rewrite later.

2. **Eviction deadline:** `first_seen + eviction - safety_margin` is approaching. The key MUST be flushed before it expires from hot, regardless of write activity.

A priority queue orders buffer entries by the earlier of these two deadlines, with jitter applied to the deadline flush to prevent thundering herds:

```
flush_priority = min(
    last_modified + quiet_threshold,
    first_seen + eviction - safety_margin - jitter
)
```

Where `jitter` is a per-key random offset in the range `[0, safety_margin * 0.5]`, computed once when the entry enters the buffer and stable across priority queue reorderings.

### Consumer Thread Loop

```
loop:
    1. Drain new entries from queue into compaction buffer
       (non-blocking, process whatever is available)

    2. Peek at priority queue head
       - If flush_priority is in the past → flush it
       - Pop entry, call cold_store.apply_batch(entry.emit())
       - On success: remove from buffer, ack queue
       - On failure: retry with backoff, emit alert

    3. If nothing to flush, sleep until next flush_priority
       or until new queue entries arrive
```

**Post-flush:** When flushed, the entry is removed from the buffer. If a new write arrives for the same key later, it re-enters with a fresh `first_seen`.

**Absolute TTL interaction:** If a key's absolute `ttl` has expired by flush time, the entry is dropped without writing to cold.

### Memory Management

The buffer is bounded by unique keys in the eviction window. Under normal operation this is manageable. If buffer memory exceeds a configurable high-water mark, the cold consumer switches to aggressive mode — flushing the oldest entries by deadline order regardless of quiet window. This sacrifices write efficiency for memory stability.

### Lag Monitoring

The most important metric is the age of the oldest un-flushed buffer entry, which directly indicates cold gap risk:

```
oldest_unflushed_age = now - min(entry.first_seen for all buffer entries)

WARN if oldest_unflushed_age > (default_eviction * 0.8)
CRIT if oldest_unflushed_age > default_eviction
```

Additionally:

```
cold_consumer_queue_lag = hot_consumer_seq - cold_consumer_seq
```

Where `cold_consumer_seq` reflects the latest entry read into the buffer, not the latest entry flushed to cold.

### Configuration

```yaml
cold_consumer:
  quiet_threshold_seconds: 30
  safety_margin_seconds: 300
  deadline_jitter_ratio: 0.5
  buffer_high_water_bytes: 536870912  # 512 MiB
  max_flush_batch_size: 10000
```

## Invariants

1. Every key in the compaction buffer has a bounded lifetime: it will be flushed before `first_seen + eviction`.
2. The buffer never contains stale data — `Absorb` is the only write path, and it merges correctly per data structure type.
3. A `DEL` resets all accumulated state for a key. No prior writes survive a tombstone.
4. Flush is idempotent from cold's perspective. If the cold consumer crashes mid-flush and replays, the same compacted state is re-emitted and applied. Last-write-wins in the cold store handles duplicates.
5. Buffer reads do not promote. The cold consumer owns data in the buffer and will flush it on its own schedule. See [ADP-006](006-read-write-paths.md) for promotion semantics.

## Trade-offs

**Why buffer in memory instead of writing every entry to cold?** Write amplification. A hot key updated 1000 times in a minute would generate 1000 disk writes. The compaction buffer collapses this to one. The success metric is the ratio of quiet-window flushes to deadline flushes — a majority of quiet-window flushes means the buffer is absorbing bursts effectively.

**Why two flush triggers instead of just the deadline?** If we only flushed at the deadline, we'd hold entries for the entire eviction window even if the key went quiet after 5 seconds. This wastes buffer memory and increases the blast radius of a crash (more un-flushed data). The quiet window trigger releases entries as soon as they're likely done being written, keeping the buffer lean.

**Why jitter on the deadline flush?** Without jitter, keys that enter the buffer at similar times would all hit their deadlines simultaneously, causing a thundering herd of flushes. The jitter spreads deadline flushes across `safety_margin * 0.5` seconds.

**Why `shared_mutex` instead of a concurrent map?** The compaction buffer has asymmetric access: one writer (cold consumer thread) doing absorb/remove, many readers (I/O threads) doing lookups. A `shared_mutex` with shared read locks and exclusive write locks matches this pattern well. A concurrent map would be over-engineered given that the cold consumer is a single thread.
