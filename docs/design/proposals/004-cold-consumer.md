# ADP-004: Cold Consumer and Compaction Buffer

**Status:** Accepted
**Created:** 2026-04-09

> **Amended by [ADP-015](015-write-path-and-durability.md).** The consumer loop reads by its own position, so the persisted acknowledgement no longer limits what it can drain; the loop below already describes this. Absorption is capped at the power-durable watermark, and consumers run as a pool sized to cores (Phase 1b). Until then the loop absorbs up to the published tail, one thread per shard.

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

Where `jitter` is a per-key random offset in the range `[0, quiet_threshold * jitter_fraction]`, computed once when the entry enters the buffer and stable across priority queue reorderings. See "Flush Machinery Design Decisions §3" below for the defaults and rationale.

### Consumer Thread Loop

The cold consumer is **per-shard**: each shard has its own `ColdConsumer` owning its own `CompactionBuffer`, and a dedicated thread running a fused drain-flush loop. A `ColdConsumerPool` orchestrator owns the fleet and exposes a `CompactionBufferRouter` through which the tiering engine resolves key→shard→buffer for read-path lookups. This follows the shared-nothing direction described in ADP-008 and avoids the central-lock bottleneck a single shared buffer would introduce at scale.

Each per-shard thread loop is:

```
loop:
    1. Drain: queue.Read(shard, next_read_seq, max_count, short_timeout)
       - next_read_seq is the consumer's own position: committed offset + 1
         on start, then one past the last entry read
       - Decode each QueueEntry (Write / Conditional / Resolved-apply)
       - Parse its RESP command into a typed WriteOp
       - Expand multi-key ops (DEL, MSET) into per-key absorbs
       - buffer.Absorb(key, op, eviction, seq)

    2. Flush: under normal mode, pop entries whose scheduled_time ≤ now
       (quiet window or eviction-deadline fired). Under aggressive mode
       (see Memory Management), pop by deadline order irrespective of now.
       - Drop entries whose absolute TTL has expired before cold is contacted
       - Emit typed ops (tombstones become DEL, live states emit per-type ops)
       - cold_store.ApplyBatch(ops)
         - Success: the flushed entries vanish from the buffer
         - Failure: retain entries, back off exponentially, retry

    3. Commit: advance the committed offset to min(latest_drained_seq,
       oldest_pending_seq - 1), clamped to the cold checkpoint and the
       durable WAL tail. This low-water-mark commit keeps the WAL retaining
       any un-flushed writes, so a crash replays them from the queue.
```

**Commit policy — low-water per shard.** A compacted buffer entry absorbs many seqs. The consumer must not commit past any seq whose writes have not yet been flushed. The committed offset governs retention and restart position only. The read position is separate, so a key that stays in the buffer for hours pins retention without stopping the consumer from draining everything after it. Because each shard's consumer owns its own buffer and queue partition, the watermark is computed locally: the smallest `first_seen_seq` across the buffer entries, minus one, bounded by the latest drained seq. No cross-shard coordination is required.

**Undecodable entries.** An entry whose parser exists but rejects it (decoder skew) is poison. The consumer records it, pins its committed offset below the poison so the WAL keeps the entry, and reads on. The pin stays visible in metrics until an operator intervenes. Until a fixed build replays from the pinned offset, the cold store may hold that key's state from writes after the poison. A `Flush` after the poison releases the pin, because the wipe discards the state the entry would have produced.

**Failure policy.** Apply failures hold the flusher on that shard — it keeps retrying with exponential backoff and records `apply_failures` / `retry_attempts`. Back-pressure is deliberate: if the cold store is unwritable, the WAL retains data and we prefer stalling over silent drops. Drain on other shards is unaffected.

**Post-flush:** When flushed, the entry is removed from the buffer. If a new write arrives for the same key later, it re-enters with a fresh `first_seen` and `first_seen_seq`.

**Absolute TTL interaction:** If a key's absolute `ttl` has expired by flush time, the entry is dropped without writing to cold.

### Memory Management

The buffer is bounded by unique keys in the eviction window. Under normal operation this is manageable. If buffer memory exceeds the **high-water** mark, the cold consumer switches to `Aggressive` mode — flushing the oldest entries by deadline order regardless of quiet window. This sacrifices write efficiency for memory stability.

Mode transitions use **hysteresis** to avoid thrashing: the consumer enters aggressive at `buffer_high_water_bytes`, and only returns to `Normal` once the buffer has drained below `buffer_low_water_bytes` (default: 75% of high-water). Each transition increments a `mode_transitions` counter; under a healthy workload the mode should flip at most once per burst.

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

The two flush-trigger counters are a leading indicator of buffer churn:

```
deadline_ratio = flushes_deadline / (flushes_quiet + flushes_deadline)

WARN if deadline_ratio > 0.1 over a 5-minute window
```

A healthy workload should flush via the quiet trigger the vast majority of the time. A rising deadline ratio means keys are being rewritten too frequently for the quiet window to elapse, so the buffer is holding entries longer than necessary and the cold gap is creeping toward the eviction deadline.

### Configuration

```yaml
cold_consumer:
  quiet_threshold_seconds: 30
  safety_margin_seconds: 300
  jitter_fraction: 0.1
  buffer_high_water_bytes: 536870912    # 512 MiB
  buffer_low_water_bytes: 0             # 0 = auto, 3/4 of high_water
  max_flush_batch_size: 10000
  queue_read_max_count: 1024
  queue_read_timeout_ms: 50
  retry_initial_backoff_ms: 50
  retry_max_backoff_ms: 30000
  checkpoint_max_flushes: 32            # Checkpoint after this many applied batches
  checkpoint_min_interval_ms: 50        # ...or this interval, whichever comes first
  loop_initial_backoff_ms: 1            # Idle-loop backoff floor
  loop_max_backoff_ms: 1000             # Idle-loop backoff ceiling
  drain_grace_seconds: 15               # Bound on the graceful drain-and-flush
```

The checkpoint knobs bound the cold durable-checkpoint cadence. The cold consumer's ack cannot pass data the last checkpoint has not made durable, so together they bound how far WAL retention release trails the applied frontier — trading checkpoint cost against retained WAL.

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

## Flush Machinery Design Decisions

Decided during flush implementation (#25, #26). These are locked in and should not be revisited without measurement data.

### 1. Lazy staleness for heap entries

When a key is re-absorbed, its quiet deadline slides forward. Rather than finding and updating the existing heap entry (O(N) for a binary heap), we insert a new heap entry and let stale entries be discarded on pop. The staleness check compares the heap entry's scheduled time against `NextFlushTime(entry, entry.eviction) + entry.jitter_offset` recomputed from the entry's current state. If they differ, the heap entry is stale and is discarded.

This is simpler and cheaper than eager updates. The only cost is extra heap entries — one per re-absorb — but these are cheap (a time point + string key) and are lazily cleaned on the next FlushReady call. Pathological rewrite-heavy workloads could accumulate many stale entries, but the cold consumer calls FlushReady regularly, bounding the accumulation.

### 2. Eviction stored on BufferEntry

The flush strategy needs the per-key eviction TTL to compute the eviction deadline. Storing it on BufferEntry at Absorb time makes the heap self-contained — no additional lookups needed when popping entries. The eviction is updated on every re-absorb to reflect the latest TTL from the queue.

### 3. Jitter: 10% of quiet_threshold

Jitter is a per-entry random offset in `[0, quiet_threshold * jitter_fraction]`, computed once when the entry first enters the buffer. At default values (quiet_threshold=30s, jitter_fraction=0.1), the range is [0, 3s]. The jitter is stable across re-absorbs so the staleness check remains valid. The jitter is added to the result of `NextFlushTime`, spreading both quiet-window and deadline flushes. The fraction is configurable via `FlushStrategy` but the default should not be tuned until measured under production load.
