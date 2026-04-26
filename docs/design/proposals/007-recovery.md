# ADP-007: Recovery

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-04-15

## Context

When an Abyss pod restarts, the hot store is empty (it was in-memory) and the cold store may be stale (the cold consumer had buffered writes that weren't yet flushed). The queue is the source of truth. Recovery rebuilds all consumers' state from the queue.

Phase 1 has three consumers: the Resolver ([ADP-011](011-conditional-writes-and-consumer-rpc.md)), the cold consumer ([ADP-004](004-cold-consumer.md)), and the hot consumer ([ADP-002](002-hot-store.md)). Recovery replays them in that order.

## Design

### Recovery Process

```
Pod starts
  │
  ├─ 1. Determine replay point
  │     Oldest un-acknowledged sequence ID across all consumers.
  │
  ├─ 2. Replay queue to resolver
  │     Resolver rebuilds its recent-writes existence cache from
  │     Write, Conditional, and Resolved entries. For every
  │     Conditional whose matching Resolved is already in the log,
  │     no new emission happens — the log carries the decision.
  │     For every Conditional with no matching Resolved (the crash
  │     happened between Conditional fsync and Resolved fsync), the
  │     resolver re-decides and emits a fresh Resolved. The decision
  │     is deterministic given the log state up to the Conditional's
  │     seq, so the post-recovery outcome is identical to the
  │     pre-crash decision had the original Resolved fsynced.
  │
  ├─ 3. Replay queue to cold consumer
  │     Cold consumer absorbs entries into compaction buffer,
  │     which deduplicates and merges per key. Conditional entries
  │     are handled via block-and-scan against Resolved entries
  │     (ADP-011). Entries whose absolute TTL has expired are skipped.
  │     Buffer is then flushed to cold store (compacted).
  │
  ├─ 4. Replay queue to hot consumer
  │     Hot consumer replays entries still within eviction window.
  │     Block-and-scan handles Conditional entries (ADP-011).
  │     Entries whose eviction would have expired are skipped.
  │     Entries whose absolute TTL has expired are skipped.
  │     Applied in order — last write wins naturally.
  │
  ├─ 5. Resume normal operation
  │     All consumers switch to real-time queue tailing.
  │     Readiness probe goes healthy.
  │
  └─ During recovery: RESP port returns LOADING errors
     (narrow admin set remains available — see ADP-005)
```

### Recovery Semantics

- The queue WAL or external broker retains all entries since the oldest un-acked position.
- For the embedded profile: cold store RocksDB survives on PVC. The cold consumer only replays entries since its last ack point.
- For the hot store: replays everything within the eviction window that hasn't absolutely expired. The hot store is fully reconstructed from the queue without reading cold.
- **The cold store is never read during recovery.** Recovery is purely queue replay.
- Cold replay benefits from the compaction buffer — recovery write volume to cold is bounded by unique keys, not total queue entries.

### Replay Ordering

Resolver replays first, then cold, then hot. Rationale:

- **Resolver first** because cold and hot use block-and-scan over `Resolved` entries. While block-and-scan works off the queue directly (not the resolver's in-memory state), replaying the resolver first re-warms its existence cache so post-recovery conditional writes do not pay cold-lookup latency on keys the resolver already knew about pre-crash.
- **Cold before hot** ensures the cold store is up-to-date before the hot consumer starts serving reads. If hot replayed first and a client read missed hot, the cold fallback might return stale data if cold had not caught up.

Once hot finishes replay and switches to real-time tailing, the system is fully consistent.

Within each consumer's replay, entries are applied in sequence order. For the hot store, last-write-wins naturally produces the correct state. For the cold consumer, the compaction buffer merges entries per key, producing the same result regardless of how many intermediate writes exist. Block-and-scan semantics for `Conditional`/`Resolved` pairs are preserved during replay just as in steady state.

### Queue Retention for Recovery

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

This must fit on the WAL PVC (embedded) or within broker retention config (external). If the queue does not retain enough entries, recovery may be incomplete — keys that were in the eviction window but whose queue entries have been garbage collected will be lost from hot. They will still be available in cold (the cold consumer flushed them before eviction), so they are not lost entirely, but the first read post-recovery will require a cold-path lookup and promotion.

### Configuration

```yaml
recovery:
  replay_parallelism: 4
  hot_replay_batch_size: 10000
  cold_replay_batch_size: 50000
```

## Invariants

1. Recovery is pure queue replay. The cold store is never read. All consumers rebuild their state entirely from the queue.
2. Replay order is resolver → cold → hot.
3. Entries whose absolute TTL has expired at replay time are skipped.
4. For the hot consumer, entries whose eviction would have expired at replay time are skipped.
5. During recovery, the RESP port returns `LOADING` errors for data-plane commands; a narrow admin set (see [ADP-005](005-resp-frontend.md)) remains available.
6. After recovery, the readiness probe goes healthy and normal operation resumes.
7. The resolver emits no new `Resolved` entries during replay — the log already contains matching decisions from the original run.

## Trade-offs

**Why replay cold before hot?** The cold store needs to be consistent before hot starts serving reads. If hot replayed first and a client read missed hot, the cold fallback might return stale data if cold hadn't caught up. Replaying cold first eliminates this window.

**Why not read from cold during hot recovery?** Simplicity. Reading from cold during recovery would require the tiering engine to be partially operational before recovery completes. It also introduces a dependency between the hot consumer's replay and the cold store's state. Pure queue replay is deterministic and independent — each consumer rebuilds from the log without external state.

**Why skip expired entries during replay?** Applying a key whose TTL has already passed wastes work and fills the hot store with data that would be immediately expired. Skipping expired entries speeds up recovery and keeps the post-recovery hot store lean.

**Why use the compaction buffer during cold recovery?** Without the buffer, replaying N queue entries for a key that was written N times would produce N cold store writes. The compaction buffer collapses these into one write per key, making recovery write volume proportional to unique keys, not total queue depth. For a 24-hour queue with 1M entries but only 100K unique keys, this is a 10x reduction.
