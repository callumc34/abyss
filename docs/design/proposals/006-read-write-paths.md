# ADP-006: Read and Write Paths

**Status:** Accepted
**Created:** 2026-04-09

## Context

The read and write paths are the core data flows through Abyss. The write path ensures durability and consistency through the queue. The read path provides tiered access across hot, buffer, and cold with promotion semantics for cold hits.

## Design

### Write Path

```
Client ──▶ RESP Frontend ──▶ Queue.Append() ──▶ Hot Consumer ──▶ HotStore.Apply()
                                  │                                    │
                                  │         promise fulfilled ◀────────┘
                                  │                │
                              fsync complete        │
                                  │                │
                                  └──── BOTH done ──▶ return OK to client
```

1. The RESP frontend receives a write command, wraps it in a `QueueEntry` (Write or Conditional variant), and appends it to the queue for the appropriate shard.
2. The write handler registers a promise in the `ConsumerRpc` registry, keyed by the returned sequence ID.
3. The queue append blocks until the write is durable (for group commit: the batch containing this write has been fsynced).
4. Concurrently, the hot consumer reads the entry from the queue's in-memory buffer and applies the typed operation to the hot store.
5. After applying, the hot consumer fulfils the promise with the response value.
6. The write handler awaits both the queue fsync and the promise fulfillment. When both are complete, it returns the response to the client.

The queue is the sole write path. There is no dual write. The hot consumer ACK is an in-process synchronisation — this is why the hot consumer is always an in-process thread, even when the queue and hot store are external.

**Timeout:** The promise has a configurable timeout (default 5s). If the hot consumer fails to ACK within this window, the write handler returns a Redis error. The write is still durable in the queue and will eventually be applied.

### Read Path

```
Read command arrives
  │
  ├─ Check Hot Store
  │   ├─ HIT → refresh eviction timer, return result
  │   └─ MISS ──┐
  │              │
  │   ┌──────────▼───────────────────┐
  │   │ Check Cold Consumer Buffer   │
  │   │ (shared_mutex read lock)     │
  │   ├─ HIT → return result         │
  │   │   (do NOT promote — cold     │
  │   │    consumer owns this entry  │
  │   │    and will flush it)        │
  │   └─ MISS ──┐                    │
  │              │
  │   ┌──────────▼──────────┐
  │   │   Check Cold Store  │
  │   ├─ HIT → Promote:     │
  │   │   • Append to queue │
  │   │     (fresh entry)   │
  │   │   • Hot consumer    │
  │   │     applies with    │
  │   │     fresh eviction  │
  │   │   • Cold consumer   │
  │   │     re-absorbs      │
  │   │     (harmless merge)│
  │   │   Return result     │
  │   └─ MISS → Return nil  │
  │
  └─ Record metrics: hit tier (hot/buffer/cold/miss), latency
```

### Buffer Hit Semantics

Buffer hits do **not** promote. The cold consumer owns data in the buffer and will flush it to cold on its own schedule. Promoting from the buffer would create a duplicate queue entry for data the cold consumer is already managing.

The buffer read acquires a shared lock on the `shared_mutex`. Multiple I/O threads can read from the buffer concurrently. The cold consumer acquires an exclusive lock only when absorbing new entries or removing flushed entries.

### Cold Hit Promotion

Cold store hits **do** promote via the queue. This gives the promoted key:

- A fresh queue entry (survives recovery).
- A fresh eviction timer in the hot store.
- Re-entry into the cold consumer's compaction buffer (harmless no-op merge since cold already has the data).

Promotion is a queue append, not a direct hot store write. This preserves the invariant that the queue is the sole write path.

### Write Promise Lifecycle

All client-facing async operations — unconditional writes, conditional writes, and admin RPCs — use a single promise registry (`ConsumerRpc`). This replaces the earlier dual-system design where writes and consumer RPCs had separate promise maps.

1. **Register:** The write handler registers a promise keyed by the queue sequence ID (for writes/conditionals) or a tagged counter (for admin RPCs). Returns a future.
2. **Fulfill:** The responsible consumer (hot consumer for unconditional writes, Resolver for conditionals) fulfils the promise with a `RespValue` after applying. The promise is removed from the registry.
3. **Await:** The write handler blocks on the future with a timeout. On success, it returns the fulfilled response to the client. On timeout, it returns a Redis error.

The critical section is short: one map insert (register) or one map lookup + erase (fulfill). Lock contention is minimal.

See `include/abyss/core/consumer_rpc.h` for the current interface.

### Metrics

Every read records which tier served the response:

- `abyss_hits_total{tier="hot"}` — hot store hit
- `abyss_hits_total{tier="buffer"}` — compaction buffer hit
- `abyss_hits_total{tier="cold"}` — cold store hit (with promotion)
- `abyss_misses_total` — key not found in any tier
- `abyss_promotions_total` — cold hits promoted back to hot

## Invariants

1. Reads check tiers in order: hot → buffer → cold. No tier is skipped.
2. Hot hits refresh the eviction timer. Buffer and cold hits do not.
3. Buffer hits do not promote. Cold hits do promote (via queue append).
4. A write is never acknowledged until both the queue fsync and hot consumer apply are complete.
5. The promise registry is bounded: entries are removed on fulfillment or timeout. A stalled consumer causes promise timeouts, not unbounded registry growth.

## Trade-offs

**Why not promote from buffer?** The cold consumer is already managing that key. It will flush it to cold on its own schedule. Promoting from the buffer would create a duplicate queue entry, and the cold consumer would need to reconcile the buffer entry with the promoted entry. This adds complexity for no benefit — the key is already accessible via the buffer.

**Why promote from cold via queue instead of writing directly to hot?** The queue is the sole write path. If we wrote directly to hot, the promoted key would not have a queue entry and would be lost on crash. Promoting through the queue gives the key a fresh entry that survives recovery. The cold consumer harmlessly re-absorbs it (the cold store already has the data, so the compacted flush is a no-op).

**Why block the write handler on both fsync and hot apply?** The client expects that after receiving OK, a subsequent read returns the written value. If we only waited for fsync, there would be a window where the write is durable but not yet readable from hot. The client might read stale data immediately after a successful write. Waiting for both ensures read-after-write consistency.
