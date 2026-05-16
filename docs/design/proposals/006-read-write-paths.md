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

### Broadcast Write Path (FLUSHDB / FLUSHALL)

FLUSHDB and FLUSHALL clear every key. To preserve the Kappa invariant — the queue is the single source of truth and every materialised view observes events in queue order — the wipe is routed through the queue rather than executed as a side-channel operation against the stores.

```
Client ──▶ RESP Frontend ──▶ For every owned shard:
                                  ├─ Queue.BeginAppend(entry::Flush)
                                  ├─ Register 3 RPCs (hot, cold, resolver)
                                  └─ Publish
                              │
                              ├─ Wait on every shard's durable future
                              └─ Wait on every consumer's apply future
                                          │
                                          └──▶ +OK to client
```

Each shard receives a `Flush` queue entry. The hot consumer wipes its in-memory store and fulfils its per-shard flush RPC. The cold consumer drops its compaction buffer (pre-Flush writes never reach cold), wipes the cold store, and fulfils its RPC. The Resolver clears its existence cache, emits Skip Resolveds for any pre-Flush Conditional whose Resolved had not yet been issued (so consumers' block-and-scan can drain past the parked Conditionals), and fulfils its RPC.

The client sees `+OK` only when every consumer on every shard has applied the wipe. Partial fan-out failures (a shard's durable wait or any consumer's apply timeout) surface as a Redis error; the Flush entries that did land remain durable in the queue and apply on consumer catch-up. A retry of FLUSHDB is idempotent at the wipe level.

**Durability invariant — Flush ack precedes RPC fulfilment.** Each consumer persists its per-shard Flush ack *before* fulfilling the Flush RPC that the engine waits on. Without this, FLUSHDB could return `+OK` while a peer shard's persisted ack is still pre-Flush; a crash in that window would leave recovery to re-process the missed Flush, and because `ColdStore::Wipe` is global (single RocksDB instance), a replay-time Wipe in any shard destroys data that a parallel shard's replay has already flushed to cold disk after its own Flush. The invariant holds uniformly across hot/cold/resolver consumers — even where the store is in-memory and self-correcting on recovery — so a future persistent hot snapshot does not inherit the race.

Multi-pod (Phase 2+) extends this naturally: each pod receives the broadcast at the RESP layer and runs the same fan-out across its owned shards. There is no cross-pod synchronisation step.

### Write Promise Lifecycle

All client-facing async operations — unconditional writes, conditional writes, and admin RPCs — use a single promise registry (`ConsumerRpc`). This replaces the earlier dual-system design where writes and consumer RPCs had separate promise maps.

1. **Register:** The write handler registers a promise keyed by the queue sequence ID (for writes/conditionals) or a tagged counter (for admin RPCs). Returns a future.
2. **Fulfill:** The responsible consumer (hot consumer for unconditional writes, Resolver for conditionals) fulfils the promise with a `RespValue` after applying. The promise is removed from the registry.
3. **Await:** The write handler blocks on the future with a timeout. On success, it returns the fulfilled response to the client. On timeout, it returns a Redis error.

The critical section is short: one map insert (register) or one map lookup + erase (fulfill). Lock contention is minimal.

See `include/abyss/core/consumer_rpc.h` for the current interface.

### Multi-Key Fan-Out

Multi-key commands tagged in the registry (`CommandSpec::multi_key_kind`) bypass `DispatchRead`/`DispatchWrite` and route to `CommandDispatcher::DispatchFanOut`. The engine decomposes them into per-key sub-commands before any queue append or tier access:

- **Reads** (`MGET`, `EXISTS`) — issue per-key single-key reads through the same hot → buffer → cold path; aggregate positional array (MGET) or sum (EXISTS). `EXISTS` uses a tombstone-aware buffer probe so a not-yet-flushed `DEL` overrides a stale cold residual.
- **Writes** (`MSET`, `DEL`, `UNLINK`) — issue one `Write` queue entry per key. Each `BeginAppend` → `Register` → `Publish` runs sequentially because `BeginAppend` returns with the per-shard append mutex held; the destructor on `PendingAppend` auto-publishes if scope unwinds before an explicit `Publish`. Per-sub durabilities and consumer RPCs are awaited against a shared `write_timeout` deadline. The first sub-error short-circuits the response.

Aggregation rules: `MGET` returns a positional array (nil on miss or hot WRONGTYPE per Redis); `EXISTS` returns the int count without dedup (`EXISTS k k` returns 2); `MSET` returns `+OK` only if every sub succeeded; `DEL`/`UNLINK` returns the integer sum of per-sub replies. No cross-key atomicity — partial state is permitted and clients retry.

Promotion-through-queue on cold hits during fan-out follows the same rule as single-key reads: the promotion entry lands on the *cold-hit key's* owning shard, not on the first key's shard, so Phase-2 horizontal scaling routes correctly.

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
