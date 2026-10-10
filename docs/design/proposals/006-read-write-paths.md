# ADP-006: Read and Write Paths

**Status:** Accepted
**Created:** 2026-04-09

> **Amended by [ADP-015](015-write-path-and-durability.md).** The write path becomes a sequenced write that is decided, logged and applied under the shard lock, then acknowledged at the configured durability class. A cold hit fills hot directly instead of appending a promotion entry, and the read-consistency wait is replaced by the residency invariant (Phase 2). The Flush acknowledgement no longer requires a persisted consumer offset (Phase 1a). The sections below describe current behaviour until each phase lands.

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

### Hot-tier delete tombstones and the read-consistency gate

A read that misses hot may need to consult the compaction buffer overlay and/or cold. That overlay is the cold consumer's eventually-consistent view of the queue and can lag hot — a write hot has already applied (and the client observed `+OK` for) is not visible to the buffer until the cold consumer absorbs it. Naively consulting the overlay on a hot miss breaks read-after-write whenever the lagging state is a *deletion*:

- `SET k v` then `DEL k` then `GET k`: if cold absorbed the `SET` but not the `DEL`, a stale overlay returns `v` instead of `nil`.
- `HSET h a 1 b 2` then `HDEL h a b` then `HGETALL h`: a stale overlay resurrects the deleted fields.

Hot resolves this for the common case by retaining a **delete tombstone**. When the hot consumer applies a delete (`DEL`/`GETDEL`, or an `SREM`/`ZREM`/`HDEL` that empties a collection), it keeps an authoritative "absent" marker stamped with the delete's queue seq instead of erasing the key. A read of a tombstone returns `nil`/empty directly from hot — it never consults the overlay and never waits. The tombstone is reclaimed by the eviction worker once the per-shard cold consumer's drained seq passes the delete's seq, at which point the overlay and cold reflect the delete and a true miss is safe. Tombstones do not count toward `DBSIZE`. See [ADP-002](002-hot-store.md) §Delete Tombstones.

This makes read-after-delete — and the read hot path generally — gate-free. A residual wait remains only for a read that genuinely misses hot (a key hot has never held or has evicted) **and** must merge a collection overlay, because a partial mutation (`SREM`/`HDEL`) against a *cold-resident* collection lands only in the buffer, which hot cannot tombstone. For those reads the engine waits for the per-shard cold consumer's `latest_drained_seq` to reach hot's `HighestSettledSeq` before consulting the overlay, bounded by `engine.buffer_consistency_wait_timeout_ms` (default 100ms). The wait is **signal-driven in both directions** — the cold consumer's drain loop wakes waiters via a condition variable as it advances, and a reader entering the wait signals the drain loop to resume immediately rather than finish its current idle backoff. The second direction is load-bearing: the consumer's idle backoff ceiling is deliberately far longer than this wait's deadline, so without it an idle (not wedged) consumer would trip the timeout on the first collection read after a quiet period. Neither direction polls. The timeout fires only if the cold consumer is wedged, in which case the engine returns a Redis error rather than serving stale data: failing closed preserves the invariant. String reads, deletes, and hot hits never wait. Eliminating this residual wait for cold-resident collection mutations is tracked as a follow-up (hot-side field tombstones).

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

Each shard receives a `Flush` queue entry. The hot consumer wipes its in-memory store and fulfils its per-shard flush RPC. The cold consumer drops its compaction buffer (pre-Flush writes never reach cold), wipes **its own shard's slice** of the cold store, and fulfils its RPC. The Resolver clears its existence cache, emits Skip Resolveds for any pre-Flush Conditional whose Resolved had not yet been issued (so consumers' block-and-scan can drain past the parked Conditionals), and fulfils its RPC.

The client sees `+OK` only when every consumer on every shard has applied the wipe. Partial fan-out failures (a shard's durable wait or any consumer's apply timeout) surface as a Redis error; the Flush entries that did land remain durable in the queue and apply on consumer catch-up. A retry of FLUSHDB is idempotent at the wipe level.

**Per-shard wipe isolation.** `ColdStore::Wipe(shard)` deletes only the keys whose shard slot equals that shard (ADP-010 §Per-shard wipe). A single embedded RocksDB instance backs all of a pod's shards, but the shard-prefixed key encoding partitions it into disjoint per-shard slices, so a shard's wipe touches no peer's data. This closes a cross-shard data-loss race that a global wipe exposed: during parallel recovery replay (ADP-007) or an aggressive-mode early flush, a lagging shard's replayed `Flush` would re-run a global wipe and destroy data a peer shard had already flushed to cold after *its* own `Flush`. With per-shard isolation, a replayed `Flush` re-wipes only its own slice and is idempotent against it; cross-shard interleaving is irrelevant.

**Durability invariant — Flush ack precedes RPC fulfilment.** Each consumer persists its per-shard Flush ack *before* fulfilling the Flush RPC that the engine waits on, so a FLUSHDB `+OK` never precedes the durable per-shard ack. The invariant holds uniformly across hot/cold/resolver consumers — even where the store is in-memory and self-correcting on recovery — so a future persistent hot snapshot inherits the ordering guarantee. (Per-shard wipe isolation removes the cross-shard destruction this ordering previously had to guard against; the ordering remains because the observable `+OK` must still never run ahead of durability.)

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

### Canonical form on the write path

An unconditional write is parsed once, at the frontend, before anything reaches the queue, and it is the parsed form — re-expressed canonically — that gets appended. Canonical means one spelling per operation: command aliases collapsed (`SETEX` and `SET ... EX` become the same `SET`), and relative TTLs already resolved to the absolute instant they denote. The contract is a round trip: re-parsing the canonical form must reproduce the operation the client's original spelling produced. That is what keeps the change invisible to the reply and to both tiers, since a reply and a tier mutation are each a pure function of the parsed operation. Commands that are distinct operations stay distinct commands even where their arguments coincide — `HMSET` acknowledges with `OK` where `HSET` returns a count, so it is not a spelling of `HSET`.

Two properties follow. Malformed input cannot be made durable, because a command that does not parse never reaches the append; the parse *is* the validation, rather than a separate check that could drift from it. And every reader of the log — hot, cold, the resolver, recovery — sees one spelling with nothing left to re-derive, so there is no opportunity for two readers to derive different meanings from the same entry. Absolute TTLs in particular are fixed at the moment of the write rather than recomputed against each entry's append timestamp, which removes replay's dependence on that timestamp agreeing across tiers.

Conditional writes are validated the same way but are appended in the client's own spelling, because the resolver reads the predicate out of the command text when it decides. Commands with no parser in the build are exempt from validation entirely: that is a capability gap rather than malformed input, and the command registry remains the sole authority for them.

## Invariants

1. Reads check tiers in order: hot → buffer → cold. No tier is skipped.
2. Hot hits refresh the eviction timer. Buffer and cold hits do not.
3. Buffer hits do not promote. Cold hits do promote (via queue append).
4. A write is never acknowledged until both the queue fsync and hot consumer apply are complete.
5. The promise registry is bounded: entries are removed on fulfillment or timeout. A stalled consumer causes promise timeouts, not unbounded registry growth.
6. A recent delete is an authoritative hot tombstone: reads of a deleted key (`GET`, `EXISTS`, emptied-collection reads) return `nil`/empty from hot without consulting the lagging overlay or waiting. A read that genuinely misses hot **and** must merge a collection overlay waits — signal-driven, not polling — for the per-shard cold consumer to reach hot's settled seq; if the wait exceeds `engine.buffer_consistency_wait_timeout_ms` the engine returns a Redis error rather than serving a stale overlay. Because entering the wait also wakes the cold consumer out of any idle backoff, that timeout indicates a genuinely wedged consumer rather than a merely sleeping one. Tombstones are reclaimed once cold has drained past the delete and do not count toward `DBSIZE`.

## Trade-offs

**Why not promote from buffer?** The cold consumer is already managing that key. It will flush it to cold on its own schedule. Promoting from the buffer would create a duplicate queue entry, and the cold consumer would need to reconcile the buffer entry with the promoted entry. This adds complexity for no benefit — the key is already accessible via the buffer.

**Why promote from cold via queue instead of writing directly to hot?** The queue is the sole write path. If we wrote directly to hot, the promoted key would not have a queue entry and would be lost on crash. Promoting through the queue gives the key a fresh entry that survives recovery. The cold consumer harmlessly re-absorbs it (the cold store already has the data, so the compacted flush is a no-op).

**Why block the write handler on both fsync and hot apply?** The client expects that after receiving OK, a subsequent read returns the written value. If we only waited for fsync, there would be a window where the write is durable but not yet readable from hot. The client might read stale data immediately after a successful write. Waiting for both ensures read-after-write consistency.

## Amendment: the settled seq is a floor, not a raw max

**Status:** Accepted amendment to Accepted ADP-006. Resolves finding HOTC-7.

Invariant 6 documents the read-consistency gate as waiting for the cold consumer's latest-drained seq to reach the hot consumer's highest settled seq, defined as "applied **and** any pending conditional has resolved". The original implementation advanced that signal to the highest hot-applied seq, including sequences that belong to conditional writes whose decision had not yet been applied. A read gated on that value could therefore observe a tier state reflecting an undecided conditional.

This amendment redefines the highest settled seq as the **settled floor**: the highest sequence that is both hot-applied and not behind any unresolved pending conditional — equivalently, the lesser of the highest applied seq and one below the oldest pending conditional's seq. The read-consistency gate consumes this floor, so it never waits on, nor passes a read against, a sequence whose conditional outcome is still undecided. The hot consumer maintains the floor as a separate monotonic value computed once per settle — the same clamp already used to derive the ack target — and publishes it to both the ack and the progress signal. The unsigned-seq-0 guard already present on the ack path is reused, so a conditional pending at seq 0 leaves the floor at its prior value rather than underflowing.

This supersedes the implicit reading of invariant 6 that equated the highest settled seq with the highest applied seq. The gate's externally observable behaviour and the consumer-independence invariant (invariant 3) are otherwise unchanged.

**Implications.** Strengthens read-your-write correctness for conditional writes (`SET NX`, `SETNX`, `MSETNX`, `ZADD GT/LT`, `EXPIRE NX`, `RENAMENX`, `COPY`, `HSETNX`) without changing cold/hot consumer independence. No on-disk or wire format change. When the floor is genuinely 0 on a fresh shard the gate early-out (nothing settled yet) remains correct.
