# ADP-002: Hot Store

**Status:** Accepted
**Created:** 2026-04-09

## Context

The hot store is the in-memory tier of Abyss. It serves the lowest-latency reads and is the first tier checked on every read. The hot consumer applies writes from the queue in real-time, and I/O threads read from it concurrently.

## Design

### Interface

The hot store interface accepts typed operations rather than raw RESP commands. Reads arrive as a `ReadOp` variant (e.g. string get, set membership check, sorted set range query). Writes arrive as a `WriteOp` variant (e.g. string set, set add, sorted set add). This decouples RESP parsing from storage — the RESP frontend parses once, and stores execute typed results without string-matching on command names.

Read operations (`Exec`) refresh the eviction timer on hits. Write operations (`Apply`, `ApplyBatch`) set the eviction timer per the configured duration for the key's prefix.

See `include/abyss/core/hot_store.h` and `include/abyss/core/ops.h` for the current interface.

Phase 1 built-in hot store supports: strings, sets, sorted sets. Hashes and lists are Phase 2 candidates. External stores (DragonflyDB, Redis, Valkey) support whatever they natively support — Abyss passes commands through.

### Built-in Concurrent Hash Map

The Phase 1 hot store is a concurrent hash map with sharded locks.

**Lock striping:** The keyspace is divided into N lock shards (fixed at deployment). Each key hashes (xxHash) to a shard. I/O threads acquire a shared lock on the relevant shard for reads. The hot consumer acquires an exclusive lock for writes.

The number of lock shards should equal the planned horizontal shard count. Phase 1's internal sharding boundaries then match Phase 2's pod boundaries, easing migration.

**Data structures per key:**
- Value storage (string, set members, sorted set members + scores)
- Eviction deadline (steady clock timestamp, refreshed on read)
- Absolute TTL deadline (wall clock timestamp, not refreshed)

### Eviction

Keys live in the hot store for their `eviction` duration, which is refreshed on every read hit. When a key's eviction deadline passes without a read, it is evicted from hot. The key remains available in cold — eviction is a tier transition, not a deletion.

Eviction duration is configured globally or per-prefix:

```yaml
hot:
  default_eviction_seconds: 86400
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300
```

**LRU eviction under memory pressure:** If the hot store's memory usage exceeds its configured maximum, keys are evicted in LRU order (by last-read time) regardless of their eviction deadline. Data is safe — it's in the queue and eventually in cold. Reads for prematurely evicted keys fall through to the buffer and cold store.

**Absolute TTL:** If a key has an absolute `ttl` set (via Redis `SET ... EX`, `EXPIRE`, etc.), the hot store tracks this separately. When the absolute TTL expires, the key is deleted — not just evicted. It is removed from both hot and cold. This is different from eviction: TTL expiry means the data is gone.

### Hot Consumer

The hot consumer runs as a dedicated thread that tails the queue and applies writes to the hot store.

**Behaviour:**
- Always at or near the head of the queue.
- Applies writes immediately as they arrive via `HotStore::Apply`.
- Sets the eviction duration on each key (from global default or per-prefix config).
- Fulfils the write handler's promise after each successful apply, unblocking the client response. See [ADP-006](006-read-write-paths.md) for the write promise lifecycle.

**Lag budget:** Effectively zero. The hot consumer must keep up with the write rate. If it falls behind, write latency increases because clients are awaiting their promises. This is self-regulating — rising latency naturally reduces write throughput via client backpressure.

**Eviction refresh vs queue retention:** Read refreshes extend a key's life in the hot store indefinitely, but the key's queue entry is subject to normal retention (`min_retention_seconds`). If the pod crashes and the key has outlived its queue entry, it is lost from hot but present in cold (the cold consumer flushed it before the eviction deadline). The first read post-recovery hits cold, triggers a promotion (fresh queue entry), and the key returns to hot. Cost: one cold-path read per such key after recovery.

### Configuration

```yaml
hot:
  backend: builtin_hashmap
  max_memory_bytes: 4294967296        # 4 GiB
  default_eviction_seconds: 86400     # 24 hours
  eviction_policy: lru
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300
```

## Invariants

1. `Exec` refreshes the eviction timer on read hits.
2. `Apply` sets the eviction timer to the configured duration for the key's prefix.
3. A key evicted from hot is never deleted — it remains available in cold.
4. A key whose absolute TTL has expired is deleted from hot (and cold, via the cold store's expiry mechanism).
5. The hot consumer is always an in-process thread, even when the hot store backend is external. This is required for the write promise lifecycle — the promise fulfillment must be an in-process synchronisation, not a network round trip.

## Trade-offs

**Why sharded locks instead of lock-free structures?** Lock-free concurrent hash maps are complex, harder to reason about, and harder to extend with eviction tracking. Sharded locks give good concurrency (contention is proportional to 1/N where N is shard count) and are straightforward to implement correctly. The shard count can be tuned to match core count.

**Why not thread-per-core (Seastar model) from the start?** Thread-per-core eliminates locks entirely by partitioning data across cores. It's the right end state (Phase 4) but requires a custom runtime or Seastar dependency. Phase 1's sharded lock model is simpler to build, debug, and reason about while we validate the architecture. The shard-aligned design means migration to thread-per-core changes the concurrency model but not the data model.

**Why is the hot consumer always in-process?** The write acknowledgement flow requires the hot consumer to fulfil a `std::promise` after applying. This is an in-process synchronisation. If the hot consumer were remote (e.g., a separate process reading from Kafka and writing to external Redis), the promise mechanism wouldn't work — you'd need a network callback. Keeping the hot consumer in-process means write ACK is a local operation regardless of the deployment profile.
