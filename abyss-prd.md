# PRD: Abyss

## Status: Draft
## Author: [TBD]
## Last Updated: 2026-04-09

---

## 1. Problem Statement

Many pipelines and services require a fast key-value store with persistence guarantees that outlive process restarts, but without the operational overhead of a full database. In-memory stores like Redis and DragonflyDB offer excellent latency but lose data on restart and become expensive as data volumes grow. Disk-backed stores offer persistence but can't match in-memory read latency. Running both independently creates consistency and synchronisation headaches.

We need a single, Redis-compatible service that transparently manages two storage tiers — fast in-memory and durable on-disk — with clear semantics for data lifecycle across tiers, durable recovery, and the ability to swap implementations for each component depending on the deployment context.

## 2. Proposed Solution

**Abyss** — a Kubernetes-native service that exposes a standard Redis protocol interface backed by a Kappa architecture: an append-only queue is the single source of truth, and two independent consumers — a hot (in-memory) store and a cold (on-disk) store — materialise state from it. Written in C++. Stores opaque bytes — it is data-model agnostic.

Callers connect with any Redis client library. Abyss handles tiering, persistence, and recovery transparently.

### 2.1 Core Architecture

```
                    ┌───────────────┐
  Redis Client ────▶│  RESP Frontend │
                    └──────┬────────┘
                           │ write
                           ▼
                    ┌───────────────┐
                    │    Queue      │  ◄── Single source of truth
                    │  (append-only │
                    │     log)      │
                    └──┬─────────┬──┘
                       │         │
            ┌──────────▼──┐  ┌──▼───────────┐
            │ Hot Consumer │  │ Cold Consumer │
            │  (eager,     │  │  (smart,      │
            │   real-time) │  │   compacting) │
            └──────┬───────┘  └──────┬───────┘
                   │                 │
            ┌──────▼───────┐  ┌──────▼───────┐
            │  Hot Store   │  │  Cold Store  │
            │ (in-memory)  │  │  (on-disk)   │
            └──────────────┘  └──────────────┘

  Redis Client ────▶ Read: Hot → Buffer → Cold → nil
```

**Write path:** Client write → Queue (committed) → Hot consumer applies and ACKs → client receives OK.

**Hot consumer:** Reads from the queue eagerly and in real-time. Applies writes to the hot store immediately. Data lives in hot for its `eviction` duration (refreshed on every read), then is evicted from hot but remains available in cold.

**Cold consumer:** Reads from the queue into an in-memory compaction buffer. Intelligently defers flushing to cold until keys go quiet or approach their eviction deadline. The eviction window is its time budget — it can use nearly all of it to wait for optimal flush opportunities.

**Read path:** Hot → Cold Consumer Buffer → Cold Store → nil.

**Recovery:** Replay the queue. Both consumers rebuild their state from the log.

### 2.2 Two TTL Model

Abyss distinguishes between two independent time-to-live values:

| TTL Type | Semantics | What happens on expiry |
|----------|-----------|----------------------|
| `eviction` | How long a key lives in the hot store. Refreshed on every read. | Key is evicted from hot. Key remains available in cold. **Data is not deleted — it moves tier.** |
| `ttl` | Absolute time-to-live. How long the key exists at all. Not refreshed. | Key is deleted from both hot and cold. **Data is gone.** |

**Behaviour matrix:**

| `eviction` set? | `ttl` set? | Behaviour |
|-----------------|------------|-----------|
| Yes | No | Key lives in hot for `eviction` (refreshed on reads), then cold indefinitely. |
| No | Yes | Key lives in hot for default `eviction` (refreshed on reads), then cold until `ttl` expires. |
| Yes | Yes | Key lives in hot for `min(eviction, ttl)`. Lives in cold until `ttl` expires. |
| No | No | Key lives in hot for default `eviction` (refreshed on reads), then cold indefinitely. |

Standard Redis TTL commands (`SET key value EX 3600`, `EXPIRE`) set the absolute `ttl`. The `eviction` duration is configured globally or per-prefix — not per-command — to avoid protocol extensions.

```yaml
hot:
  default_eviction_seconds: 86400

  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300
```

### 2.3 Deployment Profiles

#### Profile: Embedded (Default)

All components run in-process. Zero external dependencies. Single-pod only.

| Component | Implementation |
|-----------|---------------|
| Hot Store | Built-in concurrent hash map (in-memory) |
| Cold Store | Built-in RocksDB-backed store (on PVC) |
| Queue | Built-in append-only WAL on PVC |

#### Profile: External

Each component delegates to an external system. Required for horizontal scaling.

| Component | Example Implementation |
|-----------|----------------------|
| Hot Store | DragonflyDB / Redis / Valkey |
| Cold Store | KVRocks / another Redis-compatible on-disk store |
| Queue | Kafka / Redpanda / NATS JetStream |

#### Profile: Hybrid

Mix of embedded and external. E.g., embedded hot + NATS queue + external KVRocks cold.

## 3. Execution Model

### 3.1 Phase 1: Single-Pod, Multithreaded

Phase 1 uses a conventional multithreaded model within a single pod:

- **RESP I/O threads** (pool, sized to core count): accept connections, parse commands, route to tiering engine
- **Hot consumer thread** (single, dedicated): tails queue, applies to hot store, fulfils write promises
- **Cold consumer thread** (single, dedicated): tails queue into compaction buffer, flushes to cold
- **Background threads**: WAL segment cleanup, cold store compaction, TTL expiry scanning

Hot store access is protected by a sharded lock scheme (lock striping by key hash, using xxHash) to allow concurrent reads from I/O threads while the hot consumer applies writes. The compaction buffer uses a `shared_mutex` (concurrent reads from I/O threads, exclusive writes from cold consumer).

The number of lock shards is fixed at deployment and should equal the planned horizontal shard count to ease migration. This means Phase 1's internal sharding boundaries match Phase 2's pod boundaries.

### 3.2 Phase 2+: Horizontal Scaling

Abyss supports horizontal scaling by partitioning the keyspace across multiple pods. Each key hashes (xxHash) to exactly one pod. Pods share nothing — each has its own queue, hot store, cold store, and compaction buffer. No cross-pod communication is needed for reads or writes.

**Routing:** Abyss implements the Redis Cluster protocol for topology discovery. Each pod knows the full shard map and responds to `CLUSTER SLOTS` / `CLUSTER SHARDS` queries. Clients that support Redis Cluster (Jedis, Lettuce, redis-py, ioredis, etc.) connect to any pod, discover the topology, and route commands directly to the owning pod. If a command arrives at the wrong pod, Abyss responds with a `MOVED` redirect. This is the industry-standard approach and avoids a proxy bottleneck.

**Shard count:** Fixed at deployment. A rebalance changes the shard-to-pod assignment but not the total shard count. Choosing a shard count larger than the initial pod count (e.g., 64 shards across 4 pods) allows scaling up without changing the hash function — just reassign shard ownership.

**Queue partitioning:** Each shard corresponds to a queue partition. With the external profile, this maps directly to Kafka partitions (key-based partitioning) or NATS subjects (`abyss.shard.{N}`). Since we always partition by key hash, ordering within a key is guaranteed by every broker. The `IQueue` interface is shard-aware from the start (see §4.1).

**Resharding:** Adding pods requires migrating shard ownership. With the external queue profile, the new pod starts consuming its assigned partitions from the beginning of the topic, filters for its shards, and rebuilds state. Existing pods stop processing migrated shards. The cold store data for migrated shards remains on the old pod's PVC — the new pod rebuilds cold from the queue replay, and the old pod's data is eventually garbage collected.

**Constraint:** Horizontal scaling requires the external queue profile. The embedded WAL is per-pod and cannot be consumed by other pods. The embedded profile is single-pod only.

```yaml
cluster:
  enabled: true
  shard_count: 64                  # Fixed at deployment
  hash_function: xxhash            # Consistent across all pods
  this_pod_shards: [0, 1, 2, 3]   # Assigned via Helm values per pod ordinal
```

## 4. Interfaces

All pluggable components implement C++ abstract interfaces. Implementations are selected at startup via configuration and compiled-in.

### 4.1 Queue Interface (Source of Truth)

```cpp
class IQueue {
public:
    virtual ~IQueue() = default;

    // Append a command to the log for a given shard.
    // This is the commit point — once this returns OK,
    // the write is durable.
    virtual Result<SequenceId> append(ShardId shard, RespCommand cmd) = 0;
    virtual Result<SequenceId> append_batch(
        ShardId shard, std::span<const RespCommand> cmds) = 0;

    // Consumer interface. Each consumer maintains its own
    // cursor per shard independently.
    virtual Result<std::vector<LogEntry>> read(
        ConsumerId consumer,
        ShardId shard,
        size_t max_count,
        Duration timeout) = 0;

    // Acknowledge processing up to a sequence ID.
    virtual Result<void> ack(
        ConsumerId consumer, ShardId shard, SequenceId seq) = 0;

    // Queue retains entries until all consumers have acked.
    virtual Result<SequenceId> oldest_retained(ShardId shard) = 0;

    virtual Result<QueueStats> stats() = 0;
};

struct LogEntry {
    SequenceId seq;
    RespCommand cmd;
    Timestamp appended_at;
};

constexpr ConsumerId HOT_CONSUMER = 0;
constexpr ConsumerId COLD_CONSUMER = 1;
```

**Queue segment management (embedded WAL):**

The queue is an append-only log composed of fixed-size segments per shard:
- Segments are files on the WAL PVC, named by shard and base offset (e.g., `shard-00/00000000000000000000.wal`)
- Segment size: configurable, default 64 MiB
- A background thread periodically deletes segments fully acknowledged by all consumers
- Consumer offsets are persisted to a metadata file on the WAL PVC

For external brokers:
- Kafka: one partition per shard. Retention time-based (`min_retention_seconds`). Consumer offsets managed by Kafka.
- NATS JetStream: one subject per shard (`abyss.shard.{N}`). Durable consumers track ack position.

### 4.2 WAL Durability and Fsync Policy

The embedded WAL's fsync strategy determines the trade-off between write throughput and durability.

**Solution: group commit.** Abyss batches all appends within a configurable window into a single fsync. Write handlers block until their batch is fsynced. The hot consumer can read from the in-memory buffer immediately, but the client promise is not fulfilled until both the fsync completes and the hot consumer applies — these happen in parallel.

```
Writer A ──append──┐
Writer B ──append──┤──▶ [batch buffer] ──fsync──▶ batch complete
Writer C ──append──┘         │
                             ├──▶ hot consumer reads from buffer concurrently
                             │
                      promise fulfilled when BOTH fsync + hot apply done
```

| Policy | Throughput | Max data loss on crash | Use case |
|--------|-----------|----------------------|----------|
| `fsync_per_write` | ~1K ops/s | 0 | Safety-critical |
| `group_commit` (default) | ~50-100K ops/s | Up to `group_commit_interval` of un-ACKed writes | Most workloads |
| `fsync_none` | ~500K+ ops/s | All un-flushed WAL data | Ephemeral data |

The key guarantee: **any write the client received OK for is durable.** Group commit only risks losing writes that were in the batch buffer at crash time and hadn't been fsynced or ACKed to the client yet. The client never saw OK for those, so it can retry.

```yaml
queue:
  wal_fsync_policy: group_commit
  group_commit_interval_us: 1000     # 1ms batch window
  group_commit_max_bytes: 1048576    # Or flush at 1 MiB, whichever first
```

### 4.3 Hot Store Interface

```cpp
class IHotStore {
public:
    virtual ~IHotStore() = default;

    // Execute a Redis command (for reads from clients).
    // Implementations refresh the eviction timer on read hits.
    virtual Result<RespValue> exec(const RespCommand& cmd) = 0;

    // Apply a write from the queue consumer.
    virtual Result<void> apply(const RespCommand& cmd, EvictionTTL eviction) = 0;

    // Bulk apply for recovery (replaying queue).
    virtual Result<void> apply_batch(
        std::span<const RespCommand> cmds, EvictionTTL eviction) = 0;

    virtual Result<MemoryStats> stats() = 0;
    virtual Result<void> flush() = 0;
};
```

### 4.4 Cold Store Interface

```cpp
class IColdStore {
public:
    virtual ~IColdStore() = default;

    // Execute a Redis command (for reads on hot-miss + buffer-miss).
    virtual Result<RespValue> exec(const RespCommand& cmd) = 0;

    // Batch write from the cold consumer.
    virtual Result<void> apply_batch(std::span<const RespCommand> cmds) = 0;

    virtual Result<StorageStats> stats() = 0;
    virtual Result<void> compact() = 0;
};
```

### 4.5 Design Notes

The interfaces accept `RespCommand` as the unit of work. Abyss doesn't interpret the data — it routes Redis commands through the queue to both stores. Individual store implementations decide which commands they support and return standard Redis errors for unsupported ones.

Phase 1 built-in hot store supports: strings, sets, sorted sets. Hashes and lists are Phase 2 candidates. External stores (DragonflyDB, KVRocks) support whatever they natively support — Abyss passes commands through.

## 5. Redis Protocol Frontend

Abyss exposes a TCP listener implementing the Redis wire protocol (RESP2). Any standard Redis client library connects without modification.

### 5.1 Command Routing

| Class | Routing | Examples |
|-------|---------|---------|
| Read | Hot → Buffer → Cold | `GET`, `SMEMBERS`, `ZRANGEBYSCORE`, `EXISTS`, `TTL` |
| Write | Append to Queue | `SET`, `DEL`, `SADD`, `ZADD`, `EXPIRE` |
| Admin | Handled by Abyss | `PING`, `INFO`, `DBSIZE`, `CLUSTER SLOTS` |

Unknown commands default to **write** classification.

### 5.2 Multi-Key Commands

Commands like `MGET` and `MSET` that span multiple keys may target different shards.

**Single-pod (Phase 1):** All keys are local. `MGET` fans out across hot, buffer, and cold per key and assembles the response. `MSET` decomposes into per-key queue entries. There is no cross-key atomicity guarantee — a crash mid-decomposition may persist some keys but not others. This matches DragonflyDB's behaviour under its internal sharding model for practical purposes, and is consistent with how Redis Cluster clients handle cross-slot fan-out.

**Multi-pod (Phase 2+):** If keys in a multi-key command span pods, Abyss responds with `CROSSSLOT` error, matching Redis Cluster semantics. Clients that support Redis Cluster handle this by fanning out per-slot and assembling results client-side. Users can use hash tags (e.g., `{user123}.name`, `{user123}.email`) to co-locate related keys on the same shard.

### 5.3 Write Acknowledgement

A write is acknowledged to the client only after two things have happened:

1. The queue `append()` completes (write is durable — for group commit, the batch fsync has completed).
2. The hot consumer has applied the write to the hot store and acknowledged it.

These happen in parallel: the hot consumer reads from the in-memory buffer while the fsync is in flight. The promise is fulfilled when both complete.

The queue is the sole write path — there is no dual write. The hot consumer ACK is an in-process synchronisation: the write handler registers a `promise` keyed by the queue sequence ID, then awaits it. The hot consumer, which is always an in-process thread (even when the queue and hot store are external), fulfils the promise after applying.

```
Write handler                    Hot consumer (in-process thread)
     │                                  │
     ├─ append to queue ───────────────▶│
     │   (blocks until fsync)           │
     ├─ register promise(seq_id)        │ (reads from buffer concurrently)
     ├─ await promise                   │
     │                                  ├─ apply to hot store
     │                                  ├─ fulfil promise(seq_id)
     │◀─────────────────────────────────┤
     ├─ return OK to client             │
```

**Timeout:** The promise has a configurable timeout (default 5s). If the hot consumer fails to ACK within this window, the write handler returns a Redis error. The write is still durable in the queue and will eventually be applied.

### 5.4 Connection Handling

- Async I/O via `io_uring` (Linux) with `epoll` fallback
- Max concurrent connections: configurable, default 1024
- Idle timeout: configurable, default 300s

## 6. Consumers

### 6.1 Hot Consumer

The hot consumer runs as a dedicated thread that reads from the queue in real-time and applies writes to the hot store.

**Characteristics:**
- Always at or near the head of the queue
- Applies writes immediately as they arrive
- Fulfils the write handler's promise after each successful apply, unblocking the client response
- Sets `eviction` on each key (from global default or per-prefix config)
- When `eviction` expires (without a read refresh), the key is evicted from hot — the cold store retains it
- Hot store refreshes eviction timer on every read hit (standard LRU, no queue involvement)
- If an absolute `ttl` was set on the key, the hot store also tracks this and deletes on expiry

**Lag budget:** Effectively zero. The hot consumer must keep up with the write rate. If it falls behind, write latency increases (clients are awaiting the promise). This is self-regulating.

**Eviction refresh vs queue retention:** Read refreshes extend a key's life in the hot store indefinitely, but the key's queue entry is subject to normal retention (`min_retention_seconds`). If the pod crashes and the key has outlived its queue entry, it is lost from hot but present in cold (the cold consumer flushed it before the eviction deadline). The first read post-recovery hits cold, triggers a promotion (fresh queue entry), and the key returns to hot. Cost: one cold-path read per such key after recovery.

### 6.2 Cold Consumer

The cold consumer is the most architecturally significant component in Abyss. It reads from the queue, maintains an in-memory compaction buffer, and flushes to the cold store only when it is smart to do so.

The key insight: if a key has an `eviction` of 4 hours, we have up to 3.99 hours before we need to persist it to cold. During that window, the key may be updated hundreds of times. Writing every intermediate state to disk is wasteful. Instead, the cold consumer searches for windows where keys are not being actively written, then flushes. If no quiet window appears, it flushes before the eviction deadline as a safety net.

#### 6.2.1 Compaction Buffer

The cold consumer does **not** read from the raw queue and write directly to cold. It maintains an in-memory compaction buffer that absorbs queue entries and deduplicates/merges them per key.

```
Raw Queue ──▶ Cold Consumer ──▶ Compaction Buffer ──▶ (flush decisions) ──▶ Cold Store
```

Each entry in the compaction buffer tracks:

```cpp
struct BufferEntry {
    std::string key;
    CompactedState state;         // Merged state (see §6.2.2)
    Timestamp first_seen;         // Anchors eviction deadline
    Timestamp last_modified;      // Quiet window detection
    uint64_t write_count;         // Writes absorbed since last flush
};
```

The buffer is protected by a `shared_mutex`. The cold consumer thread holds an exclusive lock when absorbing new entries or removing flushed entries. I/O threads performing reads hold a shared lock. This allows concurrent buffer reads with minimal contention.

#### 6.2.2 Compaction Semantics by Command Type

Different Redis data structures require different merge strategies:

**Scalar commands (SET, DEL, EXPIRE):** Last-write-wins. A `SET` replaces any prior state. A `DEL` cancels all preceding writes (tombstone). A `SET` after `DEL` replaces the tombstone.

**Set commands (SADD, SREM):** Merge-accumulate. Consecutive `SADD` commands merge members. `SREM` cancels specific previously-added members. The buffer maintains net-add and net-remove sets. On flush, it emits a single `SADD` for net additions and a single `SREM` for net removals.

**Sorted set commands (ZADD, ZREM, ZREMRANGEBYSCORE):** Same merge-accumulate approach. `ZADD` entries merge (latest score wins for duplicate members). `ZREM` cancels specific members. `ZREMRANGEBYSCORE` applies against accumulated entries by score range.

**A `DEL` for any key type resets the buffer entry entirely.** All accumulated state is discarded and replaced with a tombstone.

```cpp
class CompactedState {
public:
    std::optional<RespCommand> latest_set;              // Scalar keys
    std::unordered_map<std::string, CollectionOp> pending_adds;  // Collections
    std::unordered_set<std::string> pending_removes;    // Collections
    bool is_tombstone = false;

    void absorb(const RespCommand& cmd);
    std::vector<RespCommand> emit() const;  // Minimal commands to apply to cold
};
```

#### 6.2.3 Flush Strategy

Each key has two potential flush triggers:

1. **Quiet window:** The key hasn't been modified for `quiet_threshold`. The burst of writes has likely ended — flush now avoids a rewrite later.

2. **Eviction deadline:** `first_seen + eviction - safety_margin` is approaching. The key MUST be flushed before it expires from hot, regardless of write activity.

A priority queue orders buffer entries by the **earlier** of these two deadlines, with jitter applied to the deadline flush to prevent thundering herds:

```
flush_priority = min(
    last_modified + quiet_threshold,
    first_seen + eviction - safety_margin - jitter
)
```

Where `jitter` is a per-key random offset in the range `[0, safety_margin * 0.5]`, computed once when the entry enters the buffer and stable across priority queue reorderings.

The cold consumer thread loop:

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

#### 6.2.4 Buffer Memory Management

The buffer is bounded by unique keys in the eviction window. Under normal operation this is manageable. If buffer memory exceeds a configurable high-water mark, the cold consumer switches to aggressive mode — flushing the oldest entries by deadline order regardless of quiet window. This sacrifices write efficiency for memory stability.

`abyss_cold_buffer_entries` and `abyss_cold_buffer_bytes` gauges track buffer size. Alerts fire at the high-water mark.

#### 6.2.5 Lag Monitoring

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

### 6.3 Consumer Coordination and Queue Retention

The queue retains entries until **both** consumers have acknowledged. Under normal operation, the cold consumer lags by the eviction window.

### 6.4 Backpressure

1. **Queue full (embedded WAL disk full):** `append()` returns error. Abyss returns Redis error to client. Writes fail. The queue is the truth — if it can't accept writes, writes fail.

2. **Hot consumer stalled:** Promise times out. Abyss returns Redis error. Write is durable in queue, will be applied when hot consumer recovers.

3. **Cold consumer falling behind:** Warning metrics fire. Hot store continues serving reads. No writes blocked. Operator needs to speed up cold consumer or extend `eviction`.

4. **Cold store disk full:** Cold consumer's `apply_batch()` fails. Cold consumer stalls. Queue grows. Eventually queue fills and writes fail. Operator provisions more cold storage.

5. **Hot store memory pressure:** LRU evicts keys before `eviction` expires. Data is safe in queue. Reads for evicted keys check buffer, then cold. If cold consumer hasn't caught up either, data is in the queue but temporarily inaccessible via reads until cold consumer flushes. This is a provisioning problem — allocate more hot store memory or tune eviction.

There is no magic. If you run out of disk, writes fail. If you under-provision memory, reads degrade. Configure your resources.

## 7. Read Path

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

**Buffer hits do NOT promote.** The cold consumer owns that data and will flush it to cold on its own schedule. Promoting from the buffer would create a duplicate queue entry for data the cold consumer is already managing.

**Cold store hits DO promote via the queue.** This gives the promoted key a fresh queue entry (survives recovery), a fresh eviction timer, and re-enters the cold consumer's compaction buffer (harmless no-op merge since cold already has the data).

## 8. Recovery

When an Abyss pod restarts, the hot store is empty and the cold store may be stale. The queue is the source of truth.

### 8.1 Recovery Process

```
Pod starts
  │
  ├─ 1. Determine replay point
  │     Oldest un-acknowledged sequence ID across both consumers.
  │
  ├─ 2. Replay queue to cold consumer
  │     Cold consumer absorbs entries into compaction buffer,
  │     which deduplicates and merges per key.
  │     Entries whose absolute TTL has expired are skipped.
  │     Buffer is then flushed to cold store (compacted).
  │
  ├─ 3. Replay queue to hot consumer
  │     Hot consumer replays entries still within eviction window.
  │     Entries whose eviction would have expired are skipped.
  │     Entries whose absolute TTL has expired are skipped.
  │     Applied in order — last write wins naturally.
  │
  ├─ 4. Resume normal operation
  │     Both consumers switch to real-time queue tailing.
  │     Readiness probe goes healthy.
  │
  └─ During recovery: RESP port returns LOADING errors
```

### 8.2 Recovery Semantics

- The queue WAL or external broker retains all entries since the oldest un-acked position.
- For embedded profile: cold store RocksDB survives on PVC. Cold consumer only replays entries since its last ack point.
- For hot store: replays everything within `eviction` window that hasn't absolutely expired. Hot store is fully reconstructed from the queue without reading cold.
- **The cold store is never read during recovery.** Recovery is purely queue replay.
- Cold replay benefits from the compaction buffer — recovery write volume to cold is bounded by unique keys, not total queue entries.

### 8.3 Queue Retention for Recovery

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

Must fit on the WAL PVC (embedded) or within broker retention config (external).

### 8.4 Recovery Configuration

```yaml
recovery:
  replay_parallelism: 4
  hot_replay_batch_size: 10000
  cold_replay_batch_size: 50000
```

## 9. Cold Store TTL Expiry

Abyss uses a dual expiry model for the cold store, matching the industry-standard approach used by Redis:

**Lazy expiry:** On every cold store read, check the key's absolute TTL. If expired, delete it and return nil. This is zero-cost when keys are not being read.

**Active expiry:** A background thread periodically samples random keys from the cold store and deletes expired ones. The sampling rate ramps up adaptively based on the hit ratio (fraction of sampled keys that were expired):

```
loop:
    sample N random keys (default N=20)
    delete any that are expired
    expired_ratio = expired_count / N

    if expired_ratio > high_threshold (default 0.25):
        increase sample rate (shorter sleep, larger N)
    elif expired_ratio < low_threshold (default 0.05):
        decrease sample rate (longer sleep, smaller N)

    if cold_store_disk_usage > disk_pressure_threshold:
        force maximum sample rate
```

This ensures the cold store doesn't accumulate expired keys indefinitely while keeping CPU usage minimal under normal conditions. Under disk pressure, the scanner becomes more aggressive to reclaim space.

```yaml
cold:
  ttl_expiry:
    active_enabled: true
    sample_size: 20
    base_interval_ms: 1000
    high_threshold: 0.25
    low_threshold: 0.05
    disk_pressure_threshold: 0.9      # Fraction of PVC capacity
    max_cpu_percent: 10               # Cap CPU budget for active expiry
```

## 10. Kubernetes Deployment

### 10.1 StatefulSet (Single Pod)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: abyss
spec:
  serviceName: abyss
  replicas: 1
  template:
    spec:
      containers:
        - name: abyss
          image: abyss:latest
          ports:
            - containerPort: 6379
              name: resp
            - containerPort: 9090
              name: metrics
            - containerPort: 8080
              name: admin
          resources:
            requests:
              memory: "4Gi"
              cpu: "2"
            limits:
              memory: "8Gi"
              cpu: "4"
          volumeMounts:
            - name: cold-storage
              mountPath: /data/cold
            - name: queue-wal
              mountPath: /data/wal
          env:
            - name: ABYSS_PROFILE
              value: "embedded"
            - name: ABYSS_CONFIG_PATH
              value: "/etc/abyss/config.yaml"
          livenessProbe:
            tcpSocket:
              port: resp
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: admin
            periodSeconds: 5
          startupProbe:
            httpGet:
              path: /ready
              port: admin
            failureThreshold: 60
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata:
        name: cold-storage
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 50Gi
    - metadata:
        name: queue-wal
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
```

### 10.2 Multi-Pod (Phase 2+, External Profile)

Multi-pod deployment uses a StatefulSet with one pod per shard group, fronted by a headless Service for direct pod addressing. Clients use Redis Cluster protocol to discover and route to the correct pod.

### 10.3 Configuration

Embedded profile:

```yaml
profile: embedded

cluster:
  enabled: false

hot:
  backend: builtin_hashmap
  max_memory_bytes: 4294967296
  default_eviction_seconds: 86400
  eviction_policy: lru
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300

cold:
  backend: builtin_rocksdb
  data_path: /data/cold
  compaction_style: level
  write_buffer_size_bytes: 67108864
  max_write_buffer_number: 4
  bloom_filter_bits_per_key: 10
  ttl_expiry:
    active_enabled: true
    sample_size: 20
    base_interval_ms: 1000
    disk_pressure_threshold: 0.9

queue:
  backend: builtin_wal
  wal_path: /data/wal
  segment_size_bytes: 67108864
  min_retention_seconds: 86400
  offset_fsync_interval_ms: 1000
  wal_fsync_policy: group_commit
  group_commit_interval_us: 1000
  group_commit_max_bytes: 1048576

cold_consumer:
  quiet_threshold_seconds: 30
  safety_margin_seconds: 300
  deadline_jitter_ratio: 0.5
  buffer_high_water_bytes: 536870912
  max_flush_batch_size: 10000

recovery:
  replay_parallelism: 4
  hot_replay_batch_size: 10000
  cold_replay_batch_size: 50000

resp:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300

metrics:
  bind: 0.0.0.0
  port: 9090

admin:
  bind: 0.0.0.0
  port: 8080
```

External profile:

```yaml
profile: external

cluster:
  enabled: true
  shard_count: 64
  hash_function: xxhash
  this_pod_shards: [0, 1, 2, 3]

hot:
  backend: redis_client
  endpoint: "dragonfly.svc:6379"
  pool_size: 32
  default_eviction_seconds: 86400

cold:
  backend: redis_client
  endpoint: "kvrocks.svc:6666"
  pool_size: 16

queue:
  backend: kafka
  brokers: "kafka.svc:9092"
  topic: "abyss-log"
  partitions_per_shard: 1
  retention_ms: 86400000
```

### 10.4 Failure Modes

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Pod crash (embedded) | Hot store lost. WAL + cold intact on PVC. | Queue replay → hot rebuilt. Cold catches up. |
| Pod crash (external) | NTC orchestrator lost. External stores retain data. | Pod restarts, resumes queue consumption. |
| Cold store PVC full | Cold consumer stalls → queue grows → writes fail. | Provision more storage. |
| Queue WAL PVC full | Queue append fails → writes return errors. | Provision more WAL storage or speed up cold consumer. |
| Cold consumer lag > eviction | Reads may miss hot and cold. Data in queue/buffer. | Cold consumer catches up. Buffer serves reads during gap. |
| Hot store memory pressure | LRU evicts early. Reads fall through to buffer/cold. | Provision more memory. |

## 11. Observability

### 11.1 Prometheus Metrics

**Latency histograms:**
- `abyss_hot_op_duration_seconds{cmd="..."}`
- `abyss_cold_op_duration_seconds{cmd="..."}`
- `abyss_buffer_op_duration_seconds{cmd="..."}`
- `abyss_resp_request_duration_seconds{cmd="..."}`
- `abyss_queue_append_duration_seconds`

**Consumer lag:**
- `abyss_hot_consumer_lag_entries`
- `abyss_cold_consumer_lag_entries`
- `abyss_cold_buffer_oldest_entry_age_seconds` — most critical metric
- `abyss_hot_consumer_seq`, `abyss_cold_consumer_seq`

**Counters:**
- `abyss_hits_total{tier="hot|buffer|cold"}`, `abyss_misses_total`
- `abyss_queue_appended_total`
- `abyss_cold_flush_total{status="success|failure"}`
- `abyss_cold_flush_reason_total{reason="quiet|deadline|pressure"}`
- `abyss_cold_flush_batch_size` (histogram)
- `abyss_ttl_expired_total{tier="hot|cold"}`
- `abyss_evicted_total` — keys moved from hot to cold-only
- `abyss_promotions_total` — cold hits promoted back to hot

**Gauges:**
- `abyss_hot_memory_bytes`, `abyss_hot_keys`
- `abyss_cold_disk_bytes`, `abyss_cold_keys`
- `abyss_queue_depth`, `abyss_queue_disk_bytes`
- `abyss_cold_buffer_entries`, `abyss_cold_buffer_bytes`

### 11.2 Health Endpoints

| Endpoint | Port | Purpose |
|----------|------|---------|
| `GET /healthz` | 8080 | Liveness: process alive, RESP bound |
| `GET /ready` | 8080 | Readiness: recovery complete |
| `GET /metrics` | 9090 | Prometheus scrape target |
| `GET /status` | 8080 | JSON: component stats, consumer positions, lag |

### 11.3 Logging

JSON structured logs. Key events: cold consumer flush cycles (reason, batch size, latency), consumer lag transitions, queue/disk space warnings, recovery progress, TTL expiry scan results, cluster topology changes.

## 12. Performance Targets

| Metric | Target |
|--------|--------|
| Hot read (embedded) | < 100 μs p99 |
| Hot write (queue append + ACK, embedded) | < 50 μs p99 |
| Buffer read | < 50 μs p99 |
| Queue append (external) | < 1 ms p99 |
| Cold read | < 5 ms p99 |
| Cold batch write (10K ops) | < 50 ms p99 |
| Recovery (24h queue, 1M entries) | < 60 s |
| Write throughput (embedded) | > 100K ops/s |
| Write throughput (external) | > 50K ops/s |

## 13. Build & Dependencies

### 13.1 Language & Toolchain

- **Language**: C++20
- **Build system**: CMake + vcpkg/Conan
- **Compiler targets**: GCC 13+, Clang 17+
- **Container**: Distroless or Alpine, target < 50 MB

### 13.2 Dependencies

| Dependency | Purpose | License |
|------------|---------|---------|
| RocksDB | Built-in cold store | Apache 2.0 / GPL 2.0 |
| xxHash | Key hashing / shard routing | BSD |
| hiredis | RESP parsing, external Redis client | BSD |
| liburing | io_uring async I/O | LGPL / MIT |
| spdlog | Structured logging | MIT |
| prometheus-cpp | Metrics export | MIT |
| protobuf | Queue WAL entry serialisation | BSD |
| yaml-cpp | Configuration | MIT |
| googletest | Testing | BSD |
| benchmark | Microbenchmarks | Apache 2.0 |

Optional (external profile): `librdkafka` (BSD), `nats.c` (Apache 2.0).

## 14. Testing Strategy

### 14.1 Unit Tests

- Interface implementations tested via parameterized suites
- Tiering engine + consumer logic with mock stores and queues
- RESP parser with protocol conformance vectors
- TTL semantics (eviction vs absolute ttl, eviction refresh on reads)
- Compaction buffer: scalar last-write-wins
- Compaction buffer: set merge-accumulate (SADD/SREM interleaving)
- Compaction buffer: sorted set merge (ZADD/ZREM, score dedup)
- Compaction buffer: DEL resets all prior state
- Flush strategy: quiet window detection
- Flush strategy: deadline flush with jitter
- Flush strategy: memory pressure triggers aggressive flush
- Promise-based write ACK lifecycle
- Shard routing correctness (xxHash consistency)
- Consumer lag detection with synthetic clock

### 14.2 Integration Tests

- End-to-end: Redis client → Abyss → verify read-after-write
- Recovery: write, kill, restart, verify all non-expired keys available
- Cold consumer quiet window: write rapidly, stop, verify flush after threshold
- Cold consumer deadline: write continuously, verify deadline flush fires
- Cold consumer compaction: write same key 10K times, verify single cold write
- Buffer reads: verify reads hit buffer for keys evicted from hot but not yet cold
- WAL segment rotation: verify old segments cleaned up after ack
- Profile switching: same suite against embedded, external, hybrid
- Promotion: cold hit generates queue entry, hot consumer applies, key survives restart
- Multi-key fan-out: MGET across hot/buffer/cold tiers

### 14.3 Performance Tests

- Sustained throughput at target ops/s for 1 hour
- Cold consumer batching efficiency under various write rates
- Recovery time vs queue depth
- Memory stability over 24h
- Hot-key workload: measure tail latency under lock contention

### 14.4 Chaos Tests

- Kill pod mid-flush, verify recovery correctness
- Kill pod during compaction buffer flush
- Corrupt WAL segment, verify graceful skip to next valid segment
- Fill cold store PVC, verify error propagation
- Fill queue WAL PVC, verify error propagation
- Slow disk on cold store, verify lag metrics and buffer growth
- Memory pressure: push buffer past high-water, verify aggressive flush

## 15. Milestones

### Phase 1: Core (Embedded Profile, Single Pod)

- [ ] `IQueue`, `IHotStore`, `IColdStore` interfaces (shard-aware)
- [ ] Built-in append-only WAL with segment rotation and offset persistence
- [ ] Group commit fsync with configurable policy
- [ ] Built-in hash map hot store with LRU eviction, eviction refresh on read, absolute TTL
- [ ] Built-in RocksDB cold store with dual TTL expiry (lazy + active)
- [ ] Hot consumer (eager, real-time, promise-based write ACK)
- [ ] Cold consumer with compaction buffer (quiet-window + deadline + jitter flush)
- [ ] Compaction: scalar last-write-wins, set/sorted-set merge-accumulate
- [ ] Tiering engine: read routing (hot → buffer → cold), write routing (→ queue)
- [ ] Cold-hit promotion via queue
- [ ] Two-TTL model: eviction + absolute ttl
- [ ] Recovery: queue replay to both consumers
- [ ] RESP2 frontend (parse, classify, route)
- [ ] Multi-key fan-out (MGET/MSET decomposition)
- [ ] Prometheus metrics and health endpoints
- [ ] Kubernetes StatefulSet + Helm chart
- [ ] Unit, integration, performance tests

### Phase 2: External Profile + Horizontal Scaling

- [ ] Redis client hot/cold store backend
- [ ] Kafka queue backend
- [ ] NATS JetStream queue backend
- [ ] Hybrid profile configuration
- [ ] Redis Cluster protocol (CLUSTER SLOTS, MOVED redirects)
- [ ] Shard routing with xxHash
- [ ] Multi-pod StatefulSet deployment
- [ ] Resharding via queue replay
- [ ] External profile integration tests

### Phase 3: Hardening

- [ ] Chaos test suite
- [ ] Grafana dashboard templates
- [ ] Alerting rules
- [ ] Operational runbook
- [ ] Long-duration soak tests (7+ days)
- [ ] Resharding under load testing

### Phase 4 (Future): Shared-Nothing Per-Core

- [ ] Evaluate Seastar or custom thread-per-core runtime
- [ ] Per-core memory arenas
- [ ] Per-core io_uring instances
- [ ] Per-core queue shards
- [ ] Cross-core message passing for promise fulfillment
- [ ] Benchmarks demonstrating improvement over Phase 1 threading model

## 16. Open Questions

These should be resolved during early implementation, not before starting.

| # | Question |
|---|----------|
| 1 | **Quiet threshold defaults.** Percentage of eviction with a floor, or static value? Determine during benchmarking with real workload patterns. |
| 2 | **Thundering herd cap.** Should we cap max keys flushed per deadline cycle, letting overflow spill to the next cycle? Benchmark the cold consumer under burst writes to decide. |
| 3 | **Resharding snapshot optimisation.** For large external queue topics, full replay on resharding may be slow. Evaluate "snapshot cold store + replay recent" as an optimisation if replay times are unacceptable. |

## 17. Success Criteria

1. **Latency**: Hot reads < 100μs p99, cold reads < 5ms p99
2. **Durability**: Zero data loss across pod restart for non-expired keys. Clients never receive OK for a write that is subsequently lost.
3. **Throughput**: > 100K ops/s sustained on embedded profile
4. **Recovery**: Pod restart to ready in < 60 seconds for 24h queue window
5. **Simplicity**: Embedded profile deployable with zero external dependencies beyond K8s and PVCs
6. **Compatibility**: Standard Redis client libraries connect and work. Redis Cluster clients route correctly in multi-pod mode.
7. **Observability**: Cold consumer lag and buffer age always visible. Alerts fire before read degradation window.
8. **Efficiency**: Cold consumer flush-reason metrics show majority quiet-window flushes (not deadline) under normal workloads, indicating the compaction buffer is working as intended.
