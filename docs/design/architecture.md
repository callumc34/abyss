# Architecture

## Overview

Abyss is a Kappa architecture: an append-only queue is the single source of truth. A per-shard sequencer decides each write, logs its effect and applies it to the hot (in-memory) store in one step, and an independent cold consumer materialises the log into the cold (on-disk) store.

```
                    ┌────────────────┐
  Redis Client ────▶│ RESP Frontend  │
                    └───────┬────────┘
                            │ write
                            ▼
                    ┌────────────────┐  apply   ┌──────────────┐
                    │   Sequencer    │─────────▶│  Hot Store   │
                    │  (per shard:   │          │ (in-memory)  │
                    │ decide, log,   │          └──────────────┘
                    │    apply)      │
                    └───────┬────────┘
                            │ append
                            ▼
                    ┌────────────────┐
                    │     Queue      │  ◄── Single source of truth
                    │ (append-only   │
                    │      log)      │
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │ Cold Consumer  │
                    │  (compacting)  │
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │   Cold Store   │
                    │   (on-disk)    │
                    └────────────────┘

  Redis Client ────▶ Read: Hot → Buffer → Cold → nil
```

Abyss stores opaque bytes — it is data-model agnostic. The RESP frontend accepts standard Redis commands and routes them through the system. Callers connect with any Redis client library.

## Components

**Queue** — the append-only log that serves as the single source of truth. Every write is logged here as the effect it decided. The cold consumer reads it at its own pace; hot is applied as each write is logged, and rebuilt from the log on recovery. See [ADP-001](proposals/001-queue-wal.md).

**Hot Store** — an in-memory key-value store for low-latency reads. The sequencer applies each write to hot as it logs it. Keys live in hot for their eviction duration, refreshed on every read of their value, and leave only once cold has drained their latest write; an evicted key may leave a small stub. See [ADP-002](proposals/002-hot-store.md).

**Cold Store** — a durable on-disk key-value store. Data that has been evicted from hot remains readable in cold. The cold consumer writes to cold via a compaction buffer that collapses intermediate writes. See [ADP-003](proposals/003-cold-store.md).

**Cold Consumer** — reads from the queue into an in-memory compaction buffer, then flushes to cold when keys go quiet or approach their eviction deadline. The most architecturally significant component. See [ADP-004](proposals/004-cold-consumer.md).

**Sequencer** — one per shard, run on the calling thread under the hot shard's exclusive lock. It decides each write against the key's complete state in hot (loading a non-resident key first, off the lock), reserves log space, applies the decided effects to hot and publishes the frames. Conditional writes (`SET NX`, `ZADD GT`, `MSETNX`, etc.) are logged as the effects they decided (decide-then-log), so hot, cold and recovery replay apply the same effects and none re-evaluates a predicate. A multi-key write takes its shards' locks in order and logs one batch. The sequencer replaced [ADP-011](proposals/011-conditional-writes-and-consumer-rpc.md)'s resolver and the trailing hot consumer. See [ADP-015](proposals/015-write-path-and-durability.md).

**RESP Frontend** — TCP listener implementing the RESP2 wire protocol. Classifies commands as reads, writes, or admin, and routes them accordingly. See [ADP-005](proposals/005-resp-frontend.md).

**Tiering Engine** — orchestrates the read path (hot → buffer → cold → nil) and the write path (sequencer → durability class → client reply). See [ADP-006](proposals/006-read-write-paths.md).

## Data Flow

**Write path:** Client write → RESP frontend → sequencer decides against hot, reserves, applies to hot and publishes → the frames reach the configured durability class → client receives the decided reply. The queue is the sole write path — there is no dual write. See [ADP-006](proposals/006-read-write-paths.md) for the full write acknowledgement flow.

**Read path:** Hot store → compaction buffer → cold store → nil. Hot hits refresh the eviction timer, except metadata reads (`EXISTS`, `TYPE`, `TTL`) and reads of a delete tombstone. A miss never waits: hot holds every key's complete state until cold has drained it (the residency invariant), so buffer plus cold are current for any key hot does not hold. A miss may fill hot with the loaded key directly; reads never write to the log. See [ADP-006](proposals/006-read-write-paths.md) for the full read path, including cache fill.

**Recovery:** One scan of each log feeds both tiers. Cold rebuilds from its committed offset, and the hot replayer rebuilds hot from each shard's first retained entry, applying the logged effects without re-deciding any. The cold store is never read during recovery. See [ADP-007](proposals/007-recovery.md).

## Two-TTL Model

Abyss distinguishes between two independent time-to-live values:

| TTL Type | Semantics | On Expiry |
|----------|-----------|-----------|
| `eviction` | How long a key lives in the hot store. Refreshed on every read of its value; `EXISTS`, `TYPE` and `TTL` do not count. | Key is evicted from hot never before its deadline, and lazily within one `eviction` window after it, plus up to one eviction tick and however long cold takes to drain its latest write. Remains available in cold. Data is not deleted — it moves tier. |
| `ttl` | Absolute time-to-live. How long the key exists at all. Not refreshed. | Key is deleted from both hot and cold. Data is gone. |

Behaviour matrix:

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

## Pluggable Interfaces

The queue and the cold store sit behind abstract interfaces, so another implementation can be added without changing the engine. Today the server builds one implementation of each; the `queue.backend` and `cold.backend` settings are reported in `/status`, not used to choose.

| Interface | Purpose | Defined in |
|-----------|---------|------------|
| `Queue` | Append-only log, consumer reads, offset management | `abyss/core/queue.h` |
| `ColdStore` | On-disk KV operations, compaction, batch apply | `abyss/core/cold_store.h` |

The hot store is not pluggable. Since ADP-015's Phase 2 it is in-process: the sequencer decides each write against it and applies the effects under its shard lock ([ADP-015](proposals/015-write-path-and-durability.md) §Sequenced write path), so the server always builds the built-in sharded store and every engine component takes that type directly. A `HotStore` interface (`abyss/core/hot_store.h`) still exists in the code, but nothing selects among implementations. Whether an external hot tier survives at all is an open decision, [#187](https://github.com/callumc34/abyss/issues/187).

Interface details are in each component's design proposal.

## Deployment Profiles

Three deployment profiles are planned. Only the embedded profile is implemented: the server refuses to start with any other `profile`.

### Embedded (Default)

All components run in-process. Zero external dependencies. Single-pod only.

| Component | Implementation |
|-----------|---------------|
| Hot Store | Built-in concurrent hash map |
| Cold Store | Built-in RocksDB-backed store (on PVC) |
| Queue | Built-in append-only WAL on PVC |

### External (planned)

The queue and cold store delegate to external systems. Required for horizontal scaling.

| Component | Example Implementation |
|-----------|----------------------|
| Hot Store | In-process, as in the embedded profile. An external hot tier is an open decision ([#187](https://github.com/callumc34/abyss/issues/187)). |
| Cold Store | KVRocks / another Redis-compatible on-disk store |
| Queue | Kafka / Redpanda / NATS JetStream |

### Hybrid (planned)

Mix of embedded and external. For example: the in-process hot store with a NATS queue and an external KVRocks cold store.

## Execution Model

### Phase 1: Single-Pod, Multithreaded

> **Changing under [ADP-015](proposals/015-write-path-and-durability.md).** Reactors will stop waiting on durability and cold reads (#161), and cold consumers will become a pool sized to cores (#177). The model below describes current behaviour until then.

Phase 1 uses a conventional multithreaded model within a single pod:

- **RESP I/O threads** (pool, sized to core count) — accept connections, parse commands and run each one through the tiering engine on the calling thread: a read, or a write's sequencer step (decide, reserve, apply to hot, publish) followed by its durable wait.
- **Cold consumer threads** (one per shard) — tail the queue into the compaction buffer, flush to cold.
- **Background threads** — the WAL flusher and segment preparer, WAL segment cleanup, hot eviction and tombstone reclamation, cold store compaction, TTL expiry scanning.

There is no hot consumer or resolver thread: the sequencer applies each write to hot as it logs it, on the calling thread. At startup, recovery's scan rebuilds hot through the hot replayer before any client is served.

Hot store access is protected by a sharded lock scheme (lock striping by shard, where the shard is derived from the key's CRC16 slot — see [ADP-014](proposals/014-slot-routing-and-topology.md)). Reads take a shard's lock shared; the sequencer takes it exclusively to decide, reserve and apply a write, and loads of non-resident keys run off the lock under per-key load tokens. The compaction buffer uses a `shared_mutex` (concurrent reads from I/O threads, exclusive writes from cold consumer).

The number of lock shards is fixed at deployment and should equal the planned horizontal shard count to ease migration. Phase 1's internal sharding boundaries match Phase 2's pod boundaries.

### Phase 2+: Horizontal Scaling

See [ADP-008](proposals/008-horizontal-scaling.md) for the full horizontal scaling design, including Redis Cluster protocol, shard routing, and resharding.
