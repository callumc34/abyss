# Architecture

## Overview

Abyss is a Kappa architecture: an append-only queue is the single source of truth, and two independent consumers materialise state from it into a hot (in-memory) store and a cold (on-disk) store.

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

Abyss stores opaque bytes — it is data-model agnostic. The RESP frontend accepts standard Redis commands and routes them through the system. Callers connect with any Redis client library.

## Components

**Queue** — the append-only log that serves as the single source of truth. Every write is committed here first. Both consumers read from the queue independently at their own pace. See [ADP-001](proposals/001-queue-wal.md).

**Hot Store** — an in-memory key-value store for low-latency reads. The hot consumer applies writes from the queue in real-time. Keys live in hot for their eviction duration, refreshed on every read. See [ADP-002](proposals/002-hot-store.md).

**Cold Store** — a durable on-disk key-value store. Data that has been evicted from hot remains readable in cold. The cold consumer writes to cold via a compaction buffer that collapses intermediate writes. See [ADP-003](proposals/003-cold-store.md).

**Cold Consumer** — reads from the queue into an in-memory compaction buffer, then flushes to cold when keys go quiet or approach their eviction deadline. The most architecturally significant component. See [ADP-004](proposals/004-cold-consumer.md).

**Resolver** — in-process consumer that evaluates conditional commands (`SET NX`, `ZADD GT`, `MSETNX`, etc.) against a tiered existence view and records the decision as a `Resolved` entry in the queue. Hot and cold consumers then apply the resolved decision rather than re-evaluating the predicate. Preserves Kappa semantics for conditional writes without split-brain between tiers. See [ADP-011](proposals/011-conditional-writes-and-consumer-rpc.md).

**RESP Frontend** — TCP listener implementing the RESP2 wire protocol. Classifies commands as reads, writes, or admin, and routes them accordingly. See [ADP-005](proposals/005-resp-frontend.md).

**Tiering Engine** — orchestrates the read path (hot → buffer → cold → nil) and write path (→ queue → consumer ACK → client OK). Manages Consumer RPC lifecycle (generalised write promise; see [ADP-011](proposals/011-conditional-writes-and-consumer-rpc.md)). See [ADP-006](proposals/006-read-write-paths.md).

## Data Flow

**Write path:** Client write → RESP frontend → queue append (durable) → hot consumer applies → client receives OK. The queue is the sole write path — there is no dual write. See [ADP-006](proposals/006-read-write-paths.md) for the full write acknowledgement flow.

**Read path:** Hot store → compaction buffer → cold store → nil. Hot hits refresh the eviction timer. Buffer hits do not promote (the cold consumer owns that data). Cold hits promote the key back through the queue so it re-enters hot with a fresh eviction timer. See [ADP-006](proposals/006-read-write-paths.md) for the full read path including promotion semantics.

**Recovery:** Replay the queue from the oldest un-acknowledged position. Both consumers rebuild their state from the log. The cold store is never read during recovery. See [ADP-007](proposals/007-recovery.md).

## Two-TTL Model

Abyss distinguishes between two independent time-to-live values:

| TTL Type | Semantics | On Expiry |
|----------|-----------|-----------|
| `eviction` | How long a key lives in the hot store. Refreshed on every read. | Key is evicted from hot. Remains available in cold. Data is not deleted — it moves tier. |
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

All major components implement C++ abstract interfaces. Implementations are selected at startup via configuration.

| Interface | Purpose | Defined in |
|-----------|---------|------------|
| `Queue` | Append-only log, consumer reads, offset management | `abyss/core/queue.h` |
| `HotStore` | In-memory KV operations, eviction, batch apply | `abyss/core/hot_store.h` |
| `ColdStore` | On-disk KV operations, compaction, batch apply | `abyss/core/cold_store.h` |

Interface details are in each component's design proposal. The interface definitions include command signatures, error semantics, and concurrency contracts.

## Deployment Profiles

Abyss supports three deployment profiles that select which implementations back each interface.

### Embedded (Default)

All components run in-process. Zero external dependencies. Single-pod only.

| Component | Implementation |
|-----------|---------------|
| Hot Store | Built-in concurrent hash map |
| Cold Store | Built-in RocksDB-backed store (on PVC) |
| Queue | Built-in append-only WAL on PVC |

### External

Each component delegates to an external system. Required for horizontal scaling.

| Component | Example Implementation |
|-----------|----------------------|
| Hot Store | DragonflyDB / Redis / Valkey |
| Cold Store | KVRocks / another Redis-compatible on-disk store |
| Queue | Kafka / Redpanda / NATS JetStream |

### Hybrid

Mix of embedded and external. For example: embedded hot store + NATS queue + external KVRocks cold store.

## Execution Model

### Phase 1: Single-Pod, Multithreaded

Phase 1 uses a conventional multithreaded model within a single pod:

- **RESP I/O threads** (pool, sized to core count) — accept connections, parse commands, route to tiering engine.
- **Hot consumer thread** (single, dedicated) — tails queue, applies to hot store, fulfils Consumer RPC promises for unconditional writes.
- **Cold consumer thread** (single, dedicated) — tails queue into compaction buffer, flushes to cold.
- **Resolver thread** (single, dedicated) — tails queue, resolves conditional writes, emits `Resolved` entries, fulfils Consumer RPC promises for conditional writes. See [ADP-011](proposals/011-conditional-writes-and-consumer-rpc.md).
- **Background threads** — WAL segment cleanup, cold store compaction, TTL expiry scanning.

Hot store access is protected by a sharded lock scheme (lock striping by shard, where the shard is derived from the key's CRC16 slot — see [ADP-014](proposals/014-slot-routing-and-topology.md)) to allow concurrent reads from I/O threads while the hot consumer applies writes. The compaction buffer uses a `shared_mutex` (concurrent reads from I/O threads, exclusive writes from cold consumer).

The number of lock shards is fixed at deployment and should equal the planned horizontal shard count to ease migration. Phase 1's internal sharding boundaries match Phase 2's pod boundaries.

### Phase 2+: Horizontal Scaling

See [ADP-008](proposals/008-horizontal-scaling.md) for the full horizontal scaling design, including Redis Cluster protocol, shard routing, and resharding.
