# Requirements

## Design Principles

These are the invariants that all components must uphold. They are not guidelines — they are hard constraints.

**The queue is the single source of truth.** Every write goes to the queue. There is no dual write. The hot store and cold store are materialised views of the queue. If they diverge, the queue is correct.

**A client never receives OK for a write that is subsequently lost.** A write is acknowledged only after the queue append is durable (fsync complete for group commit) and the hot consumer has applied it. If the process crashes between these two events, the client never saw OK, so it can retry.

**Consumers are independent.** The hot consumer and cold consumer read from the queue at their own pace. Neither blocks the other. The hot consumer is always near the head. The cold consumer deliberately lags to accumulate writes for compaction.

**Recovery is pure queue replay.** The cold store is never read during recovery. Both consumers rebuild their state entirely from the queue. This eliminates consistency concerns between the queue and the stores.

**No magic resource management.** If you run out of disk, writes fail. If you under-provision memory, reads degrade. Abyss surfaces resource pressure through metrics and errors rather than silently degrading.

## Performance Targets

| Metric | Target |
|--------|--------|
| Hot read (embedded) | < 100 us p99 |
| Hot write (queue append + ACK, embedded) | < 50 us p99 |
| Buffer read | < 50 us p99 |
| Queue append (external) | < 1 ms p99 |
| Cold read | < 5 ms p99 |
| Cold batch write (10K ops) | < 50 ms p99 |
| Recovery (24h queue, 1M entries) | < 60 s |
| Write throughput (embedded) | > 100K ops/s |
| Write throughput (external) | > 50K ops/s |

## Durability Guarantees

**Group commit (default):** Writes are batched within a configurable window and fsynced together. The client blocks until its batch is fsynced. Maximum data loss on crash is limited to writes in the current unfsynced batch — but those writes were never acknowledged to the client.

| Fsync Policy | Throughput | Max Data Loss on Crash | Use Case |
|-------------|-----------|----------------------|----------|
| `fsync_per_write` | ~1K ops/s | 0 | Safety-critical |
| `group_commit` (default) | ~50-100K ops/s | Up to `group_commit_interval` of un-ACKed writes | Most workloads |
| `fsync_none` | ~500K+ ops/s | All un-flushed WAL data | Ephemeral data |

The key guarantee: any write the client received OK for is durable. Group commit only risks losing writes that were in the batch buffer at crash time and hadn't been fsynced or ACKed to the client yet. The client never saw OK for those, so it can retry.

## Concurrency Model

### Phase 1 Threading

- **RESP I/O threads** (pool, sized to core count) — accept connections, parse commands, route to tiering engine.
- **Hot consumer thread** (single, dedicated) — tails queue, applies to hot store, fulfils Consumer RPC promises for unconditional writes.
- **Cold consumer thread** (single, dedicated) — tails queue into compaction buffer, flushes to cold.
- **Resolver thread** (single, dedicated) — tails queue, resolves conditional writes, emits `Resolved` entries, fulfils Consumer RPC promises for conditional writes.
- **Background threads** — WAL segment cleanup, cold store compaction, TTL expiry scanning.

### Lock Discipline

**Hot store:** Sharded lock scheme (lock striping by shard, where the shard is derived from the key's CRC16 slot — see [ADP-014](proposals/014-slot-routing-and-topology.md)). I/O threads acquire a shared lock on the relevant shard for reads. The hot consumer acquires an exclusive lock for writes. The number of lock shards is fixed at deployment and should equal the planned horizontal shard count to ease migration to Phase 2.

**Compaction buffer:** `shared_mutex`. I/O threads acquire a shared lock for reads. The cold consumer acquires an exclusive lock when absorbing new entries or removing flushed entries.

**Consumer RPC registry:** `mutex`. The write handler registers a promise keyed by sequence id (or RPC id); the responsible consumer (hot for unconditional writes, Resolver for conditional writes, any consumer for admin RPCs) fulfils it. Short critical section — insert or erase from an unordered map. See [ADP-011](proposals/011-conditional-writes-and-consumer-rpc.md).

### Phase 2+ Threading

Phase 2 does not change the threading model within a pod. Each pod runs the same thread structure as Phase 1. Horizontal scaling is achieved by partitioning the keyspace across pods, not by changing the concurrency model. See [ADP-008](proposals/008-horizontal-scaling.md).

## Consumer Coordination

The queue retains entries until both consumers have acknowledged. Under normal operation, the cold consumer lags behind the hot consumer by up to the eviction window (since it uses that window to accumulate and compact writes before flushing).

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

This must fit on the WAL PVC (embedded) or within broker retention config (external). The config validator enforces this relationship at startup — `queue.min_retention_seconds < max(eviction)` is rejected, because a shorter retention silently degrades recovery (queue entries for keys still in their hot residency get GC'd, so they never replay into hot).

## Backpressure

| Scenario | Impact | Resolution |
|----------|--------|------------|
| Queue full (embedded WAL disk full) | `append()` returns error. Writes fail. | Provision more WAL storage or speed up cold consumer to allow segment cleanup. |
| Hot consumer stalled | Promise times out. Write returns Redis error. Write is durable in queue, will be applied when hot consumer recovers. | Investigate hot consumer. Self-regulating: write latency increases as promises wait. |
| Cold consumer falling behind | Warning metrics fire. Hot store continues serving reads. No writes blocked. | Speed up cold consumer or extend `eviction`. |
| Cold store disk full | Cold consumer's `apply_batch()` fails. Cold consumer stalls. Queue grows. Eventually queue fills and writes fail. | Provision more cold storage. |
| Hot store memory pressure | LRU evicts keys before `eviction` expires. Reads for evicted keys fall through to buffer then cold. | Provision more hot store memory or tune eviction. Data is safe — in queue and eventually in cold. |

## Success Criteria

1. **Latency** — hot reads < 100us p99, cold reads < 5ms p99.
2. **Durability** — zero data loss across pod restart for non-expired keys. Clients never receive OK for a write that is subsequently lost.
3. **Throughput** — > 100K ops/s sustained on embedded profile.
4. **Recovery** — pod restart to ready in < 60 seconds for a 24-hour queue window.
5. **Simplicity** — embedded profile deployable with zero external dependencies beyond Kubernetes and PVCs.
6. **Compatibility** — standard Redis client libraries connect and work. Redis Cluster clients route correctly in multi-pod mode.
7. **Observability** — cold consumer lag and buffer age always visible. Alerts fire before read degradation window.
8. **Efficiency** — cold consumer flush-reason metrics show majority quiet-window flushes (not deadline) under normal workloads, indicating the compaction buffer is working as intended.

## Milestones

### Phase 1: Core (Embedded Profile, Single Pod)

- Queue, HotStore, ColdStore interfaces (shard-aware)
- Built-in append-only WAL with segment rotation and offset persistence
- Group commit fsync with configurable policy
- Built-in hash map hot store with LRU eviction, eviction refresh on read, absolute TTL
- Built-in RocksDB cold store with dual TTL expiry (lazy + active)
- Hot consumer (eager, real-time, promise-based write ACK)
- Cold consumer with compaction buffer (quiet-window + deadline + jitter flush)
- Compaction: scalar last-write-wins, set/sorted-set merge-accumulate
- Tiering engine: read routing (hot → buffer → cold), write routing (→ queue)
- Cold-hit promotion via queue
- Two-TTL model: eviction + absolute TTL
- Recovery: queue replay to both consumers
- RESP2 frontend (parse, classify, route)
- Multi-key fan-out (MGET/MSET decomposition)
- Prometheus metrics and health endpoints
- Kubernetes StatefulSet + Helm chart
- Unit, integration, performance tests

### Phase 2: External Profile + Horizontal Scaling

- Redis client hot/cold store backend
- Kafka queue backend
- NATS JetStream queue backend
- Hybrid profile configuration
- Redis Cluster protocol (CLUSTER SLOTS, MOVED redirects)
- Slot-range shard ownership across pods (routing by CRC16 slot, [ADP-014](proposals/014-slot-routing-and-topology.md))
- Multi-pod StatefulSet deployment
- Resharding via queue replay
- External profile integration tests

### Phase 3: Hardening

- Chaos test suite
- Grafana dashboard templates
- Alerting rules
- Operational runbook
- Long-duration soak tests (7+ days)
- Resharding under load testing

### Phase 4 (Future): Shared-Nothing Per-Core

- Evaluate Seastar or custom thread-per-core runtime
- Per-core memory arenas
- Per-core io_uring instances
- Per-core queue shards
- Cross-core message passing for promise fulfillment
- Benchmarks demonstrating improvement over Phase 1 threading model

## Open Questions

These should be resolved during early implementation, not before starting.

| # | Question |
|---|----------|
| 1 | **Quiet threshold defaults.** Percentage of eviction with a floor, or static value? Determine during benchmarking with real workload patterns. |
| 2 | **Thundering herd cap.** Should we cap max keys flushed per deadline cycle, letting overflow spill to the next cycle? Benchmark the cold consumer under burst writes to decide. |
| 3 | **Resharding snapshot optimisation.** For large external queue topics, full replay on resharding may be slow. Evaluate "snapshot cold store + replay recent" as an optimisation if replay times are unacceptable. |
