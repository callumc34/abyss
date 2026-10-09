# Requirements

## Design Principles

These are the invariants that all components must uphold. They are not guidelines — they are hard constraints.

**The queue is the single source of truth.** Every write goes to the queue. There is no dual write. The hot store and cold store are materialised views of the queue. If they diverge, the queue is correct.

**No reply, to a read or a write, reflects a write that a failure in the configured durability class can lose.** In particular, a write is acknowledged only after its queue entry reaches the configured durability class and its effect is applied to hot. If the process fails before then, the client never saw OK and can retry. See [ADP-015](proposals/015-write-path-and-durability.md) §Durability classes and §Read visibility.

**Cold lag is decoupled from writes.** The cold consumer reads from the queue at its own pace and deliberately lags to accumulate writes for compaction. Its lag reaches the write path only through bounded, observable memory backpressure: hot cannot evict a key that cold has not drained. It never causes unbounded memory growth. (Until [ADP-015](proposals/015-write-path-and-durability.md) Phase 2 lands, hot is also a queue consumer that stays near the head.)

**Recovery is pure queue replay.** The cold store is never read during recovery. Both consumers rebuild their state entirely from the queue. This eliminates consistency concerns between the queue and the stores.

**No magic resource management.** If you run out of disk, writes fail. If you under-provision memory, reads degrade. Abyss surfaces resource pressure through metrics and errors rather than silently degrading.

## Performance Targets

A durable write cannot be acknowledged faster than the device's flush, so write targets are stated per durability class (see [ADP-015](proposals/015-write-path-and-durability.md)). The `power_loss` target is stated relative to the flush latency measured on the same volume in the same run. Where a target is measured is set out in [ADP-013](proposals/013-performance-harness.md).

| ID | Metric | Definition | Target |
|----|--------|------------|--------|
| H1 | Hot apply (engine) | In-process apply of one write to the hot store | < 5 us p99 |
| R1 | Hot read (embedded) | GET of a hot-resident key | < 100 us p99 |
| W1 | Write overhead | SET acknowledged at `process_crash`, in-process, at ≤ 50% of saturation | < 20 us p99 |
| W1-L | Write over loopback | SET acknowledged at `process_crash`, RESP over loopback, 64 connections, open-loop burst arrivals of 16 pipelined requests per connection, at ≤ 50% of saturation | p99 ≤ Valkey 8 (`appendfsync everysec`, I/O threads tuned) at the same offered load and shape, same run; provisional ceiling 250 us |
| R1-L | Read under writes | GET over loopback in a 90/10 GET/SET mix, 64 connections, pipeline depth 1, at ≤ 50% of saturation | p99 ≤ Valkey 8 at the same offered load and shape, same run; provisional ceiling 250 us |
| W2 | Durable write | SET acknowledged at `power_loss`, at ≤ 50% of saturation | ≤ 2 × measured device flush p99 + 50 us |
| W3 | Durable write throughput | The highest open-loop offered SET rate sustained with p99 within the W2 bound, found by a rate sweep, on a device whose flush p99 ≤ 1 ms (NVMe or provisioned cloud volume, named in the report). Concurrency is whatever the rate needs; no in-flight count is fixed, because Little's law ties it to latency. | > 100K ops/s |
| X1 | Comparative | The comparison matrix in [ADP-013](proposals/013-performance-harness.md) §Comparative baselines: Valkey 8 per durability class, and Dragonfly vs `process_crash` (labelled no-AOF), with tuning parity | On an 8-core host: lower p99 at the same offered load, and higher maximum throughput within the same latency SLO |
| B1 | Buffer read | Compaction-buffer point read | < 50 us p99 |
| C1 | Cold read | Cold point read | < 5 ms p99 |
| C2 | Cold batch write | 10K-op cold batch | < 50 ms p99 |
| Q1 | Queue append (external) | Broker produce acknowledged at the configured class | < 1 ms p99 |
| RC1 | Recovery | 24 h queue, 1M entries | < 60 s |
| T2 | Write throughput (external) | Sustained | > 50K ops/s |

## Durability Guarantees

Abyss acknowledges a write when it reaches the durability class set by `queue.durability` ([ADP-015](proposals/015-write-path-and-durability.md) §Durability classes):

| Class | Acknowledged when | A failure loses |
|-------|-------------------|-----------------|
| `process_crash` (default) | The entry is in the operating system's page cache | Nothing on a process crash, OOM kill or container restart. A node power loss loses at most the durability window. |
| `power_loss` | The fdatasync covering the entry has completed | Nothing on power loss |

- **Continuous flushing.** The log flushes continuously by natural batching, with no commit timer.
- **A bounded window.** Acknowledged but not yet power-durable data is bounded by `queue.durability_window_bytes` and `queue.durability_window_ms`. When the device cannot keep up, writes wait and then fail with an error, and metrics report the lag. The window never grows silently.
- **What replies can show.** No reply, to a read or a write, reflects a write that a failure in the configured class can lose.
- **What gets persisted.** No persisted derived state (the cold store, committed offsets) ever runs ahead of the power-durable log, whatever the class.

## Concurrency Model

### Phase 1 Threading

> **Changing under [ADP-015](proposals/015-write-path-and-durability.md).** The hot consumer and resolver threads go away (Phase 2). Writes are sequenced under the shard lock on the calling thread. Cold consumers become a pool sized to cores after that (#177). Each log already has one segment-preparer thread and one flusher thread. The list below describes current behaviour.

- **RESP I/O threads** (pool, sized to core count) — accept connections, parse commands, route to tiering engine.
- **Hot consumer thread** (single, dedicated) — tails queue, applies to hot store, fulfils Consumer RPC promises for unconditional writes.
- **Cold consumer thread** (single, dedicated) — tails queue into compaction buffer, flushes to cold.
- **Resolver thread** (single, dedicated) — tails queue, resolves conditional writes, emits `Resolved` entries, fulfils Consumer RPC promises for conditional writes.
- **Background threads** — WAL segment cleanup, cold store compaction, TTL expiry scanning.

### Lock Discipline

> **Changing under [ADP-015](proposals/015-write-path-and-durability.md).** The shard lock is taken by the per-shard sequencer, not a hot consumer, and becomes a spin-then-park lock with a constant-time critical section. The Consumer RPC registry is reduced to admin and flush use. The text below describes current behaviour.

**Hot store:** Sharded lock scheme (lock striping by shard, where the shard is derived from the key's CRC16 slot — see [ADP-014](proposals/014-slot-routing-and-topology.md)). I/O threads acquire a shared lock on the relevant shard for reads. The hot consumer acquires an exclusive lock for writes. The number of lock shards is fixed at deployment and should equal the planned horizontal shard count to ease migration to Phase 2.

**Compaction buffer:** `shared_mutex`. I/O threads acquire a shared lock for reads. The cold consumer acquires an exclusive lock when absorbing new entries or removing flushed entries.

**Consumer RPC registry:** `mutex`. The write handler registers a promise keyed by sequence id (or RPC id); the responsible consumer (hot for unconditional writes, Resolver for conditional writes, any consumer for admin RPCs) fulfils it. Short critical section — insert or erase from an unordered map. See [ADP-011](proposals/011-conditional-writes-and-consumer-rpc.md).

### Phase 2+ Threading

Phase 2 does not change the threading model within a pod. Each pod runs the same thread structure as Phase 1. Horizontal scaling is achieved by partitioning the keyspace across pods, not by changing the concurrency model. See [ADP-008](proposals/008-horizontal-scaling.md).

## Consumer Coordination

> **Changing under [ADP-015](proposals/015-write-path-and-durability.md).** Retention is gated by the persisted committed offsets of the retention consumers (cold, and the resolver until it is removed), per log segment and oldest first. In one physical log per volume, a stuck shard pins reclamation for that volume. The text below describes the retention bound, which is unchanged.

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
- Durability classes (`process_crash`, `power_loss`) with natural-batching group commit
- Built-in hash map hot store with LRU eviction, eviction refresh on read, absolute TTL
- Built-in RocksDB cold store with TTL expiry: reads answer nil for expired keys, and an active scanner deletes them by the log's clock
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
