# ADP-007: Recovery

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-05-03

> **Amended by [ADP-015](015-write-path-and-durability.md).** Recovery flushes the retained log before replay, so the power-durable watermark is known; step 1 below already describes this. The resolver replay phase is removed, because the log records decided effects that replay applies without re-deciding (Phase 2). Until then the process below is current.

## Context

When an Abyss pod restarts, the hot store is empty (it was in-memory) and the cold store may be stale (the cold consumer had buffered writes that weren't yet flushed). The queue is the source of truth. Recovery rebuilds all consumers' state from the queue.

Phase 1 has three consumers: the Resolver ([ADP-011](011-conditional-writes-and-consumer-rpc.md)), the cold consumer ([ADP-004](004-cold-consumer.md)), and the hot consumer ([ADP-002](002-hot-store.md)). Recovery is driven by a dedicated `RecoveryCoordinator` that owns the phase ordering, captures per-shard targets, dispatches per-shard work through a `ShardScheduler`, and exposes progress for `/status` and Prometheus.

## Design

### Recovery Process

```
Pod starts
  │
  ├─ 1. Open the queue (synchronous self-recovery)
  │     The embedded WAL backend scans segments, validates per-entry CRCs,
  │     truncates a torn tail, flushes the recovered tail so every retained
  │     entry is power-durable, and loads the committed-offset checkpoint.
  │     Both durable ends start at the recovered head, so replaying recovered
  │     entries never waits on a flush; only entries replay itself appends
  │     (the resolver's re-emitted Resolveds) wait for their power
  │     durability. External queue backends (Kafka, NATS) typically no-op
  │     this phase. Phase surfaces as kQueueOpen on
  │     RecoveryCoordinator::Snapshot for consistency across backends.
  │
  ├─ 2. Capture initial targets and run resolver replay
  │     Coordinator captures TailSeq(s) per shard. For each shard, it
  │     submits Resolver::ReplayForRecovery(cancel) to the ShardScheduler.
  │     The resolver scans entries from its persisted ack to the captured
  │     tail, rebuilding the existence cache from Write/Conditional/Resolved
  │     entries. Conditionals whose matching Resolved is already in the log
  │     produce no new emission — the log carries the decision. Conditionals
  │     with no matching Resolved (the rare crash window between Conditional
  │     fsync and Resolved fsync) are re-decided and a fresh Resolved is
  │     emitted. The decision is deterministic given the log state up to the
  │     Conditional's seq, so the post-recovery outcome is identical to the
  │     pre-crash decision had the original Resolved fsynced.
  │     Phase surfaces as kResolverReplay.
  │
  ├─ 3. Recapture targets and run cold + hot replay in parallel
  │     Resolver replay may have extended the tail with new Resolved entries.
  │     The coordinator re-captures TailSeq(s) per shard so cold and hot drain
  │     through every entry, including those just emitted. For each shard,
  │     ColdConsumer::ReplayUntil(target) and HotConsumer::ReplayUntil(target)
  │     are submitted concurrently via the scheduler.
  │     - Cold absorbs entries into its compaction buffer, periodically
  │       flushing if the buffer crosses high-water, then drains the buffer
  │       to disk before declaring its shard done. Block-and-scan handles
  │       Conditional/Resolved pairs (ADP-011). Entries whose absolute TTL
  │       has expired are dropped pre-flush.
  │     - Hot applies entries in seq order. Skip-stale rules are active
  │       under replay_mode_:
  │       (a) entries whose appended_at + EvictionFor(key) < wall_now are
  │           skipped — they would have been evicted from hot in steady
  │           state and re-enter via cold-hit promotion on first read;
  │       (b) entries whose absolute TTL has expired are skipped — lazy
  │           expiry would produce the same end state.
  │       Skip increments are reported on the per-shard ConsumerSnapshot.
  │     Phase surfaces as kColdHotReplay.
  │
  ├─ 4. Resume normal operation
  │     Coordinator returns. The server starts the per-shard consumer
  │     threads for steady-state tailing, the eviction worker, and flips
  │     ready_=true. The LOADING gate (loading_->IsLoading() backed by
  │     coordinator.IsRecovering()) goes false in the same observable
  │     transition; /ready flips to 200.
  │
  └─ During recovery: RESP port returns LOADING errors (narrow admin set
     remains available — see ADP-005). /ready reports 503 with
     recovery_complete=false. /status reports the live phase, per-consumer
     entries_replayed/target counters, and elapsed_ms.
```

### Recovery Semantics

- The queue WAL or external broker retains all entries since the oldest un-acked position.
- For the embedded profile: cold store RocksDB survives on PVC. The cold consumer only replays entries since its last ack point.
- For the hot store: replays everything within the eviction window that hasn't absolutely expired. The hot store is fully reconstructed from the queue without reading cold.
- **The cold store is never read during recovery.** Recovery is purely queue replay.
- Cold replay benefits from the compaction buffer — recovery write volume to cold is bounded by unique keys, not total queue entries.
- **`entry::Flush` during replay.** Hot wipes its store and drops any pending Conditionals at seq < Flush.seq. Cold drops its compaction buffer and wipes its own shard's slice of the cold backend (ADP-010 §Per-shard wipe). Because each shard wipes only its slice, a lagging shard's replayed Flush cannot destroy a peer shard's post-Flush data that parallel replay has already flushed. Resolver clears its existence cache and emits Skip Resolveds for pre-Flush dangling Conditionals so hot/cold's block-and-scan can advance past them. While the Resolver is in replay mode and has observed a Flush at seq `F`, dangling Conditionals at seq > F are decided cache-only — the cold tier on disk reflects pre-Flush state until cold replay runs (which is sequenced after resolver replay), so a cache miss is treated as definitively absent rather than falling through to stale cold data.

### Replay start positions

- Cold and the resolver resume one past their persisted committed offset, or at the first retained entry if they have none. Entries after the persisted offset may be replayed a second time, which is idempotent.
- Hot commits nothing. It rebuilds from the first retained entry.
- A retention consumer whose start position has already been reclaimed has lost data it never committed. It fails the process instead of skipping ahead.

### Replay Ordering

Resolver replay completes before cold and hot. Cold and hot run **in parallel**, each per-shard, both bounded by `recovery.replay_parallelism` total scheduler workers.

Rationale:

- **Resolver before cold/hot.** Cold and hot consume `Resolved` entries via block-and-scan; a dangling Conditional whose Resolved hasn't yet been emitted would block them indefinitely. The resolver's replay re-emits any such Resolveds before cold/hot start, eliminating the dependency. Replaying the resolver first also re-warms its existence cache so post-recovery conditional writes do not pay cold-lookup latency on keys the resolver knew about pre-crash.
- **Cold and hot in parallel.** The LOADING gate prevents client reads during the entire recovery; therefore no consistency hole exists between "hot caught up" and "cold caught up" while the gate is closed. Running them sequentially would roughly double recovery wall time for no observable benefit. The gate flips off only after both pools' per-shard ReplayUntil have completed.

### Replay Implementation

`engine::RecoveryCoordinator` owns the phase state machine. It depends on:

- `core::Queue` — to capture per-shard `TailSeq`, `FirstSeq` and each retention consumer's `CommittedOffset`.
- `consumer::ResolverPool` / `ColdConsumerPool` / `HotConsumerPool` — to dispatch per-shard `ReplayForRecovery` / `ReplayUntil`.
- `engine::ShardScheduler` — abstract over per-shard work dispatch. Phase 1 ships `BoundedThreadShardScheduler`; the per-core / Seastar implementation in Phase 4 will plug into the same interface.

Each consumer exposes a synchronous per-shard replay primitive:

| Consumer | Method | Termination signal |
|----------|--------|--------------------|
| `Resolver` | `ReplayForRecovery(cancel)` | Queue read returns empty AND no dangling conditionals remain |
| `ColdConsumer` | `ReplayUntil(target, cancel)` | Its read position passes `target`, then the buffer drains to the cold store |
| `HotConsumer` | `ReplayUntil(target, cancel)` | Its read position passes `target` |

Cancellation is observable end-to-end: `Server::Run` passes the SIGTERM-backed `std::atomic<bool>` to `RecoveryCoordinator::Run`, which propagates it to every `ReplayUntil` / `ReplayForRecovery`. A cancel mid-replay returns `kUnavailable`, the server logs it, calls `Shutdown`, and exits non-zero. /ready stays 503 until the process restarts.

### Configuration

```yaml
recovery:
  replay_parallelism: 4         # ShardScheduler concurrency cap
  hot_replay_batch_size: 10000  # HotConsumer::ReplayUntil queue read max
  cold_replay_batch_size: 50000 # ColdConsumer::ReplayUntil queue read max
```

`resolver_replay_batch_size` is an internal default (5000); resolver scan latency is dominated by cache updates on the common path, so an operator knob would not move recovery time meaningfully.

### Observability

`RecoveryCoordinator::Snapshot()` exposes:

- `phase` — `kQueueOpen`, `kResolverReplay`, `kColdHotReplay`, or `kComplete`.
- `resolver_entries_replayed` / `_target` — sum across shards of (current resolver `latest_drained_seq` − starting ack) and (target − starting ack).
- `cold_entries_replayed` / `_target` — same shape, against cold's `latest_drained_seq`.
- `hot_entries_replayed` / `_target` — same shape, against hot's `HighestSettledSeq`.
- `elapsed_ms` — wall time since `Run()` started; resets on `kComplete`.

This snapshot is wired into `/status` under the `recovery: { ... }` section and serves as the source of truth for an operator inspecting a stalled recovery. The same fields will back the Prometheus metrics `abyss_recovery_phase`, `abyss_recovery_entries_replayed_total{tier=...}`, and `abyss_recovery_duration_seconds`.

### Queue Retention for Recovery

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

This must fit on the WAL PVC (embedded) or within broker retention config (external). If the queue does not retain enough entries, recovery may be incomplete — keys that were in the eviction window but whose queue entries have been garbage collected will be lost from hot. They will still be available in cold (the cold consumer flushed them before eviction), so they are not lost entirely, but the first read post-recovery will require a cold-path lookup and promotion.

## Invariants

1. Recovery is pure queue replay. The cold store is never read. All consumers rebuild their state entirely from the queue.
2. Replay order is resolver → (cold ∥ hot). Cold and hot run concurrently, both gated together by the LOADING signal.
3. Entries whose absolute TTL has expired at replay time are skipped.
4. For the hot consumer, entries whose eviction would have expired at replay time are skipped.
5. During recovery, the RESP port returns `LOADING` errors for data-plane commands; a narrow admin set (see [ADP-005](005-resp-frontend.md)) remains available. The LOADING signal is `coordinator.IsRecovering() || queue.IsRecovering()`.
6. After recovery, the readiness probe goes healthy and normal operation resumes. /ready reports `recovery_complete: true`.
7. The resolver emits no new `Resolved` entries during replay — the log already contains matching decisions from the original run — except for dangling Conditionals whose Resolved was never fsynced; those it re-decides deterministically and emits afresh.
8. Cancellation propagates from `Server::Run` through `RecoveryCoordinator::Run` to every per-shard `ReplayUntil`. A cancelled recovery exits non-zero; partial acks are persisted, so a restart resumes from the last checkpoint.

## Trade-offs

**Why parallel cold and hot replay?** The LOADING gate prevents client reads during the entire recovery, so the original ADP-007 rationale ("cold before hot, otherwise reads might miss in hot and hit stale cold") does not apply — no client is reading. Sequential replay would roughly double wall-clock recovery time for an empty correctness benefit. Parallel matches the deployment-time concurrency (per-shard threads) and is bounded by the operator knob.

**Why a `RecoveryCoordinator` rather than inline orchestration in `Server::Initialize`?** Recovery is a state machine with three phases, error / cancel propagation, and live progress reporting. Inlining it bloats the server constructor and obstructs the testability of each phase. Extracting it as a dedicated component matches the pattern established by Kafka (`LogManager.recover`), etcd (`applierV3backend`), and RocksDB (`DBImpl::Recover`). The `ShardScheduler` abstraction further decouples the coordinator from the concurrency primitive: Phase 4's per-core runtime swaps the scheduler implementation without touching the coordinator.

**Why skip expired entries during replay?** Applying a key whose TTL has already passed wastes work and fills the hot store with data that would be immediately evicted by lazy expiry. Skipping speeds up recovery and keeps the post-recovery hot store lean. The skip is gated on `replay_mode_` so steady-state writes — including a client `SET k v PX 1` whose TTL would fire microseconds after parse — are not affected.

**Why use the compaction buffer during cold recovery?** Without the buffer, replaying N queue entries for a key that was written N times would produce N cold store writes. The compaction buffer collapses these into one write per key, making recovery write volume proportional to unique keys, not total queue depth. For a 24-hour queue with 1M entries but only 100K unique keys, this is a 10x reduction.

**Why bound parallelism via `ShardScheduler` rather than spawning per-shard threads unconditionally?** A 64-shard config on a 4-core pod would otherwise launch 64 simultaneous drain-and-apply threads, each I/O-bound on the WAL and RocksDB. The wall-clock benefit beyond disk parallelism is zero, while the context-switching cost is real. The bounded scheduler trades minimum theoretical concurrency for predictable behaviour on small pods. Operators can lift the cap when they have the hardware.
