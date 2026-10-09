# ADP-007: Recovery

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-10-09

> **Amended by [ADP-015](015-write-path-and-durability.md).** Recovery flushes the retained log before replay, so the power-durable watermark is known. Cold and hot replay through one demultiplexing scan of each log. The log records decided effects, so replay applies them and re-decides nothing: the resolver replay phase of [ADP-011](011-conditional-writes-and-consumer-rpc.md) is removed. The sections below describe this.

## Context

When an Abyss pod restarts, the hot store is empty (it was in-memory) and the cold store may be stale (the cold consumer had buffered writes that weren't yet flushed). The queue is the source of truth. Recovery rebuilds both tiers from the queue.

Two things are rebuilt: cold, by the cold consumer ([ADP-004](004-cold-consumer.md)), and hot, by the hot replayer (§Hot replay). In steady state the sequencer applies every write to hot itself ([ADP-002](002-hot-store.md)), so the hot replayer exists only for recovery. Recovery is driven by a dedicated recovery coordinator that owns the phase ordering, captures per-shard targets, dispatches per-shard work through a shard scheduler, and exposes progress for `/status` and Prometheus.

## Design

### Recovery Process

```
Pod starts
  │
  ├─ 1. Open the queue (synchronous self-recovery)
  │     The embedded WAL backend scans each log once, demultiplexing frames
  │     into per-shard streams. It CRC-verifies the segments that can hold
  │     unflushed bytes, ends the log at the first unfilled or torn frame,
  │     drops an incomplete trailing batch, seals the recovered tail with
  │     a padding frame, syncs so every retained entry is power-durable,
  │     and loads the committed-offset checkpoint.
  │     Both durable ends start at the recovered head, so replay never
  │     waits on a flush. External queue backends (Kafka, NATS) typically
  │     no-op this phase. Phase surfaces as queue_open.
  │
  ├─ 2. Replay cold and hot in one scan
  │     The coordinator captures each shard's durable end. One Scan per
  │     log, from min(first retained seq, cold commit + 1) to that end,
  │     delivers each shard's entries in order, never concurrently for
  │     one shard. Each batch goes to cold first (the entries above cold's
  │     commit), then to the hot replayer (those at or above the first
  │     retained seq). The retained log is read about once.
  │     - Cold absorbs entries into its compaction buffer, flushing if the
  │       buffer crosses high-water. Expiry follows the log clock (ADP-003).
  │     - The hot replayer applies each frame through the sequencer's own
  │       apply, under the residency rule (§Hot replay).
  │     Phase surfaces as cold_hot_replay.
  │
  ├─ 3. Finish
  │     Each shard's cold consumer fails unless the scan reached the
  │     shard's end, then drains its buffer to the cold store and
  │     checkpoints. The hot replayer then checks that every shard got
  │     exactly its frames, and sweeps hot (§Hot replay).
  │
  ├─ 4. Resume normal operation
  │     Coordinator returns. The server starts the eviction worker and the
  │     per-shard cold consumer threads for steady-state tailing, and marks
  │     itself ready. The LOADING gate, backed by the coordinator's
  │     recovering flag, opens in the same observable transition; /ready
  │     flips to 200.
  │
  └─ During recovery: RESP port returns LOADING errors (narrow admin set
     remains available — see ADP-005). /ready reports 503 with
     recovery_complete=false. /status reports the live phase, per-tier
     entries_replayed/target counters, and elapsed_ms.
```

### Recovery Semantics

- The queue WAL or external broker retains all entries since the oldest un-acked position.
- For the embedded profile: cold store RocksDB survives on PVC. The cold consumer only replays entries since its last ack point.
- For the hot store: rebuilt from each shard's first retained entry, holding only keys whose whole history it replayed (§Hot replay). Every other key is served from buffer plus cold, which hold it whole once cold's replay finishes.
- **The cold store is never read during recovery.** Recovery is purely queue replay.
- Cold replay benefits from the compaction buffer — recovery write volume to cold is bounded by unique keys, not total queue entries.
- **A Flush during replay.** Hot wipes the shard at the Flush's seq, clears its stubs and load placeholders, and sets its flush floor, so no pre-Flush key survives, stub or not. Cold drops its compaction buffer and wipes its own shard's slice of the cold backend (ADP-010 §Per-shard wipe). Because each shard wipes only its slice, a lagging shard's replayed Flush cannot destroy a peer shard's post-Flush data that parallel replay has already flushed.

### Replay start positions

- Cold resumes one past its persisted committed offset, or at the first retained entry if it has none. Entries after the persisted offset may be replayed a second time, which is idempotent.
- Hot commits nothing. It rebuilds from the first retained entry.
- A retention consumer whose start position has already been reclaimed has lost data it never committed. It fails the process instead of skipping ahead.

### Hot replay

The hot replayer rebuilds hot from the scan, and nothing else feeds it. It applies each frame through the function the sequencer applies writes with, so replay and the write path cannot interpret a frame differently, and it re-decides nothing ([ADP-015](015-write-path-and-durability.md) invariant 4). It lives in the engine, not in hot, because making room may need cold, and hot sees cold only through cold's drained seq.

**The residency rule.**
- A frame flagged `kReplacesState` ([ADP-009](009-wal-format.md)) makes its key resident. The flag marks an effect that alone determines its key's state: a SET, a DEL, or a write that creates its key.
- A Flush wipes its shard and sets the flush floor (§Recovery Semantics).
- Any other frame applies only if its key is already resident in the rebuild, a tombstone included.
- Otherwise the frame is skipped. The key stays non-resident and is served from buffer plus cold, which hold it whole. Its stub, if any, is dropped, because the skipped write made it stale.

A key's oldest retained frame may be the tail of its history: an SADD whose earlier frames were reclaimed. Applying it would build a partial collection, the replay form of #163. The rule keeps every resident key complete (the residency invariant, [ADP-015](015-write-path-and-durability.md) §Residency invariant). It replaces the earlier per-entry skip rules, by absolute TTL and by eviction window, which could also build partial state and made replay's result depend on when it ran. `abyss_recovery_hot_skipped_frames_total` counts skipped frames; skipping is normal.

**Replay reads no clock.** A written key is linked at its frame's `appended_at`, and no TTL is judged: the log already carries every expiry a decision observed as an explicit DEL. Every frame, applied or skipped, raises its shard's `appended_at` stamp, so the first write after recovery is never stamped behind the log. After the scan, every replayed key's latest seq and each shard's flush floor are what the sequencer's apply set.

**Eviction during replay** is gated on cold's drain, as always. Limits are per shard: a shard's budget is `hot.max_memory_bytes` ÷ `hot.shard_count`, and its ratio limit is that budget × `hot.backpressure_ratio`.
- Cold is fed first in every batch, and its drained seq means absorbed into its buffer, so the drained seq covers each batch before hot applies it.
- Replay normally stays within the ratio limit. At the limit the replayer reclaims the shard's drained tombstones and evicts it to its budget.
- If nothing is evictable, it asks the shard's cold consumer to flush its buffer through the frame's seq, capped at the power-durable end, which recovery has synced, then evicts again. `abyss_recovery_cold_drain_requests_total` counts the requests. This forced flush is only a fallback: on a normal log it never fires, because with cold fed first everything hot holds is drained.
- If the shard is still over with nothing evictable after that flush, replay carries on and retries eviction with each new scan batch, so at most one batch's growth can take it over the ratio limit.
- Past a hard ceiling of twice the ratio limit, recovery fails loudly with an error naming the shard, the seq and the likely cause: cold's drained seq pinned, by a parse-poison entry for example, or one entry larger than the shard's budget. Recovery also fails if a forced flush fails, or makes no progress for `cold_consumer.drain_grace_seconds`.

**Every frame, in order, to the end.** Each frame must arrive at its shard's cursor, and at the end each shard must have had exactly as many frames as its end minus its first retained seq, applied or skipped. Otherwise recovery fails with an error naming the shard. Cold's finish likewise fails unless its cursor reached the shard's end.

**The sweep.** After the scan and every shard's cold finish, the replayer reads the steady and wall clocks once.
- It moves each replayed key's link and access stamp from `appended_at` *t* to steady now minus (wall now − *t*), never past steady now. The map is monotone, so every LRU list keeps its order.
- It then runs the normal maintenance passes: keys past their TTL, keys whose last write is past their eviction window, drained tombstones, then LRU down to `hot.max_memory_bytes`. Cold has finished by then, so everything it absorbed is drained and evictable.
- A key written 30 h before a restart, under a 24 h eviction window, is therefore not resident after recovery, and is served from cold.
- The sweep decides residency only. It never changes a value.

### Replay Ordering

Cold and hot replay **together**, each shard on one scan worker, bounded by `recovery.replay_parallelism`. Within each batch cold goes first.

Rationale:

- **Cold and hot together.** The LOADING gate prevents client reads during the entire recovery; therefore no consistency hole exists between "hot caught up" and "cold caught up" while the gate is closed. Running them sequentially would roughly double recovery wall time for no observable benefit. The gate flips off only after the scan, cold's finish and the hot sweep have completed.
- **Cold first in each batch.** Hot evicts only what cold has drained. Feeding cold first means a batch is drained by the time hot applies it, so replay stays within its memory limit without waiting on cold.
- **One scan, not per-shard reads.** In one log carrying every shard, frames are small and interleaved, so every page holds many shards' frames. Per-shard reads would read the whole retained log once per wave of `replay_parallelism` shards. Hot replays from each shard's first retained entry, and cold's commit can trail by up to its buffer's deadline, so both cover most of the log. The scan's walker reads headers only and hands per-shard batches of positions to decode workers, shard `s` always to worker `s mod replay_parallelism`. Hand-off is bounded in batch size and distance behind the walker, so decoding stays inside the walker's page-cache window. A queue without a log reads each shard in turn through the same interface.

### Replay Implementation

The recovery coordinator owns the phase state machine. It depends on:

- the queue, to capture each shard's durable end, its first retained seq and cold's committed offset, and to scan;
- the cold consumer pool, to replay each shard's cold consumer, and to force a flush when the hot replayer asks;
- the hot replayer, over the sharded hot store;
- a shard scheduler, abstract over per-shard work dispatch. A bounded thread pool is the implementation; a per-core runtime would plug into the same abstraction.

Each side exposes synchronous per-shard replay steps:

| Component | Steps | Done when |
|-----------|-------|-----------|
| Cold consumer | Begin replay, apply each scan batch, then finish | The scan reached the shard's end (otherwise an error), then the buffer drains to the cold store and checkpoints |
| Hot replayer | Begin, apply each scan batch, then finish | Every shard got exactly its frames (otherwise an error), then the sweep |

A cold wipe that keeps failing during the scan, or a forced flush that makes no progress, gives up after `cold_consumer.drain_grace_seconds` and fails recovery, rather than hanging every shard on its worker.

Cancellation is observable end-to-end: the server passes its SIGTERM-backed cancel flag to the recovery coordinator, which propagates it to the scan and to cold's replay. A cancel mid-replay fails recovery as unavailable; the server logs it, shuts down, and exits non-zero. /ready stays 503 until the process restarts.

### Configuration

```yaml
recovery:
  replay_parallelism: 4         # scan workers, at most one per shard
```

`recovery.hot_replay_batch_size` and `recovery.cold_replay_batch_size` are removed: recovery reads the log once, in the scan's batches. A configuration that still sets either fails to load, naming the key. The resolver's internal batch size went with the resolver.

### Observability

The recovery coordinator's snapshots expose:

- `phase` — `queue_open`, `cold_hot_replay`, or `complete`.
- `cold_entries_replayed` / `_target` — sum across shards of (cold's latest drained seq − starting committed offset) and (last seq to replay − starting committed offset).
- `hot_entries_replayed` / `_target` — frames the hot replayer has applied or skipped, and the sum across shards of end − first retained seq.
- `elapsed_ms` — wall time since the run started; resets on `complete`.

This snapshot is wired into `/status` under the `recovery: { ... }` section and serves as the source of truth for an operator inspecting a stalled recovery. Prometheus carries the same through `abyss_recovery_phase` (0 `queue_open`, 1 `cold_hot_replay`, 2 `complete`), `abyss_recovery_{cold,hot}_entries_{replayed,target}`, `abyss_recovery_hot_skipped_frames_total`, `abyss_recovery_cold_drain_requests_total` and `abyss_recovery_duration_seconds`.

### Queue Retention for Recovery

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

This must fit on the WAL PVC (embedded) or within broker retention config (external). If the queue does not retain enough entries, keys that were in the eviction window but whose queue entries have been reclaimed are not rebuilt in hot. They are still in cold (eviction waits for cold's drain, and cold's commit gates retention), so they are not lost: the first read after recovery reads them from buffer plus cold, and may fill hot ([ADP-006](006-read-write-paths.md) §Read Path).

## Invariants

1. Recovery is pure queue replay. The cold store is never read. Cold and hot rebuild their state entirely from the queue, and effects are re-applied, never re-decided.
2. One scan feeds cold and hot together, cold first in each batch, both gated by the LOADING signal.
3. Hot holds a key after recovery only if it replayed the key's whole history, starting from a `kReplacesState` frame; a Flush wipes its shard and sets the flush floor. Every other key is served from buffer plus cold.
4. Replay reads no clock. The sweep after it reads the clocks once, and decides residency, never values.
5. Cold and hot each get every one of a shard's frames from their start position to the captured end, in order; otherwise recovery fails.
6. During recovery, the RESP port returns `LOADING` errors for data-plane commands; a narrow admin set (see [ADP-005](005-resp-frontend.md)) remains available. The LOADING signal is raised while either the recovery coordinator or the queue reports that it is recovering.
7. After recovery, the readiness probe goes healthy and normal operation resumes. /ready reports `recovery_complete: true`.
8. Cancellation propagates from the server through the recovery coordinator to the scan and cold's replay. A cancelled recovery exits non-zero; partial acks are persisted, so a restart resumes from the last checkpoint.

## Trade-offs

**Why parallel cold and hot replay?** The LOADING gate prevents client reads during the entire recovery, so the original ADP-007 rationale ("cold before hot, otherwise reads might miss in hot and hit stale cold") does not apply — no client is reading. Sequential replay would roughly double wall-clock recovery time for an empty correctness benefit. Parallel matches the deployment-time concurrency (per-shard threads) and is bounded by the operator knob.

**Why a recovery coordinator rather than inline orchestration in server initialisation?** Recovery is a state machine with phases, error / cancel propagation, and live progress reporting. Inlining it bloats the server constructor and obstructs the testability of each phase. Extracting it as a dedicated component matches the pattern established by Kafka's log manager recovery, etcd's backend applier and RocksDB's database recovery. The shard scheduler abstraction further decouples the coordinator from the concurrency primitive: a per-core runtime swaps the scheduler implementation without touching the coordinator.

**Why a residency rule rather than skip rules?** The earlier rules skipped entries one at a time, by absolute TTL and by eviction window. A skipped entry could be the head of a key's history, so a later entry built partial state on top of nothing; and both rules read the wall clock, so the rebuilt hot store depended on when replay ran. The residency rule makes a key resident only from an effect that determines it whole, so a resident key is always complete. TTL and eviction are applied once, after the scan, by the sweep.

**Why rebuild hot at all rather than start it empty?** An empty hot store would make the first read of every key after a restart a cold read. Replaying keeps the recently written working set warm, and costs little: cold's replay reads most of the log anyway, and the scan reads it once for both.

**Why force cold to drain rather than wait for it?** The forced flush is a fallback for a drained seq that lags the scan; on a normal log, cold fed first has already drained everything hot holds. During recovery cold flushes its buffer only past its high-water mark, so a replayer that waited on cold could wait for a drain that never comes. An explicit, capped flush request makes progress certain where progress is possible, and the hard ceiling fails recovery loudly, with the likely cause, where it is not.

**Why use the compaction buffer during cold recovery?** Without the buffer, replaying N queue entries for a key that was written N times would produce N cold store writes. The compaction buffer collapses these into one write per key, making recovery write volume proportional to unique keys, not total queue depth. For a 24-hour queue with 1M entries but only 100K unique keys, this is a 10x reduction.

**Why bound parallelism via a shard scheduler rather than spawning per-shard threads unconditionally?** A 64-shard config on a 4-core pod would otherwise launch 64 simultaneous drain-and-apply threads, each I/O-bound on the WAL and RocksDB. The wall-clock benefit beyond disk parallelism is zero, while the context-switching cost is real. The bounded scheduler trades minimum theoretical concurrency for predictable behaviour on small pods. Operators can lift the cap when they have the hardware.
