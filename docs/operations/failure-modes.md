# Failure Modes

## Failure Scenarios

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Pod crash (embedded) | Hot store lost. WAL + cold store intact on PVC. | Pod restarts. Queue replay rebuilds hot store. Cold consumer catches up from its last ack point. |
| Pod crash (external) | In-process orchestrator lost. External stores (Redis, Kafka, KVRocks) retain data. | Pod restarts, resumes queue consumption. |
| Cold store PVC full | Cold consumer's `apply_batch()` fails. Cold consumer stalls. Queue grows. Eventually queue fills and writes fail. | Provision more cold storage. |
| Cold store cannot fsync (checkpoint fails) | The cold consumer applies flushes to the memtable but `Checkpoint()` (durable WAL fsync) fails, so it does **not** advance its WAL ack — the WAL is retained, not reaped. No acked write is lost; the queue keeps growing until the fsync path recovers. | Inspect `abyss_cold_checkpoint_total{status="failure"}` and the cold-store volume. Resolve the I/O fault; the ack resumes advancing once a checkpoint succeeds. |
| Poisoned cold flush batch | A structurally-undecodable op (`kCorruption`/`kInvalidArgument`) cannot be applied. The batch is reinserted and the loop backs off (capped exponential) instead of busy-spinning a core. The shard's WAL stays pinned below the poison. | Inspect `abyss_cold_consumer_backoff_total{reason="poisoned"}` and the `cold apply batch poisoned` CRITICAL log. Operator intervention required to clear the bad entry. |
| Queue WAL PVC full | Queue `Append()` fails. Writes return Redis errors to clients. | Provision more WAL storage or speed up cold consumer (allows segment cleanup). |
| Cold consumer lag > eviction | Reads may miss hot (evicted) and cold (not yet flushed). Data is in the queue/buffer. Buffer serves reads during the gap. | Cold consumer catches up. No data loss — buffer reads bridge the gap. |
| Cold scan exceeds the scan deadline | A large `SMEMBERS`/`ZRANGE`/`HGETALL` served from cold could not complete within `cold_scan_deadline`. The read fails closed with a timeout error to the client rather than returning a silently truncated result. | Inspect `abyss_cold_scan_deadline_exceeded_total`. Raise `cold_scan_deadline` for workloads with large cold-resident collections, or address the cold-volume I/O pressure (compaction, disk) that slowed the scan. |
| Hot store memory pressure | LRU evicts keys before their eviction deadline. Reads for evicted keys fall through to buffer then cold. | Provision more hot store memory or reduce eviction durations. Data is safe in queue and eventually in cold. |
| Active TTL scanner stalled | Expired-but-unread keys accumulate on disk. Lazy expiry still cleans them on read; storage drifts upward until reads happen or the scanner resumes. | Inspect `abyss_cold_ttl_*` metrics and `abyss.cold.ttl_scanner` logs. Confirm the scanner thread is alive and not pinned by sustained CAS conflicts. Restart resets the scanner state. |
| Data volume cannot make directory entries durable | The startup durability probe reports the WAL/data volume cannot `fsync` directories (FAT/exFAT, some network/overlay mounts). With any retention `fsync_policy` this is a **refuse-to-start** condition — a persisted ack could outrun durable storage, violating "no OK for a lost write". | Move the data directory to a volume that supports durable directory fsync (e.g. ext4/xfs/APFS/NTFS local disk). As a deliberate, durability-disabling override, set `fsync_policy: none`. Watch `abyss_fs_durable_dir_supported`. |

## Durability Capability Gate

Durability ("a write that returned OK survives power loss") depends on the data volume's
`fsync` actually reaching stable media — including the directory entry that links a freshly
created or renamed file. Not every volume can do this: FAT/exFAT, some network shares, and some
overlay/tmpfs mounts silently drop directory syncs.

Abyss makes this observable and fail-closed rather than silently degrading (invariant 5):

- At startup the WAL open path probes the data volume and emits
  `abyss_fs_durable_dir_supported` (1 = durable directory fsync available, 0 = not).
- The per-OS `fsync` backend is logged at startup (`fsync_backend` on the `WAL durability probe`
  line): macOS uses `F_FULLFSYNC` (the only Darwin call that pushes the drive cache to platter),
  Linux uses `fsync`, Windows uses `FlushFileBuffers` (rename durability via
  `MOVEFILE_WRITE_THROUGH`).
- If the volume cannot make directory entries durable **and** a retention `fsync_policy`
  (`per_write` or `group_commit`) is configured, startup fails with a `kFailedPrecondition`
  error rather than accepting writes it cannot honour.
- The same check is enforced at the point of every durability-critical atomic write (node
  identity, consumer offsets): a directory-sync-unsupported volume returns an error instead of
  reporting a durable commit.

To run on a volume that genuinely cannot provide durable directory fsync, set
`fsync_policy: none` — this disables durability by design and logs a CRITICAL warning at startup.

## Backpressure Cascade

The backpressure model is intentionally simple and cascading:

```
Cold store disk full
  → cold consumer stalls
    → queue entries accumulate (not acked by cold consumer)
      → queue WAL grows
        → WAL PVC fills
          → Append() fails
            → writes return errors to clients
```

There is no magic. Each stage is visible in metrics. Operators must provision resources or tune configuration to resolve the cascade.

## Write Failures

A write that fails at the queue level (disk full, I/O error) returns a Redis error to the client. The write was never committed to the queue, so no state is inconsistent.

A write that succeeds at the queue level but whose hot consumer promise times out returns a Redis error to the client. The write IS durable in the queue and WILL be applied eventually. The client received an error, so it may retry — the retry will be a duplicate write, which is safe because last-write-wins is the default semantic.

## Recovery After Crash

See [ADP-007](../design/proposals/007-recovery.md) for the full recovery design. Key points:

1. Recovery is pure queue replay. No external coordination.
2. During recovery, the RESP port returns `LOADING` errors.
3. The readiness probe (`/ready`) returns 503 until recovery is complete.
4. Recovery time is bounded by queue depth and replay batch sizes.

## Cold Consumer Stall

If the cold consumer stalls (cold store I/O errors, bugs, resource exhaustion):

- Hot store continues serving reads normally.
- The compaction buffer continues serving buffer-hit reads.
- The queue grows because the cold consumer isn't acking entries.
- Write throughput is unaffected until the queue fills.
- **Metric to watch:** `abyss_cold_buffer_oldest_entry_age_seconds` and `abyss_cold_consumer_lag_entries`.

The cold consumer stall is the most insidious failure because it has no immediate client-visible impact. Writes succeed, reads work (from hot + buffer). The danger is delayed: if the buffer eventually exceeds its high-water mark, it switches to aggressive flush mode. If the stall persists long enough, the queue fills and writes fail.

## Cold Durability Checkpoint

"Cold acked seq N" means **N is on cold's stable storage AND N is past the durable WAL tail** — never merely "handed to RocksDB". The cold consumer makes this true with a two-phase write/checkpoint:

- `ApplyBatch` is a cheap memtable write (`sync=false`); it does not fsync.
- `Checkpoint` (`FlushWAL(sync=true)`) makes every prior batch durable. It fires on a **bounded cadence** — at most every `checkpoint_max_flushes` applied batches or `checkpoint_min_interval` — so a burst of small flushes amortises into one fsync rather than an fsync-per-batch storm (`F_FULLFSYNC` is expensive on macOS).
- The cold WAL ack only advances to `min(low_water, last_checkpointed_seq, DurableSeq(shard))`. It can never outrun cold's own durable storage nor the durable WAL tail, so the segment reaper never releases WAL retention for data that is not yet durable on both tiers.

This is fail-closed (invariant 5): a failed or slow checkpoint **pins** the ack (back-pressure), it does not silently advance.

- **Metrics to watch:** `abyss_cold_checkpoint_total{status}` (success/failure), `abyss_cold_checkpoint_duration_seconds` (fsync cost), `abyss_cold_checkpoint_interval_seconds` (observed cadence — confirms the bound is being honoured), and `abyss_cold_consumer_backoff_total{reason}` (idle/poisoned/backpressure loop backoff).
- A rising checkpoint interval or duration is the early signal that the cold volume's fsync is becoming a throughput bottleneck before the WAL fills.

## Cold Read Deadline (Point Reads and Scans)

Every read served from the cold tier carries a finite deadline, so a cold read can never block a
reactor thread unboundedly under disk pressure or for a pathologically large key (invariant 5).
The bound is enforced inside the storage engine (`rocksdb::ReadOptions::deadline`), so it covers
both point reads and prefix scans — not just an ad-hoc wrapper.

There are two knobs because the two access shapes have different latency budgets:

- `cold_read_deadline` (default **5ms**) bounds cold **point reads** (GET/SISMEMBER/ZSCORE/HGET/
  HMGET/HEXISTS/SCARD/ZCARD/EXISTS). This is the ADP-003 <5ms p99 cold-read SLA.
- `cold_scan_deadline` (default **50ms**) bounds cold **collection scans** (SMEMBERS/ZRANGE/
  HGETALL/HKEYS/HVALS), whose latency scales with cardinality. Set it generously enough to serve
  your largest cold-resident collection.

This is fail-closed, not best-effort truncation: a scan that overruns its deadline returns a
timeout **error** to the client and increments `abyss_cold_scan_deadline_exceeded_total` — it
never returns a partial/silently-capped array that the client would mistake for the full set.

- **Metric to watch:** `abyss_cold_scan_deadline_exceeded_total`. A non-zero, rising rate means a
  legitimately-large collection is being capped by the deadline.
- **Tuning:** raise `cold_scan_deadline` for workloads with large cold collections, or relieve
  the cold-volume I/O pressure (compaction backlog, slow disk) that is slowing the scan. The
  deadline is best-effort at iterator-step granularity, so a single very large SST block read can
  overshoot slightly; size the deadline with margin rather than at the exact p99.

## Hot Consumer Stall

If the hot consumer stalls:

- Write promises time out. Clients receive Redis errors.
- Writes are still durable in the queue and will be applied when the consumer recovers.
- This is self-regulating: as promises time out, clients back off, reducing write pressure.
- **Metric to watch:** `abyss_hot_consumer_lag_entries`.

Hot consumer stalls have immediate client impact (write errors), which makes them easy to detect and respond to.
