# Failure Modes

## Failure Scenarios

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Pod crash (embedded) | Hot store lost. WAL + cold store intact on PVC. | Pod restarts. Queue replay rebuilds hot store. Cold consumer catches up from its last ack point. |
| Pod crash (external) | In-process orchestrator lost. External stores (Redis, Kafka, KVRocks) retain data. | Pod restarts, resumes queue consumption. |
| Cold store PVC full | Cold consumer's `apply_batch()` fails. Cold consumer stalls. Queue grows. Eventually queue fills and writes fail. | Provision more cold storage. |
| Cold store cannot fsync (checkpoint fails) | The cold consumer applies flushes to the memtable but `Checkpoint()` (durable WAL fsync) fails, so it does **not** advance its WAL ack — the WAL is retained, not reaped. No acked write is lost; the queue keeps growing until the fsync path recovers. | Inspect `abyss_cold_checkpoint_total{status="failure"}` and the cold-store volume. Resolve the I/O fault; the ack resumes advancing once a checkpoint succeeds. |
| Poisoned cold flush batch | A structurally-undecodable op (`kCorruption`/`kInvalidArgument`) cannot be applied. The batch is reinserted and the loop backs off (capped exponential) instead of busy-spinning a core. The shard's WAL stays pinned below the poison. | Inspect `abyss_cold_consumer_backoff_total{reason="poisoned"}` and the `cold apply batch poisoned` CRITICAL log. Operator intervention required to clear the bad entry. |
| Cold parse poison (undecodable WAL op) | A WAL entry the cold consumer cannot parse into a materialisable op (`ParseWriteOp` failure, empty command, or empty key). The cold view never advances its ack past the un-materialised seq — the WAL retains it for the whole shard — and the loop backs off instead of busy-spinning. No acked write is dropped; cold simply stops making forward progress on that shard until the entry is dealt with. | Inspect `abyss_cold_parse_poison_total` and the `cold parse poison; WAL retention pinned below seq` CRITICAL log (carries the shard + seq). The shard's WAL retention age / disk bytes will rise. See [Cold Parse Poison Quarantine](#cold-parse-poison-quarantine) below for the inspect / quarantine / skip recovery procedure. |
| Queue WAL PVC full | The segment preparer cannot create a spare, so the next rotation finds none. Appends wait for a spare until their deadline, then fail with "no spare WAL segment ready". Nothing is applied or acknowledged for a rejected write. | Provision more WAL storage or speed up cold consumer (allows segment cleanup). `abyss_wal_segment_prepare_failures_total` rises first, then `abyss_wal_spare_segments` falls to 0 and `abyss_wal_spare_waits_total` rises. |
| Spares exhausted during warm-up | Before retention first reclaims a segment, every new segment is zero-filled. A sustained write rate above about half the volume's bandwidth can outrun the preparer, with the same symptoms as a full PVC. | Lower the write rate, or provision more volume bandwidth. `abyss_wal_segments_grown_total` rising with spares at zero confirms it. It clears once recycling starts. |
| Cold consumer lag > eviction | Reads may miss hot (evicted) and cold (not yet flushed). Data is in the queue/buffer. Buffer serves reads during the gap. | Cold consumer catches up. No data loss — buffer reads bridge the gap. |
| Cold scan exceeds the scan deadline | A large `SMEMBERS`/`ZRANGE`/`HGETALL` served from cold could not complete within `cold_scan_deadline`. The read fails closed with a timeout error to the client rather than returning a silently truncated result. | Inspect `abyss_cold_scan_deadline_exceeded_total`. Raise `cold_scan_deadline` for workloads with large cold-resident collections, or address the cold-volume I/O pressure (compaction, disk) that slowed the scan. |
| Hot store memory pressure | LRU evicts keys before their eviction deadline. Reads for evicted keys fall through to buffer then cold. | Provision more hot store memory or reduce eviction durations. Data is safe in queue and eventually in cold. |
| Active TTL scanner stalled | Expired keys accumulate on disk; the scanner is the only path that deletes them. Reads still answer nil for them, so storage drifts upward but no reply is wrong. | Inspect `abyss_cold_ttl_*` metrics and `abyss.cold.ttl_scanner` logs. Confirm the scanner thread is alive and not pinned by sustained CAS conflicts. Restart resets the scanner state. |
| Idle shard keeps expired keys in cold | Cold deletes a key by TTL only once its shard's log clock passes the TTL. The clock is the `appended_at` of the shard's oldest unflushed write, or of its newest write when none is pending ([ADP-004](../design/proposals/004-cold-consumer.md) §Expiry). On a shard with no new writes the clock stops, so its expired keys stay on disk until the shard's next write. After a restart the clock starts at 0 until replay or new writes advance it. Reads answer nil throughout, so this costs space only. | None needed: the shard's next write advances its clock, and the scanner reclaims the keys. If cold disk use matters on a mostly idle keyspace, compare `abyss_cold_disk_bytes` with `abyss_cold_ttl_expired_total`. |
| Cold apply finds a key holding another type | A logged SADD, HSET or ZADD found its key holding another type in cold, live or expired. The write path logs a DEL before any add that changes a key's type, so the write path and cold disagree. Cold drops the other type and applies the add as logged, so the shard keeps draining. | Inspect `abyss_cold_apply_type_conflicts_total` and the ERROR log `cold apply: an add found its key holding another type` (carries the shard and the add's type). Treat any increase as a correctness investigation. |
| Data volume cannot make directory entries durable | The startup durability probe reports the WAL/data volume cannot `fsync` directories (FAT/exFAT, some network/overlay mounts). This is a **refuse-to-start** condition under both durability classes: a power loss could drop whole segments, far beyond the durability window. | Move the data directory to a volume that supports durable directory fsync (e.g. ext4/xfs/APFS/NTFS local disk; Docker Desktop bind mounts often do not, so use a named volume). Watch `abyss_fs_durable_dir_supported`. |
| WAL flush cannot keep up | The device flushes slower than writes arrive, so acknowledged-but-not-power-durable data grows until it hits `queue.durability_window_bytes` or `queue.durability_window_ms`. Writes then wait for a flush, and after `engine.write_timeout` fail with "WAL durability window full". Acknowledged data is untouched. | Inspect `abyss_wal_durability_lag_seconds`, `abyss_wal_unflushed_bytes`, `abyss_wal_flush_duration_seconds` and `abyss_wal_backpressure_*`. Provision a faster or higher-IOPS volume; widening the window trades a larger power-loss exposure for headroom. |
| WAL flush fails, or a mapped segment faults | An fdatasync of published WAL data returns an error, or a write into a mapped segment raises `SIGBUS`. The kernel may already have dropped the dirty pages, so the process terminates rather than let a later flush mark lost data durable. The pod restarts (CrashLoopBackOff if the fault persists) and recovery replays the log. | Look for the CRITICAL `fatal invariant breach; terminating` log naming the WAL flush. Check the volume for I/O errors before restarting. |
| WAL media corruption | A reader finds a CRC failure below the range recovery verifies, in bytes that were synced long ago. This is media corruption, not a torn write, so the process terminates, naming the log, segment ordinal and offset. It is never skipped as a poison entry. | Check the volume for I/O errors. The entry is lost from the log, so restore the volume from backup, or truncate the log at the reported position and accept the loss. |
| WAL from another format or layout | The WAL directory holds the format 1 layout (`shard-NNNN/`), a segment of another major version, or a frame of a kind this build does not know. Abyss refuses to start and names which. | There is no migration. Start with an empty WAL directory, or run the build that wrote it. |

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
  Linux uses `fsync` (`fdatasync` for WAL flushes), Windows uses `FlushFileBuffers` (rename
  durability via `MOVEFILE_WRITE_THROUGH`).
- If the volume cannot make directory entries durable, startup fails with a
  `kFailedPrecondition` error rather than accepting writes it cannot honour. This holds under
  both durability classes.
- The same check is enforced at the point of every durability-critical atomic write (node
  identity, consumer offsets): a directory-sync-unsupported volume returns an error instead of
  reporting a durable commit.

There is no override: a volume that cannot provide durable directory fsync cannot host the WAL.

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

A write rejected before it is published returns a Redis error to the client: disk full creating the next segment, an I/O error on the segment write, or the durability window still full at the command's deadline. The write was never committed to the queue, so no state is inconsistent.

A failed WAL flush or segment seal after publication terminates the process instead (see "WAL flush or segment seal fails" above); there is no error reply for it.

A write that is published but not durable at the acknowledgement class within `engine.write_timeout` returns a Redis error to the client. It is already in the queue and applied to hot, and becomes durable when the WAL flush catches up. Under `process_crash` a power loss before that flush can still drop it. The client received an error, so it may retry. The retry is a duplicate write, which is safe because last-write-wins is the default semantic.

A multi-key write (`MSET`, `MSETNX`, multi-key `DEL` and `UNLINK`, `RENAMENX`, `COPY`) is one decision and one reservation in the log, so a refused one applies none of its keys. Its keys must share a WAL log: with `queue.log_count` above 1, one whose keys span logs is rejected with `-CROSSSLOT` and nothing is logged (#169). One whose frames cannot fit a single segment fails whole with a "batch exceeds the segment frame space" error; raise `queue.segment_size_bytes`.

FLUSHDB locks every shard and reserves one `Flush` per shard as a single reservation, so a refused FLUSHDB wipes nothing and readers see it whole. With one log the Flushes are one batch, atomic across a crash too. With several logs each log's Flushes are a batch of their own: a crash can keep one log's and lose another's, leaving the flush applied to some shards only. Retrying completes it, because a wipe is idempotent.

## Recovery After Crash

See [ADP-007](../design/proposals/007-recovery.md) for the full recovery design. Key points:

1. Recovery is pure queue replay. No external coordination.
2. During recovery, the RESP port returns `LOADING` errors.
3. The readiness probe (`/ready`) returns 503 until recovery is complete.
4. Recovery time is bounded by the retained log. Open scans each log once (CRC-verifying only the range that can hold unflushed bytes), and the hot and cold rebuild reads it about once more through one demultiplexing scan.
5. A torn write at a power loss, including a sector that persisted a new commit word over a recycled segment's old frame, ends the log cleanly at the tear. Writes acknowledged at `power_loss` are below it.

## Cold Consumer Stall

If the cold consumer stalls (cold store I/O errors, bugs, resource exhaustion):

- Hot store continues serving reads normally.
- The compaction buffer continues serving buffer-hit reads.
- The queue grows because the cold consumer isn't acking entries.
- Write throughput is unaffected until the queue fills.
- **Metric to watch:** `abyss_cold_buffer_oldest_entry_age_seconds` and `abyss_cold_consumer_lag_entries`.

The cold consumer stall is the most insidious failure because it has no immediate client-visible impact. Writes succeed, reads work (from hot + buffer). The danger is delayed: if the buffer eventually exceeds its high-water mark, it switches to aggressive flush mode. If the stall persists long enough, the queue fills and writes fail.

## Cold Parse Poison Quarantine

The cold consumer parses every WAL entry it drains with the SAME deterministic `ParseWriteOp`
the hot consumer uses. If an entry is structurally undecodable from cold's perspective — a parse
failure, an empty command, or an op with an empty primary key — it is a **poison**: a real
decoder/format-skew bug, because hot already accepted the same bytes. Silently skipping it would
let cold diverge from hot forever and would drop a delivered write from the cold view.

**A missing parser is not a poison.** Quarantine applies only when a parser exists and rejects
bytes hot accepted. If the command has no parser at all in this build, no tier could materialise
it — hot rejected it too — so hot and cold already agree that the entry produced no state, and
there is nothing for cold to be missing. Those entries are skipped and counted on
`abyss_cold_unsupported_op_total`. Conflating the two would let any client pin a shard's WAL
retention permanently by sending one command the registry advertises but the storage layer does
not implement, which is a denial of service rather than a safety property.

A rising `abyss_cold_unsupported_op_total` is not a data-loss signal, but it is a real defect
signal. Live traffic can no longer produce one: the registry only advertises an unconditional
write when a typed-operation parser backs it, and a unit test asserts that agreement. So a
non-zero counter on a running node means the log contains entries written by a build whose
command surface was wider than this one's — a downgrade, a mixed-version rollout, or a data
directory restored from a newer node. Check the binary version that wrote the affected segments
before assuming the entries are benign; they were skipped, which is safe for tier agreement but
means those writes are absent from both tiers.

Instead the cold consumer **quarantines** the poison (fail-closed, invariant 5):

- It does **not** advance its drained frontier or its persisted WAL ack past the poison seq. The
  ack is pinned at `poison_seq - 1` for the **whole shard** — the WAL (single source of truth)
  retains the un-materialised entry indefinitely.
- It increments `abyss_cold_parse_poison_total` and logs a CRITICAL line
  (`cold parse poison; WAL retention pinned below seq`) carrying the shard and seq.
- The drain/flush loop backs off on the capped exponential (`abyss_cold_consumer_backoff_total{reason="poisoned"}`)
  rather than busy-spinning, even though the queue keeps re-delivering the pinned entry.
- Writes **surrounding** the poison still materialise: a valid write after the poison is still
  absorbed and flushed to cold. Only the *ack frontier* is quarantined, not the data flow.

This is deliberately a hard stall on the shard's WAL reaping: a single poison entry at the
retention floor blocks segment cleanup for that shard, so **WAL retention age and disk bytes will
grow** until an operator intervenes. There is no automatic skip — skipping would silently discard
a delivered write.

**Observability (this is a fail-closed surface — wire it to alerting):**

- `abyss_cold_parse_poison_total` — a non-zero, rising value is the primary signal. Alert on
  `rate(abyss_cold_parse_poison_total[5m]) > 0`.
- The `cold parse poison; WAL retention pinned below seq` CRITICAL log — identifies the exact
  shard and seq to inspect.
- The WAL's retention gauges (`abyss_queue_disk_bytes`, and the oldest-eligible-unreaped age)
  rise because the ack cannot advance past the poison, and reclamation is oldest-first per log,
  so the poisoned shard holds back every later segment of its log. This is the disk-fill early
  warning before the WAL PVC fills.

**Recovery (inspect → quarantine → skip):**

1. **Inspect.** From the CRITICAL log, note the shard and poison seq. Read that WAL entry (offline
   WAL inspection tooling) and confirm it is genuinely undecodable — it almost always indicates a
   format/version skew between the writer and this cold build, which is a code bug to fix at the
   source, not a transient.
2. **Quarantine / fix forward.** The correct resolution for a decoder-skew poison is to deploy a
   cold build whose `ParseWriteOp` decodes the entry. On restart the cold consumer re-reads the
   pinned entry from the WAL, parses it, materialises it, and the ack resumes advancing — cold
   converges back to hot with no data loss.
3. **Skip (last resort, lossy).** If the entry is irrecoverably corrupt and cannot be decoded by
   any build, an operator must explicitly advance the cold ack past it (manual ack-offset
   override), accepting that the single write the entry carried is dropped from the cold view.
   This is the only escape and it is intentionally manual and explicit, because it discards a
   delivered write.

Do not raise the loop backoff ceiling as a "fix" — the backoff only prevents a busy-spin; it does
not clear the poison. The pin is released only by a successful parse-and-apply (fix forward) or an
explicit operator ack override (skip).

## WAL Retention Reclamation Stalled

The segment reaper reclaims a sealed WAL segment once, for every shard with frames in it, every
retention consumer's persisted offset has passed those frames, and its age exceeds
`min_retention`. Reclaimed segments are recycled for reuse, or unlinked when two already wait.
A removal can fail for reasons that have nothing to do with Abyss: a stale NFS handle, a
permissions change, a file still held open by an external process.

The sweep is **oldest-first per log**, and it stops at the first segment it cannot reclaim.
Reclaiming past it would leave a gap in some shard's retained entries, and hot's rebuild from
the first retained entry would replay an older write across the gap. A failing removal therefore
pins every later segment of that log. The failure is counted rather than swallowed, because an
un-reclaimable segment is real disk pressure and invariant 5 forbids degrading silently.

**Observability:**

- `abyss_queue_reaper_failures_total` — rising means removals are failing. Alert on
  `rate(abyss_queue_reaper_failures_total[15m]) > 0`.
- `abyss_queue_oldest_eligible_unreaped_age_seconds` — the leading indicator. Zero when nothing
  eligible is stuck; a steadily rising value means reclamation is falling behind and the WAL PVC
  will eventually fill. This is the gauge to page on, since a single stuck segment produces a
  bounded failure count but an unbounded age.
- `abyss_queue_disk_bytes` — confirms whether the stall is actually consuming disk.

Note that a rising unreaped age does **not** by itself mean the reaper is broken. A consumer that
legitimately has not acked yet — a lagging cold consumer, or a shard pinned by the poison
quarantine above — holds segments back by design. One such shard holds back every later segment
of its log, because a segment carries frames of every shard on the log. Check `abyss_queue_reaper_failures_total` first:
non-zero implicates the reaper, zero implicates a consumer that is not acking.

**Recovery:** inspect the first-error message in the reaper's log line for the failing path, and
resolve the underlying filesystem condition. The next sweep reclaims the segment with no operator
action beyond that: the log retries the stuck removal on every pass, and the sweep resumes from
the oldest segment once it succeeds.

## Cold Durability Checkpoint

"Cold acked seq N" means **N is on cold's stable storage AND N is past the durable WAL tail** — never merely "handed to RocksDB". The cold consumer makes this true with a two-phase write/checkpoint:

- `ApplyBatch` is a cheap memtable write (`sync=false`); it does not fsync.
- `Checkpoint` (`FlushWAL(sync=true)`) makes every prior batch durable. It fires on a **bounded cadence** — at most every `checkpoint_max_flushes` applied batches or `checkpoint_min_interval` — so a burst of small flushes amortises into one fsync rather than an fsync-per-batch storm (`F_FULLFSYNC` is expensive on macOS).
- Before any `ApplyBatch`, every entry the batch's effects came from must be power-durable in the WAL ([ADP-004](../design/proposals/004-cold-consumer.md) §Persisting at the power-durable log). RocksDB may persist an applied batch before any checkpoint, so the gate sits at the apply.
- The cold WAL commit only advances to `min(low_water, last_checkpointed_seq, DurableEnd(shard, power_loss) - 1)`. It can never outrun cold's own durable storage or the power-durable end of the WAL, so the segment reaper never releases WAL retention for data that is not yet durable on both tiers.

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

## Hot Memory Over Its Limit

Hot may exceed `hot.max_memory_bytes` only by what cold has not drained: eviction takes only keys cold has absorbed.

- Past `hot.max_memory_bytes` × `hot.backpressure_ratio` on a shard, a write that can grow memory (the `SET` family, `SADD`, `ZADD`, `HSET`, `HMSET`, `HSETNX`, `MSET`, `MSETNX`, `RENAMENX`, `COPY`, or one that must load a whole key) evicts what cold has drained, then waits for cold to drain more.
- At `engine.write_timeout` it is rejected with `-OOM command not allowed when hot memory is over its limit and cold is behind`, having applied and logged nothing.
- Deletes, removals, expiry changes and FLUSHDB never wait, unless one must first load a whole key from cold.
- **Metrics to watch:** `abyss_hot_backpressure_waits_total`, `abyss_hot_backpressure_rejections_total`, `abyss_hot_unevictable_bytes`, and the cold consumer's lag, which is the usual cause.
