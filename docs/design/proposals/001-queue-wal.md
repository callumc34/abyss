# ADP-001: Queue and WAL

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-10-09

> **Amended by [ADP-015](015-write-path-and-durability.md).** These sections already describe the amended behaviour:
> - **§Interface and §Offset persistence:** reads take an explicit consumer-owned position, and acknowledgements are committed offsets persisted lazily in one dual-slot checkpoint.
> - **§Durability classes and group commit:** writes are acknowledged at a named durability class, `process_crash` by default and `power_loss` opt-in, and flushed by natural batching.
> - **§Embedded WAL and §Batch atomicity:** recycled, memory-mapped segments in one physical log per volume carrying per-shard streams (#175).
> - **§Entry Types and §Interface:** the log carries only decided effects, `Write` and `Flush`, appended by the per-shard sequencer (#160). The `Conditional` and `Resolved` types are retired.

## Context

The queue is the single source of truth in Abyss. Every write is committed to the queue before it is applied to any store. The cold consumer reads from the queue independently, and hot and cold both rebuild their state from it on recovery. The queue's durability and ordering guarantees are the foundation of Abyss's correctness.

The queue interface must be shard-aware from the start to support horizontal scaling in Phase 2, even though Phase 1 runs on a single pod.

## Design

### Entry Types

Each log entry carries a type tag that determines how consumers process it. There are two variants; the set is closed and extended only by ADP:

| Type | Semantics | Written by | Read by |
|------|-----------|------------|---------|
| `Write` | One decided effect in canonical form (SET, DEL, SADD, PEXPIREAT, ...) | The sequencer | Cold; the hot replayer at recovery |
| `Flush` | FLUSHDB / FLUSHALL tombstone — wipes every key on the shard | The sequencer | Cold; the hot replayer at recovery |

The log records decided effects, not intents ([ADP-015](015-write-path-and-durability.md) §Sequenced write path). The sequencer decides a conditional command against the key's complete state and logs the `Write`s it decided on, or nothing. Replay applies them and decides nothing again. The `Conditional` and `Resolved` types, which carried an intent and its later decision ([ADP-011](011-conditional-writes-and-consumer-rpc.md)), are retired, and their type bytes stay reserved ([ADP-009](009-wal-format.md)). A `Write` whose effect alone determines its key's state carries the `kReplacesState` flag (ADP-009). The `Flush` variant is specified in [ADP-006](006-read-write-paths.md) §Broadcast write path; it is the queue-routed expression of FLUSHDB so all materialised views observe the wipe at the same logical position. The queue itself is agnostic to the semantics — it stores entries in order, preserves the type tag, and hands them to consumers unchanged.

### Interface

The queue entry (`QueueEntry`) is a struct with common metadata (sequence ID, wall-clock timestamp) and a payload variant that discriminates the entry types. Common fields are direct field accesses — no visitor needed just to read a sequence number. Type dispatch uses the variant only when consumers need to act on the payload.

The queue interface offers two append paths:

- **The sequencer's append** ([ADP-015](015-write-path-and-durability.md) §Sequenced write path). Every write the server takes goes through it, in three steps:
  - `Admit` waits for room in the shard's durability window, and `WaitForSpare` for a prepared segment, under no lock.
  - `Reserve` runs under the hot shard locks and never waits. It assigns each involved shard's seqs and reserves every part as one batch in one log, committing its frames in place until their total reaches 16 KiB. On failure nothing is taken and no seq is used: the window is full, no spare is ready, an entry is too large, or the shards span logs (`CROSSSLOT`). `ReserveFlush` reserves one `Flush` per shard, one batch per log.
  - `Complete` runs once the locks are released. It fills the reservation's remaining frames, publishes each shard once its earlier seqs are published, and returns the durability futures, which resolve when the entries reach the configured durability class (§Durability classes and group commit).
- **Two-phase and one-shot** — `BeginAppend` / `BeginAppendBatch` return a handle that publishes the entry when the caller publishes it, or on destruction if the caller drops it, so a forgotten publish degrades to a latency bug, never a lost write. `Append` / `AppendBatch` publish inline. The server's write path no longer uses them.

**Read position and committed offset are separate**, as a Kafka fetch position and committed offset are.
- **Read.** A consumer owns its read position and passes it to `Read`, together with the durability class the returned entries must have reached. `Read` returns the contiguous entries at or after that sequence that are durable at that class. A position below the oldest retained entry is an explicit out-of-range error, never silently moved forward.
- **Commit.** `CommitOffset` records how far a retention consumer has processed. It takes effect in memory at once, never passes the power-durable end of the log, and is persisted lazily (see "Offset persistence" below).
- **Durable ends.** `DurableEnd(shard, class)` is exclusive: every sequence below it is durable at that class, and 0 means none is. It is monotonic per shard and class. `AwaitDurable` waits for one sequence to reach a class, and `AckDurability` reports the class the queue acknowledges at.
- **Retention.** Committed offsets govern retention and where a consumer resumes after a restart. They never govern where a running consumer reads.
- **Hot.** Hot is not a consumer: the sequencer applies each write to it, and it commits nothing. After a restart the hot replayer rebuilds it from the oldest retained entry, through recovery's scan ([ADP-007](007-recovery.md) §Hot replay).
- **Accessors.** `FirstSeq` reports the lowest readable sequence on a shard. `CommittedOffset` reports a consumer's committed offset, or none if it has never committed.

The sequencer creates every entry. The cold consumer and recovery's scan are read-only against the queue.

See `include/abyss/core/queue.h`, `include/abyss/core/queue_entry.h`, and `include/abyss/queue/pending_append.h` for the current interface.

### Embedded WAL

The built-in queue implementation is an append-only WAL on the PVC: one physical log per data volume, carrying a logical stream per shard ([ADP-015](015-write-path-and-durability.md) §Log durability pipeline).

**Logs and segments:**

```
/data/wal/log-0000/00000000000000000041.seg
/data/wal/log-0000/00000000000000000042.seg
```

- **Logs.** `queue.log_count` logs (default 1); shard `s` belongs to log `s mod log_count`.
- **Segments.** Each log is a sequence of fixed-size, memory-mapped segments named by ordinal. The default size is 128 MiB.
- **Preparation and recycling.** A preparer thread keeps two spares ready, so rotation never touches the disk on the append path. Reclaimed segments are recycled; new ones are zero-filled only to grow the pool.
- **Reclamation runs** after each round that persists committed offsets. It is per segment and oldest first. A segment is reclaimed when, for every shard with frames in it, every retention consumer's *persisted* committed offset has passed that shard's frames there, and it was sealed more than `min_retention` ago. The active segment is never eligible. Reclamation uses persisted offsets, never in-memory ones, so a restart never resumes a consumer below a reclaimed segment.
- **Format.** See [ADP-009](009-wal-format.md).
- **Committed offsets** are persisted lazily to one checkpoint file under `{wal_path}/offsets/`. See "Offset persistence" below.

**Retention:** The queue retains entries until all consumers have acknowledged. Minimum retention is:

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

This must fit on the WAL PVC.

**Segment format:** The on-disk byte layout of segment headers and entries, including CRC-based integrity checks and schema evolution rules, is specified in [ADP-009](009-wal-format.md).

### Offset persistence

Committed offsets for every retention consumer and shard are persisted together in one checkpoint file, `{wal_path}/offsets/offsets.ckpt`.

**Cadence.** Committing an offset is an in-memory operation. A background persister writes the checkpoint every `offset_fsync_interval_ms`, but only when something has been committed since the last write. It writes again on shutdown, after the consumers have stopped.

**Two slots.** The file holds two fixed-size slots. Each slot is a whole number of 4 KiB blocks and starts on a block boundary, so a torn write to one slot can never touch the other. A slot records:

| Field | Meaning |
|-------|---------|
| magic, format version, slot size | Identify the layout |
| epoch | Monotonic counter; the valid slot with the highest epoch is current |
| shard count, consumer ids | Must match the configuration on open |
| per consumer and shard | The committed sequence, and whether one exists |
| checksum | CRC32C over the slot |

A persist overwrites the slot that does not hold the highest epoch, with the next epoch, and then makes it durable with one flush. There is no temporary file, no rename and no directory sync, as with LMDB meta pages. The file is created once, with one valid empty slot, and its directory is synced at that point only.

**Opening.**
- A missing file is a fresh store.
- A file with no valid slot, or with two valid slots of equal epoch, is corruption.
- A shard-count or consumer mismatch refuses to start.
- An older per-consumer, per-shard offset layout refuses to start and names the layout, rather than silently starting from nothing.

**Crash semantics.** After a crash, a consumer resumes from its last persisted offset. Entries after it are delivered again, at most one persist interval's worth; cold absorption is idempotent. A consumer with no committed offset starts at the first retained entry.

**Recovered tail.** On open, each log seals and syncs the tail it recovered, so entries a crashed process left only in the page cache become power-durable before any consumer reads or commits past them. Both durable ends then start at the recovered head.

One residual risk is accepted, the same one PostgreSQL accepts. Linux reports a write-back error that happened before the crash to the first flush on a new file descriptor only if the file's inode stayed cached in between. Nothing at the application level can close that gap.

A committed offset never passes the power-durable end, so a persisted offset at or past the recovered head is corruption under either class, and opening refuses.

**Failures.** A failed persist is logged and counted, and retried on the next round. Persisted offsets stay where they were, so retention waits, visibly. Nothing is lost.

**Retention reads both slots.** Reclamation uses, per consumer and shard, the lower of the two slots' offsets, and reclaims nothing until both slots hold one. If media corruption destroys the newest slot, the surviving one is still at or past everything reclaimed, so recovery never resumes a consumer, or a shard's sequence, inside a reclaimed range. Retention trails by one persist interval.

### Batch atomicity

A batch (an `AppendBatch`, or one `Reserve` of several shards' entries in one log) is atomic across crashes: every entry in it is either present in the WAL after recovery, or none is.
- A batch is reserved as one contiguous range of its log, and each frame records the bytes to its batch's end.
- Recovery drops a trailing batch whose end lies past the recovered end of the log.
- A shard's `power_loss` durable end advances only at batch ends.

A batch never spans logs. With `queue.log_count` above 1, a multi-key write whose keys span logs is rejected with `CROSSSLOT`, and FLUSHDB writes one batch per log, so a crash can leave some logs flushed and not others (#169).

See [ADP-009](009-wal-format.md) §Recovery.

### Recovery signal

The queue reports that it is recovering while it is being opened (log scan, torn-tail sealing, offset load), and stops once those steps finish. Open is synchronous today, so external callers only ever see it not recovering — but the signal lets the server gate RESP LOADING on a single uniform check regardless of whether the queue or a consumer is still catching up ([ADP-005](005-resp-frontend.md), [ADP-007](007-recovery.md)).

### Durability classes and group commit

A write's durability future resolves when its entry reaches the class set by `queue.durability` ([ADP-015](015-write-path-and-durability.md) §Durability classes):

| Class | Resolves when | Survives |
|-------|---------------|----------|
| `process_crash` (default) | The entry is published: filled into its log's mapped segment, below the log's filled prefix, so it is in the page cache | Process crash, OOM kill, container restart. A power loss loses at most the durability window. |
| `power_loss` | The fdatasync (`F_FULLFSYNC` on macOS) covering it has completed | Power loss |

The sequencer applies the entry's effect to hot before the entry is published, and the write replies once its future resolves (ADP-006).

**Natural batching.**
- Each log has a commit thread. A flush starts as soon as the previous one ends, and covers every entry published while it ran. There is no timer and no batch cap.
- At low load a write waits for one flush; under load each flush covers more writes. This is the leader/follower commit of RocksDB, and of PostgreSQL with `commit_delay = 0`, except that a dedicated thread flushes so appenders never make the system call.
- Each flush snapshots the log's filled prefix and syncs every segment holding bytes below it. Before it publishes the new durable prefix, it walks the newly durable frames' headers and advances each shard's `power_loss` end at every batch end.
- A rotation needs no sync on the append path: prepared segments had their headers synced before use.

**Who reads at which class.**
- The cold consumer's in-memory compaction buffer reads at the acknowledgement class. Hot reads nothing from the log at steady state; the read fence keeps a reply from showing a write before the write reaches the class (ADP-015 §Read visibility).
- Persisted derived state is gated at `power_loss`: cold-store writes and wipes ([ADP-004](004-cold-consumer.md)) and committed offsets.
- Under `power_loss`, no reply can therefore reflect an entry a power loss could drop.
- Under `process_crash`, persisted state still never runs ahead of the power-durable log.

**Bounded window.**
- Acknowledged-but-not-power-durable data is bounded by `queue.durability_window_bytes` (volume-wide unflushed bytes) and `queue.durability_window_ms` (the age of a log's oldest unflushed entry).
- Admission is checked before the append lock is taken. An append over either bound waits for a flush. If it is still over after `engine.write_timeout_ms`, it is rejected with an error saying the device is not keeping up.
- An empty window always admits, so a single value larger than the window cannot deadlock.
- `abyss_wal_unflushed_bytes`, `abyss_wal_durability_lag_seconds` and the backpressure counters report it.

**Failures.**
- Before an entry is published, failures reject the write cleanly. These include encoding, admission, creating the next segment (including a full disk) and the segment write itself.
- After publication, a failed flush or segment seal terminates the process. The kernel may already have dropped the dirty pages and cleared the error, so a later successful flush would advance the durable end over lost data. Recovery rebuilds from the log.

**Removed settings.** `wal_fsync_policy`, `group_commit_interval_us` and `group_commit_max_bytes` fail to load, with an error naming the replacement.
- Natural batching makes a per-write fsync pointless.
- "No fsync" was a weaker `process_crash` with an unbounded power-loss window.
- A batch byte cap has no meaning once appenders write their own entries and a flush is one fdatasync of the file; the durability window is the real bound.

### External Broker Mapping

For external queue profiles:

- **Kafka:** One partition per shard. Retention time-based (`min_retention_seconds`). Consumer offsets managed by Kafka.
- **NATS JetStream:** One subject per shard (`abyss.shard.{N}`). Durable consumers track ack position.

Since we always partition by key hash, ordering within a key is guaranteed by every broker.

### Configuration

```yaml
queue:
  backend: builtin_wal
  wal_path: /data/wal
  segment_size_bytes: 134217728       # 128 MiB, per log segment
  log_count: 1                        # physical logs, a power of two <= shard count
  ring_entries: 65536                 # per-shard offset ring, a power of two, 4096-2^24
  min_retention_seconds: 86400        # 24 hours
  offset_fsync_interval_ms: 1000      # committed-offset checkpoint cadence, 10–60000
  durability: process_crash           # or power_loss
  durability_window_bytes: 67108864   # unflushed bytes across shards, 1 MiB–4 GiB
  durability_window_ms: 1000          # oldest unflushed entry per log, 10–60000
```

## Invariants

1. An append's durability future resolving OK means the entry is durable at the configured class: published (`process_crash`) or covered by a completed fdatasync (`power_loss`).
2. `Read` returns entries in sequence order. No gaps, no reordering.
3. The queue retains every entry above the minimum persisted committed offset across retention consumers.
4. Each consumer owns its read position. No consumer's progress, and no committed offset, affects where another consumer reads.
5. Sequence IDs are contiguous and increasing per shard, including across reclamation of all of a shard's retained entries.
6. Entry type tags are immutable once appended. Every entry is a decided effect, logged once by the sequencer; replay applies it and never decides again.
7. A frame with a retired or unknown entry type is corruption, never skipped ([ADP-009](009-wal-format.md)).
8. A read below the first retained entry is an explicit out-of-range error. For a retention consumer it signals reclaimed, uncommitted data and is fatal.

## Trade-offs

**Why an append-only log instead of direct writes to stores?** A single ordered log eliminates dual-write consistency problems. Recovery is trivial: replay the log. The downside is write amplification, data is written to the WAL, then to hot (in memory), then eventually to cold (on disk). But the cold consumer's compaction buffer mitigates this by collapsing intermediate writes before they hit disk.

**Why `process_crash` as default?** A write cannot be acknowledged at `power_loss` faster than the device can flush. Acknowledging from the page cache survives the failure that dominates on Kubernetes, a process crash or restart. A power loss loses at most the bounded durability window, because the flush runs continuously. This is stronger than Redis `appendfsync everysec` ([ADP-015](015-write-path-and-durability.md) §Durability classes). Workloads that must survive power loss opt into `power_loss`.

**Why natural batching rather than a commit window?** A fixed window adds its full length to every write at low load and batches no better under load. A flush that starts as soon as the device is idle gives one flush of latency at low load and grows the batch with concurrency.
