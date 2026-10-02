# ADP-001: Queue and WAL

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-04-18

> **Amended by [ADP-015](015-write-path-and-durability.md).** These sections already describe the amended behaviour:
> - **§Interface and §Offset persistence:** reads take an explicit consumer-owned position, and acknowledgements are committed offsets persisted lazily in one dual-slot checkpoint.
> - **§Durability classes and group commit:** writes are acknowledged at a named durability class, `process_crash` by default and `power_loss` opt-in, and flushed by natural batching.
>
> Still to land, with the current behaviour described below until each does:
> - preallocated, memory-mapped segments in one physical log per volume (Phase 1c, #175);
> - the entry taxonomy losing `Conditional` and `Resolved` (Phase 2).

## Context

The queue is the single source of truth in Abyss. Every write is committed to the queue before it is applied to any store. All consumers (hot, cold, resolver) read from the queue independently and rebuild their state from it on recovery. The queue's durability and ordering guarantees are the foundation of Abyss's correctness.

The queue interface must be shard-aware from the start to support horizontal scaling in Phase 2, even though Phase 1 runs on a single pod.

## Design

### Entry Types

Each log entry carries a type tag that determines how consumers process it. Phase 1 defines four variants; the set is closed and extended only by ADP:

| Type | Semantics | Written by | Read by |
|------|-----------|------------|---------|
| `Write` | Unconditional op (SET, DEL, SADD, ...) | Frontend | Hot, Cold |
| `Conditional` | Op + predicate (SET NX, ZADD GT, ...) | Frontend | Resolver |
| `Resolved` | ref to a Conditional + decision + materialised op + return value | Resolver | Hot, Cold |
| `Flush` | FLUSHDB / FLUSHALL tombstone — wipes every key on the shard | Frontend | Hot, Cold, Resolver |

The `Conditional` / `Resolved` pair and the block-and-scan protocol are specified in [ADP-011](011-conditional-writes-and-consumer-rpc.md). The `Flush` variant is specified in [ADP-006](006-read-write-paths.md) §Broadcast write path; it is the queue-routed expression of FLUSHDB so all materialised views observe the wipe at the same logical position. The queue itself is agnostic to the semantics — it stores entries in order, preserves the type tag, and hands them to consumers unchanged.

### Interface

The queue entry (`QueueEntry`) is a struct with common metadata (sequence ID, wall-clock timestamp) and a payload variant that discriminates the three entry types. Common fields are direct field accesses — no visitor needed just to read a sequence number. Type dispatch uses the variant only when consumers need to act on the payload.

The queue interface offers two append flavours:

- **Two-phase** — `BeginAppend` / `BeginAppendBatch` allocate a sequence id and write the encoded entry to the segment, but leave it invisible to consumers until the caller runs `Publish()` on the returned RAII handle. The handle's durability future resolves when the entry reaches the configured durability class (§Durability classes and group commit). The handle holds the per-shard append mutex for the duration of the window, bounding the critical section to the caller's per-seq setup (e.g. registering a Consumer RPC promise). The destructor auto-publishes if the caller drops the handle without calling `Publish()`, so a forgotten publish degrades to a latency bug, never a lost write.
- **One-shot** — `Append` / `AppendBatch` are `BeginAppend` + `Publish()` inline. Safe only for fire-and-forget callers that do not register per-seq state before publication. Used by cold-hit promotion and by the Resolver when emitting `Resolved` entries.

Callers that await consumer apply (the tiering engine's write path) MUST use the two-phase primitive. Publishing before the producer has registered its RPC promise would race with the consumer's Fulfill — the producer could miss the response. Two-phase closes this structurally by letting the producer register under the same lock that gates visibility.

**Read position and committed offset are separate**, as a Kafka fetch position and committed offset are.
- **Read.** A consumer owns its read position and passes it to `Read`, together with the durability class the returned entries must have reached. `Read` returns the contiguous entries at or after that sequence that are durable at that class. A position below the oldest retained entry is an explicit out-of-range error, never silently moved forward.
- **Commit.** `CommitOffset` records how far a retention consumer has processed. It takes effect in memory at once, never passes the power-durable end of the log, and is persisted lazily (see "Offset persistence" below).
- **Durable ends.** `DurableEnd(shard, class)` is exclusive: every sequence below it is durable at that class, and 0 means none is. It is monotonic per shard and class. `AwaitDurable` waits for one sequence to reach a class, and `AckDurability` reports the class the queue acknowledges at.
- **Retention.** Committed offsets govern retention and where a consumer resumes after a restart. They never govern where a running consumer reads.
- **Hot.** The hot consumer keeps its position in memory and commits nothing. After a restart it rebuilds from the oldest retained entry.
- **Accessors.** `FirstSeq` reports the lowest readable sequence on a shard. `CommittedOffset` reports a consumer's committed offset, or none if it has never committed.

The frontend creates Write and Conditional entries. The Resolver creates Resolved entries. Hot and cold consumers are read-only against the queue.

See `include/abyss/core/queue.h`, `include/abyss/core/queue_entry.h`, and `include/abyss/queue/pending_append.h` for the current interface.

### Embedded WAL

The built-in queue implementation is an append-only WAL on the PVC.

**Segment management:** The WAL is composed of fixed-size segments per shard. Each segment is a file named by shard and base offset:

```
/data/wal/shard-0000/00000000000000000000.log
/data/wal/shard-0000/00000000000000065536.log
```

- Segment size: configurable, default 128 MiB.
- Segment cleanup runs after each round that persists committed offsets, and after every rotation. A segment is deleted when its `last_seq` is below the minimum *persisted* committed offset across retention consumers AND its age exceeds `min_retention`. The active segment is never eligible. Reclamation uses persisted offsets, never in-memory ones, so a restart never resumes a consumer below a deleted segment. See [ADP-009](009-wal-format.md) for file format details.
- Committed offsets are persisted lazily to one checkpoint file under `{wal_path}/offsets/`. See "Offset persistence" below.

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

**Crash semantics.** After a crash, a consumer resumes from its last persisted offset. Entries after it are delivered again, at most one persist interval's worth; cold absorption and resolver replay are idempotent. A consumer with no committed offset starts at the first retained entry.

**Recovered tail.** On open, each shard flushes the tail it recovered, so entries a crashed process left only in the page cache become power-durable before any consumer reads or commits past them. Both durable ends then start at the recovered head.

One residual risk is accepted, the same one PostgreSQL accepts. Linux reports a write-back error that happened before the crash to the first flush on a new file descriptor only if the file's inode stayed cached in between. Nothing at the application level can close that gap.

A committed offset never passes the power-durable end, so a persisted offset at or past the recovered head is corruption under either class, and opening refuses.

**Failures.** A failed persist is logged and counted, and retried on the next round. Persisted offsets stay where they were, so retention waits, visibly. Nothing is lost.

### Batch atomicity

`AppendBatch` is atomic across crashes: every entry in a batch is either present in the WAL after recovery, or none is. The mechanism is a per-entry `batch_last_seq` field added in format minor 1.1 — recovery only advances the durable tail when it decodes an entry whose `seq == batch_last_seq` (i.e., the closing entry of a batch). Mid-batch entries left behind by a crash are truncated together with the closing entry that never landed. See [ADP-009](009-wal-format.md) §Schema evolution.

### Recovery signal

`WalQueue::IsRecovering()` returns `true` while the queue is being opened (segment scan, torn-tail truncation, offset load) and `false` once those steps finish. Open is synchronous today so the flag is only ever observed `false` by external callers — but the shape of the API lets the server gate RESP LOADING on a single uniform check regardless of whether the queue or a consumer is still catching up ([ADP-005](005-resp-frontend.md), [ADP-007](007-recovery.md)).

### Durability classes and group commit

A write's durability future resolves when its entry reaches the class set by `queue.durability` ([ADP-015](015-write-path-and-durability.md) §Durability classes):

| Class | Resolves when | Survives |
|-------|---------------|----------|
| `process_crash` (default) | The entry is published: written to its segment, so it is in the page cache | Process crash, OOM kill, container restart. A power loss loses at most the durability window. |
| `power_loss` | The fdatasync (`F_FULLFSYNC` on macOS) covering it has completed | Power loss |

The write path still waits for the hot consumer to apply the entry before replying (ADP-006).

**Natural batching.**
- Each shard has a commit thread. A flush starts as soon as the previous one ends, and covers every entry published while it ran. There is no timer and no batch cap.
- At low load a write waits for one flush; under load each flush covers more writes. This is the leader/follower commit of RocksDB, and of PostgreSQL with `commit_delay = 0`, except that a dedicated thread flushes so appenders never make the system call.
- Each flush snapshots the active segment and the published end under the shard's append lock, then runs fdatasync outside it. A rotation seals the old segment with its own fdatasync before switching, so every entry below the snapshot's end is covered.

**Who reads at which class.**
- Every materialised view reads at the acknowledgement class: hot, the resolver, and the cold consumer's in-memory compaction buffer.
- Persisted derived state is gated at `power_loss`: cold-store writes and wipes ([ADP-004](004-cold-consumer.md)) and committed offsets.
- Under `power_loss`, no reply can therefore reflect an entry a power loss could drop.
- Under `process_crash`, persisted state still never runs ahead of the power-durable log.

**Bounded window.**
- Acknowledged-but-not-power-durable data is bounded by `queue.durability_window_bytes` (volume-wide unflushed bytes) and `queue.durability_window_ms` (the age of a shard's oldest unflushed entry).
- Admission is checked before the append lock is taken. An append over either bound waits for a flush. If it is still over after `engine.write_timeout`, it is rejected with an error saying the device is not keeping up.
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
  segment_size_bytes: 134217728       # 128 MiB
  min_retention_seconds: 86400        # 24 hours
  offset_fsync_interval_ms: 1000      # committed-offset checkpoint cadence, 10–60000
  durability: process_crash           # or power_loss
  durability_window_bytes: 67108864   # unflushed bytes across shards, 1 MiB–4 GiB
  durability_window_ms: 1000          # oldest unflushed entry per shard, 10–60000
```

## Invariants

1. An append's durability future resolving OK means the entry is durable at the configured class: published (`process_crash`) or covered by a completed fdatasync (`power_loss`).
2. `Read` returns entries in sequence order. No gaps, no reordering.
3. The queue retains every entry above the minimum persisted committed offset across retention consumers.
4. Each consumer owns its read position. No consumer's progress, and no committed offset, affects where another consumer reads.
5. Sequence IDs are monotonically increasing per shard.
6. Entry type tags are immutable once appended. A `Conditional` never transforms into a `Write`; the Resolver produces a separate `Resolved` entry.
7. For every `Conditional` entry at seq X, exactly one `Resolved` entry with `ref = X` follows it in the log. The queue itself does not enforce this — it is an invariant of the Resolver ([ADP-011](011-conditional-writes-and-consumer-rpc.md)) that the queue must preserve bit-for-bit.
8. A read below the first retained entry is an explicit out-of-range error. For a retention consumer it signals reclaimed, uncommitted data and is fatal.

## Trade-offs

**Why an append-only log instead of direct writes to stores?** A single ordered log eliminates dual-write consistency problems. Recovery is trivial: replay the log. The downside is write amplification, data is written to the WAL, then to hot (in memory), then eventually to cold (on disk). But the cold consumer's compaction buffer mitigates this by collapsing intermediate writes before they hit disk.

**Why `process_crash` as default?** A write cannot be acknowledged at `power_loss` faster than the device can flush. Acknowledging from the page cache survives the failure that dominates on Kubernetes, a process crash or restart. A power loss loses at most the bounded durability window, because the flush runs continuously. This is stronger than Redis `appendfsync everysec` ([ADP-015](015-write-path-and-durability.md) §Durability classes). Workloads that must survive power loss opt into `power_loss`.

**Why natural batching rather than a commit window?** A fixed window adds its full length to every write at low load and batches no better under load. A flush that starts as soon as the device is idle gives one flush of latency at low load and grows the batch with concurrency.
