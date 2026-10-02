# ADP-001: Queue and WAL

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-04-18

> **Amended by [ADP-015](015-write-path-and-durability.md).** Read takes an explicit consumer-owned position, and acknowledgements are committed offsets persisted lazily in one dual-slot checkpoint; §Interface and §Offset persistence already describe this. Group commit becomes natural batching with named durability classes, `process_crash` by default and `power_loss` opt-in, on preallocated mapped segments in one physical log per volume (Phase 1b). The entry taxonomy loses `Conditional` and `Resolved` (Phase 2). Those sections describe current behaviour until each phase lands.

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

- **Two-phase** — `BeginAppend` / `BeginAppendBatch` allocate a sequence id, encode the entry, and submit it to the group-commit fsync window, but leave the entry invisible to consumers until the caller runs `Publish()` on the returned RAII handle. The handle holds the per-shard append mutex for the duration of the window, bounding the critical section to the caller's per-seq setup (e.g. registering a Consumer RPC promise). The destructor auto-publishes if the caller drops the handle without calling `Publish()`, so a forgotten publish degrades to a latency bug, never a lost write.
- **One-shot** — `Append` / `AppendBatch` are `BeginAppend` + `Publish()` inline. Safe only for fire-and-forget callers that do not register per-seq state before publication. Used by cold-hit promotion and by the Resolver when emitting `Resolved` entries.

Callers that await consumer apply (the tiering engine's write path) MUST use the two-phase primitive. Publishing before the producer has registered its RPC promise would race with the consumer's Fulfill — the producer could miss the response. Two-phase closes this structurally by letting the producer register under the same lock that gates visibility.

**Read position and committed offset are separate**, as a Kafka fetch position and committed offset are.
- **Read.** A consumer owns its read position and passes it to `Read`, which returns the contiguous published entries at or after that sequence. A position below the oldest retained entry is an explicit out-of-range error, never silently moved forward.
- **Commit.** `CommitOffset` records how far a retention consumer has processed. It takes effect in memory at once, never passes the durable WAL tail, and is persisted lazily (see "Offset persistence" below).
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

- Segment size: configurable, default 64 MiB.
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

**Recovered tail.** On open, each shard flushes the tail it recovered, so entries a crashed process left only in the page cache become durable before any consumer commits past them. One residual risk is accepted, the same one PostgreSQL accepts. Linux reports a write-back error that happened before the crash to the first flush on a new file descriptor only if the file's inode stayed cached in between. Nothing at the application level can close that gap. Under `fsync_policy: none`, a power loss can drop entries the checkpoint already named. That policy accepts the loss, so such an offset is clamped to the recovered tail, with a warning, rather than refusing to start.

**Failures.** A failed persist is logged and counted, and retried on the next round. Persisted offsets stay where they were, so retention waits, visibly. Nothing is lost.

### Batch atomicity

`AppendBatch` is atomic across crashes: every entry in a batch is either present in the WAL after recovery, or none is. The mechanism is a per-entry `batch_last_seq` field added in format minor 1.1 — recovery only advances the durable tail when it decodes an entry whose `seq == batch_last_seq` (i.e., the closing entry of a batch). Mid-batch entries left behind by a crash are truncated together with the closing entry that never landed. See [ADP-009](009-wal-format.md) §Schema evolution.

### Recovery signal

`WalQueue::IsRecovering()` returns `true` while the queue is being opened (segment scan, torn-tail truncation, offset load) and `false` once those steps finish. Open is synchronous today so the flag is only ever observed `false` by external callers — but the shape of the API lets the server gate RESP LOADING on a single uniform check regardless of whether the queue or a consumer is still catching up ([ADP-005](005-resp-frontend.md), [ADP-007](007-recovery.md)).

### Group Commit

The embedded WAL's fsync strategy determines the trade-off between write throughput and durability.

Abyss batches all appends within a configurable window into a single fsync. Write handlers block until their batch is fsynced. The hot consumer can read from the in-memory buffer immediately, but the client promise is not fulfilled until both the fsync completes and the hot consumer applies — these happen in parallel.

```
Writer A ──append──┐
Writer B ──append──┤──▶ [batch buffer] ──fsync──▶ batch complete
Writer C ──append──┘         │
                             ├──▶ hot consumer reads from buffer concurrently
                             │
                      promise fulfilled when BOTH fsync + hot apply done
```

Three fsync policies are supported:

| Policy | Throughput | Max Data Loss on Crash | Use Case |
|--------|-----------|----------------------|----------|
| `fsync_per_write` | ~1K ops/s | 0 | Safety-critical |
| `group_commit` (default) | ~50-100K ops/s | Up to `group_commit_interval` of un-ACKed writes | Most workloads |
| `fsync_none` | ~500K+ ops/s | All un-flushed WAL data | Ephemeral data |

The key guarantee: any write the client received OK for is durable. Group commit only risks losing writes that were in the batch buffer at crash time and hadn't been fsynced or ACKed to the client yet. The client never saw OK for those, so it can retry.

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
  segment_size_bytes: 67108864        # 64 MiB
  min_retention_seconds: 86400        # 24 hours
  offset_fsync_interval_ms: 1000      # committed-offset checkpoint cadence, 10–60000
  wal_fsync_policy: group_commit
  group_commit_interval_us: 1000      # 1ms batch window
  group_commit_max_bytes: 1048576     # Or flush at 1 MiB, whichever first
```

## Invariants

1. Any `Append` operation returning OK means the entry is durable (for group commit: the batch containing this entry has been fsynced).
2. `Read` returns entries in sequence order. No gaps, no reordering.
3. The queue retains every entry above the minimum persisted committed offset across retention consumers.
4. Each consumer owns its read position. No consumer's progress, and no committed offset, affects where another consumer reads.
5. Sequence IDs are monotonically increasing per shard.
6. Entry type tags are immutable once appended. A `Conditional` never transforms into a `Write`; the Resolver produces a separate `Resolved` entry.
7. For every `Conditional` entry at seq X, exactly one `Resolved` entry with `ref = X` follows it in the log. The queue itself does not enforce this — it is an invariant of the Resolver ([ADP-011](011-conditional-writes-and-consumer-rpc.md)) that the queue must preserve bit-for-bit.
8. A read below the first retained entry is an explicit out-of-range error. For a retention consumer it signals reclaimed, uncommitted data and is fatal.

## Trade-offs

**Why an append-only log instead of direct writes to stores?** A single ordered log eliminates dual-write consistency problems. Recovery is trivial: replay the log. The downside is write amplification, data is written to the WAL, then to hot (in memory), then eventually to cold (on disk). But the cold consumer's compaction buffer mitigates this by collapsing intermediate writes before they hit disk.

**Why group commit as default?** Per-write fsync limits throughput to ~1K ops/s (bounded by disk latency). Group commit batches multiple writes into a single fsync, achieving 50-100K ops/s while maintaining the guarantee that acknowledged writes are durable. The trade-off is latency: writes wait up to the batch interval (default 1ms). For most workloads this is acceptable.
