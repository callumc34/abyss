# ADP-015: Write Path, Durability Classes and Execution Model

**Status:** Accepted
**Created:** 2026-10-01
**Supersedes:** ADP-001 §Group Commit and §Offset persistence, ADP-011 in full (the resolver, the `Conditional`/`Resolved` entry pair, block-and-scan and the consumer RPC)
**Amends:** ADP-001 §Interface, ADP-002 §Hot Consumer (now §Applying Writes and §Residency) and §Eviction, ADP-004 §Consumer Thread Loop, ADP-005 §TCP server implementation and §Write Acknowledgement, ADP-006 §Write Path, §Read Path, §Cold Hit Promotion (now §Cache Fill) and §Broadcast Write Path, ADP-007 §Recovery Process, ADP-008 §Resharding, ADP-009 §Read semantics, ADP-013 §Three substrates, `requirements.md` §Design Principles, §Performance Targets, §Durability Guarantees, §Concurrency Model and §Consumer Coordination

Delivery is phased (§Delivery) and tracked by epic #172. Each amended document keeps describing current behaviour, under a banner pointing here, until the phase that changes that behaviour lands and rewrites the section. Phases 0 and 1 have landed, and Phase 2 through its fifth stage: the sections below describe them as current behaviour, and the documents they amend have been rewritten to match. Phase 2 completes with stage 6, which adds the three-way property test (§Delivery). Asynchronous request execution (Phase 3, #161) and the later rows are still to come, and their sections say so.

## Context

`requirements.md` targeted "hot write (queue append + ACK) < 50 µs p99". The design made that impossible. A write was acknowledged only after its WAL fsync, and group commit waited a fixed 1 ms window after the first pending append before every fsync. On real devices, durable write and ACK p99 was about 1 ms plus fsync.

A review of the write path found that window to be the smallest of six structural problems. A measured baseline (macOS, indicative) gave 95 SET/s on one connection, and pipelining made no difference:

1. **Queue reads were O(position in segment).** Every consumer read re-decoded the active segment from its first entry. Three consumers per shard paid this, and the hot consumer's read gated every acknowledgement.
2. **The frontend blocked.** Each reactor thread waited for durability and apply on every command. Pipelined commands therefore ran one fsync after another, and throughput was capped at about `io_threads ÷ write latency` whatever the client count. A slow fsync stalled every read on that reactor.
3. **Group commit used a fixed timer.** Segments grew by append and were not preallocated, so every flush also committed file metadata.
4. **Every retention ack rewrote an offset file** with fsync, rename and directory fsync, then ran a reaper that locked every shard.
5. **Every write involved three threads** (reactor, committer, hot consumer) and woke two more (cold, resolver). Each hand-off cost a promise and a registry mutex.
6. **The resolver was serial.** It blocked on two durability rounds per conditional write.

The same review found correctness defects on the same paths:
- A queue read position derived from the ack offset caused a hot-consumer livelock, a cold-consumer drain stall and duplicate `Resolved` entries.
- Hot and cold applied a conditional's outcome at the `Resolved` position, not the `Conditional` position.
- Writes to keys evicted from hot built partial state in hot.
- Hot evicted keys that cold had not yet drained.
- Promotion appended stale values.
- The commit loop continued after a failed fsync.
- Cold materialised WAL entries that were not yet durable.

Most of these trace to two design choices. Hot materialisation is a separate consumer that trails the writer, and the read position is coupled to the durable ack. The fix is architectural, not tuning.

## Decision

### Durability classes

Abyss acknowledges a write when it has reached a configured durability class. The class names the failure the write survives.

| Class | Acknowledged when | Survives | Role |
|-------|-------------------|----------|------|
| `process_crash` | The write's log frame is in the operating system's page cache | Process crash, OOM kill, container restart. A node power loss loses at most one background flush window. | Default |
| `power_loss` | The fdatasync (macOS `F_FULLFSYNC`) covering the frame has completed | Power loss | Opt-in |

**Precedent.** Redis' default is snapshots. With AOF and `appendfsync everysec`, Redis writes to the OS before replying and fsyncs once a second in the background. Dragonfly has no write-ahead log; durability comes from snapshots and replication. Kafka acknowledges from the page cache and relies on replication.
- `process_crash` is stronger than Redis `everysec` and than any unreplicated page-cache acknowledgement, because the background flush runs continuously.
- It is weaker than a replication quorum, which also survives the loss of a whole node. The external profile gets its durability that way, not from local fsync.
- `power_loss` matches Redis `appendfsync always`, without stalling every client on each flush.

**Removed settings.**
- The per-write fsync policy is removed because natural batching makes it pointless.
- The "no fsync" policy is removed. It was a weaker `process_crash` with no background flush, so its power-loss window was unbounded.
- The group-commit interval is removed.

A configuration that still names a removed setting fails to load. The error names the setting and its replacement; nothing is mapped silently.

**Two rules keep the classes principled:**
- **No persisted derived state runs ahead of the power-durable log.**
  - The cold consumer may absorb entries into its volatile compaction buffer at the acknowledgement class. It writes to the cold store, by flush or wipe, only effects whose entries are at or below the fdatasync watermark, whatever the acknowledgement class. Otherwise a power loss would leave cold holding writes the log no longer has.
  - Committed offsets are gated the same way.
  - Gating absorption itself would make every hot eviction, and with it memory backpressure on writes, wait for a device flush, because hot evicts only what cold has absorbed (ADP-004 §Persisting at the power-durable log).
  - What is absorbed but not yet persistable is bounded by the durability window below.
- **The window of acknowledged-but-not-power-durable data is bounded** in bytes and time. If the device cannot keep up, writes are backpressured and a durability-lag metric reports it. The window never grows silently. It is set by `queue.durability_window_bytes` (default 64 MiB, volume-wide) and `queue.durability_window_ms` (default 1000, the age of a shard's oldest unflushed entry); an append over either bound waits, then is rejected after the write timeout.

Two consequences follow from the persistence cap, and both are accepted:
- Cold applies a Flush's wipe only once the Flush is power-durable, in every class. FLUSHDB's reply does not wait for it: hot's flush floor answers reads as absent until the wipe lands (§Residency invariant).
- A flush stall delays cold, which delays hot eviction (§Residency invariant). This surfaces as bounded memory backpressure on writes, never as unbounded hot growth.

### Log durability pipeline

**Natural-batching group commit.** A flush starts as soon as the device is idle. Appends that arrive while a flush is in flight form the next batch. There is no timer, and durability is signalled by callback. This is the leader/follower commit used by RocksDB, and by PostgreSQL with `commit_delay = 0`. At low load the latency is one flush; under load each flush covers more writes.

There is no batch byte cap. A natural batch is limited by how long the previous flush took, and appenders write their own frames, so a flush is one data sync of the file whatever the batch holds. RocksDB caps its write group only because its leader copies the group's data. The bound that matters is the durability window.

**Recycled segments, flushed with `fdatasync`.**
- A preparer thread per log keeps two spare segments ready, off the hot path, so a rotation is a pointer swap and a flush syncs data only.
- Reclaimed segments are recycled. Each frame's commit word carries its segment's generation (the low 32 bits of its ordinal), so a stale frame left from a previous life never reads as filled. This is RocksDB's recyclable record format.
- New segments are zero-filled only to grow the pool: at start-up, and while retention pins more segments than the pool holds. Zero-filling every segment would write each byte twice, halving WAL bandwidth on a 125 MiB/s volume.
- `fallocate` or `F_PREALLOCATE` alone is not enough. Unwritten extents cost metadata in the next flush, and on APFS a preallocated but unwritten tail was measured still stalling (p99 1.57 ms).
- When no spare is ready (a full disk, or a preparer behind), a reservation fails before any state changes. The append waits for a spare until its deadline, then is rejected cleanly. `abyss_wal_spare_segments` and `abyss_wal_spare_waits_total` report it.

**Memory-mapped segments, one log for both classes.**
- Copying a frame into a shared mapping makes it process-crash durable with no system call. The mapping doubles as a zero-copy tail for readers.
- `power_loss` additionally waits for the flush covering the frame.
- Every segment's blocks are written before it becomes active, recycled or zero-filled, which prevents the allocation failures (`ENOSPC`) that a mapping would raise as `SIGBUS`. Media errors can still surface, as `SIGBUS` on a fault or as an error at flush. Both are fatal.
- On Linux, fdatasync covers mapped dirty pages through the unified page cache. On Windows the flush is `FlushViewOfFile` then `FlushFileBuffers`.
- On macOS, `F_FULLFSYNC` on the file was observed to write back pages dirtied through the mapping: `mincore`'s modified bit clears. That is an assumption about APFS, not a documented guarantee, so a platform test checks it with `mincore`.

**One physical log per data volume, carrying a logical stream per shard.**
- Flush count scales with logs, not shards, which matters on IOPS-capped cloud volumes.
- Space is reserved by a compare-and-swap on the log's tail. A plain `fetch_add` could not refuse a reservation that runs into a segment that is not ready. A shard's sequence numbers and its log positions are assigned in the same shard critical section, so per-shard sequence order equals log order.
- A batch is one contiguous reservation inside one segment. Each frame records the bytes to its batch's end, so batch closure is by position and independent of shards (ADP-009).
- Appenders fill their reservations concurrently. The *filled prefix*, below which every frame is filled, is the `process_crash` watermark: an append is acknowledged only once the prefix passes it. That wait is microseconds, because a hole's owner is mid-copy inside its own shard's critical section.
- The flusher syncs up to the filled prefix. Before publishing the new `power_loss` watermark, it walks the newly durable frames' headers and advances each shard's durable end, only at batch ends.
- The log count is a power-of-two setting with a static shard-to-log mapping.
- Retention is per log segment and strictly oldest-first. A segment is reclaimed only once every shard it holds has every retention consumer's persisted offset past its frames there.
- One stuck or poisoned shard therefore pins reclamation of every later segment on its volume; the oldest-eligible-unreaped age metric reports it.
- Reclaiming past a pinned segment is not allowed: hot's rebuild from a shard's first retained entry would replay an older write across the gap and shadow cold's newer value.
- A shard whose frames were all reclaimed keeps its sequence: recovery resumes it after the highest persisted offset.

**Out-of-line large values (#162).**
- Values above a threshold of 16–64 KiB, sized by the lock-hold budget, are copied into a separate blob lane before sequencing. The log frame carries a reference.
- The main log then holds only frames filled inside the critical section, so one large write cannot stall the durable watermark for every shard. Precedent: WiscKey, RocksDB BlobDB, Badger.
- Under `power_loss` the watermark is the minimum of the log and the blob lane. Orphaned blobs are reclaimed by retention.
- Until the blob lane lands, a reservation's frames are filled inside the critical section only until their total reaches 16 KiB; its later frames are filled after it. Readers of those keys wait on the read fence, later frames wait on the contiguous watermark, and a head-of-line metric reports the stall. The maximum value size stays 64 MiB.

**Fail-stop after apply.** Failures split on whether the effect has been applied:
- **Before the effect is applied, the write is rejected cleanly** and the process continues. This covers log space reservation, including a full disk when the next preallocated segment cannot be created. Writes fail, as `requirements.md` §Backpressure requires.
- **After the effect is applied, the process terminates.** This covers filling or publishing a frame, a flush, and a mapping fault. Recovery then rebuilds from the log. After a failed flush the kernel may already have dropped the dirty pages, so continuing would let a later successful flush advance the durable watermark over lost data. Precedent: PostgreSQL after fsyncgate, RocksDB's background error, Kafka log-directory failure.

### Queue read and acknowledgement contract

**Consumers own their read position.** A read takes an explicit starting sequence, as a Kafka fetch does. The committed (acknowledged) offset is separate and governs retention only.

**Reads seek, never scan.**
- Each shard stream keeps an offset ring for its recent frames (`queue.ring_entries`), read lock-free under a per-entry seqlock.
- A sparse index, one point per 64 KiB of the shard's frame bytes, covers the rest of the retained log. The skip from an index point reads frame headers only.
- A small per-stream position hint lets consecutive reads continue where the last one stopped.
- Frames are decoded straight from the mapping.

**Positions below the oldest retained entry are an explicit out-of-range error, never a silent clamp.**
- Hot keeps no read position: recovery rebuilds it from each shard's oldest retained entry.
- For a retention consumer, out-of-range can only mean that data it had not committed was reclaimed. The process fail-stops, naming the consumer, shard and positions.

**Committed offsets are persisted lazily**, on a fixed cadence and at shutdown, into one dual-slot checkpoint that covers every retention consumer and shard.
- Each slot is a separately aligned block with an epoch and checksum. It is overwritten in place and flushed with one data sync: no rename and no directory sync, as with LMDB meta pages.
- Retention reclaims only below *persisted* offsets.
- After a crash, consumers resume from the last persisted offset and re-process entries at least once. Cold absorption is idempotent.

**Consumer reads stay bounded** under any acknowledgement policy, poison entries included. A poison entry pins the consumer's committed offset, and with it retention, visibly, while the consumer reads past it.

### Sequenced write path

A single sequencer per shard executes every write on the calling thread. Whatever the write must wait for (WAL admission, memory backpressure, loading a non-resident key) it waits for first, holding no lock. Then, under the exclusive lock of each shard the write touches, taken in ascending order:

1. Evaluate the command against the key's complete current state, computing both the reply and the effect.
2. Reserve log space. A failure here leaves nothing applied: the sequencer unlocks, waits, and evaluates again from the start, or rejects the write at its deadline.
3. Apply the effect to hot, and unlock.
4. Fill and publish the frame.

The reply is sent when the frame reaches the configured durability class. Steps 3 and 4 cannot fail without terminating the process (§Fail-stop after apply). ADP-006 §Write Path describes each step.

**The log records decided effects, not intents** ("decide-then-log").
- Conditional and read-modify-write commands (`SET NX`, `ZADD GT`, `INCR`, `SPOP`) are logged as the concrete operations they decided on.
- An expiry that a decision observed is logged as an explicit delete before the new effect.
- Precedent: Redis effects replication, which turns `SPOP` into `SREM` and `EXPIRE` into `PEXPIREAT`, and propagates lazy expiry as `DEL`.
- The sequencer and recovery replay apply effects to hot through one function, so the two cannot interpret a frame differently.

**What this removes.** The steady-state hot consumer thread, the resolver and its existence cache, the `Conditional` and `Resolved` entry types (their type bytes are reserved, [ADP-009](009-wal-format.md)), block-and-scan, the apply notifier, the consumer RPC registry, promotion through the queue and the read-consistency wait are all removed.
- Recovery no longer re-decides anything: replay applies effects, and its resolver phase is gone.
- ADP-011's determinism constraint on decisions no longer applies.
- Read-modify-write commands become ordinary writes.

**Why ADP-011's reasons for a separate resolver no longer hold:**
- *Cold reads on the critical path.* The client waited for the resolver's cold lookup anyway.
- *Transactions.* `MULTI`/`EXEC`, when implemented, executes on the sequencer like any multi-key command: its effects are logged as one batch frame. `WATCH` compares each watched key's latest sequence number at `EXEC`, loading a non-resident key first. An aborted transaction has no effects, so it logs nothing.
- *Cross-pod lookups.* The sequencer is per shard on the owning pod, exactly where the resolver ran.

**Latency and portability.**
- The critical section holds no wait: decide, reserve, fill frames, and apply to hot. Loads, admission and backpressure waits come before it. The 16 KiB under-lock budget is cumulative per reservation: frames are filled under the lock until their total reaches 16 KiB, and the reservation's later frames are filled after it. Every frame is published after it. Decided values are moved into hot, never copied, and what an apply replaces is freed after the lock.
- The hot apply still allocates, and the lock is the shard's reader-writer lock. Arena allocation, and a spin-then-park lock, are taken up only if measurement shows either is the limit. `abyss_sequencer_lock_hold_seconds` samples the hold.
- Under skewed (zipfian) load one shard lock behaves like a global lock. Lock behaviour under skew, with concurrent readers, is part of the acceptance measurement for the sequenced write path.
- The shard is reached through an executor boundary ("run this on shard S"), so moving to one owner thread per shard later is a scheduler change, not a rewrite.

**Cross-shard atomic commands.**
- In single-pod deployments, multi-key write commands spanning shards (`MSET`, multi-key `DEL` and `UNLINK`, `MSETNX`, `RENAMENX`, `COPY`) execute atomically (#165). The sequencer takes the involved shard locks in a fixed order, decides everything, makes one reservation and writes one batch carrying every shard's effects; batch atomicity is already a property of the log format. FLUSHDB is one `Flush` per shard in one batch.
- Only within one log. With `queue.log_count` above 1, a multi-key write whose keys span logs is rejected with `-CROSSSLOT`, and FLUSHDB is one batch per log: atomic to readers, but a crash can leave some logs flushed and not others. The validator warns at start-up (#169). A two-phase commit across logs was rejected: it would add a recovery protocol, several logs are opt-in, and Redis Cluster rejects cross-slot multi-key writes anyway.
- Multi-key reads (`MGET`, multi-key `EXISTS`) are not atomic across shards. They read each shard in turn and can observe a cross-shard write half-applied. #170 makes them atomic by taking the same locks shared, in the same order.
- The client-facing identity that goes with these semantics (standalone or cluster) is decided separately (#169).

**External queue profile.** With a broker as the log, the sequencer is the single producer for its shard's partition. During resharding an outgoing owner must not be able to interleave effects with the incoming one. Producer fencing (an idempotent producer with leader-epoch fencing, or transactional producer ids) is therefore required wherever a broker is the log.

### Residency invariant

For every key, at least one of these holds:
- hot holds the key's complete state, either a live entry or a tombstone; or
- the compaction buffer plus cold hold the key's current state.

After cold drains a resident key, both hold.

**Rules:**
- **A write to a non-resident key loads the key first,** outside the lock. Under the lock the loader leaves a placeholder for the key carrying a load token, then loads with no lock held. The loaded state is installed only if that same token is still the key's placeholder; a blind write replaces the placeholder and the load is discarded, and a non-blind write waits for it. The check is exact, so retries stay bounded under eviction pressure, and it does not depend on stubs, because stubs can be dropped.
- **Hot evicts a key only after cold has drained past the key's latest write** (#126). Each entry and tombstone carries its latest sequence number; cold's drained seq means absorbed into the compaction buffer, where the key is readable.
- **An evicted key may leave a stub** holding its type, absolute TTL and latest sequence number, so existence-only replies (`EXISTS`, `TYPE`, `TTL`, and predicates such as `DEL`'s count or `SET NX`) need no full load.
  - Stubs are a bounded cache (`hot.stub_memory_fraction` of hot memory, about 80 B each), dropped oldest first. Hot memory must not grow with the total number of keys ever written.
  - Without a stub, existence replies fall back to a cold probe, which bloom filters make cheap for absent keys.
  - A write's new state never lives only in a stub: a `DEL` of a non-resident key leaves a tombstone, and the `EXPIRE` family loads the key.
- **A Flush sets the shard's flush floor.** It wipes the shard's entries, stubs and placeholders, and until cold's drained seq passes the Flush, a hot miss on that shard is absent with no cold read. That keeps the invariant without waiting for cold's wipe.
- **Memory backpressure.** Hot may exceed `hot.max_memory_bytes` only by what cold has not drained. It is applied per shard: once a shard is over `hot.max_memory_bytes` ÷ `hot.shard_count` × `hot.backpressure_ratio` (default 1.25), a write to it that can grow memory waits for cold, then is rejected with `-OOM` before anything is reserved. Under skew one shard can reject writes while hot's total is under `hot.max_memory_bytes`.
- **A cold read hit fills hot directly,** under a load token, instead of appending a promotion entry (#168). Reads no longer write to the log.

A hot miss implies that buffer plus cold is current, so the read path needs no consistency wait. [ADP-002](002-hot-store.md) §Residency describes the mechanisms.

### Read visibility

No reply, to a read or a write, reflects a write that a failure in the configured class could lose. A command whose key's latest sequence number is above the class watermark waits for the watermark to pass. This includes replies that reveal prior state, such as `SET … GET`, `INCR` and a `DEL` count.
- A read copies its answer under the shared lock, with the highest latest sequence number among the keys it touched, then unlocks and waits. Copying first gives a valid linearisation point and cannot starve on a key written continuously.
- A write with effects is covered by its own durable wait, because its frames follow everything it observed in the same log. A decision with no effect (`SETNX` returning 0, for example) waits on what it observed.
- Under `process_crash`, the watermark is the published end, so the wait fires only for frames still being filled or published.
- The wait matters under `power_loss`.
- The wait blocks the calling thread until asynchronous execution lands (§Asynchronous request execution).

### Asynchronous request execution

This section is still to come (#161). Until it lands, the calling reactor thread blocks on a command's durable wait, its fence and its cold reads.

**Reactors never wait on durability or on cold reads.** Completions are delivered back to the reactor.

**Ordering.** Each command on a connection is sequenced before the next one on that connection is dispatched, which keeps Redis pipeline order. Replies are written in command order. Concurrent and pipelined writes therefore share flushes.

**Backpressure.** A per-connection in-flight limit applies, alongside the existing write-buffer thresholds.

**Cold I/O.** Cold reads and key loads run on a small I/O pool and complete asynchronously.

### Execution resources

**Pooled cold workers (#177)** move cold from one thread per shard to a pool sized to cores. They follow the sequenced write path, which changed cold's inputs and coupled hot eviction to cold's drain progress.
- One shard's drain-and-flush cycle stays on one thread, because the persistence gate's paused absorption and the buffer's node stability depend on it.
- Pooled workers park on a volume-level publish signal. The log's single atomic tail is what that signal wraps.

**Recovery is a single demultiplexing pass over each log.**
- Hot rebuilds from each shard's first retained entry, and cold from its committed offset, which can trail by up to the buffer's deadline.
- Per-shard reads over an interleaved log would read it once per wave of shard replays.
- Instead, one scan per log feeds both. A header-only walker hands per-shard batches of positions to shard-affine decode workers, bounded so they stay inside the walker's page-cache window. Each batch is split between hot and cold by sequence, and fed to cold first, so cold's drained seq covers it before hot applies it.
- The retained log is read about once. `abyss_wal_scan_bytes_total` reports it.

**The hot replayer rebuilds only complete state** ([ADP-007](007-recovery.md) §Hot replay).
- It applies frames through the sequencer's own apply. A frame flagged `kReplacesState` ([ADP-009](009-wal-format.md)) makes its key resident, and a Flush wipes its shard and sets the flush floor; any other frame applies only to a key already resident in the rebuild, tombstones included. Otherwise the key stays non-resident, served from buffer plus cold, and its stub is dropped. Replaying a reclaimed key's tail would otherwise build a partial collection, the replay form of #163.
- Replay reads no clock and judges no TTL. Eviction stays gated on cold's drain.
- Replay memory normally stays within the backpressure ratio: cold is fed first in each batch, and drained means absorbed into cold's buffer, so everything hot holds is evictable. At most one scan batch's growth can take a shard over the ratio. A shard over it with nothing evictable makes cold flush through the current frame, as a fallback that never fires on a normal log, and eviction is retried with each new batch.
- Past a hard ceiling of twice the ratio limit per shard, recovery fails loudly, with an error naming the shard, the seq and the likely cause: cold's drained seq pinned (by a parse-poison entry, for example), or one entry larger than the shard's budget. A forced flush that fails also fails recovery.
- Every shard must get exactly its frames, in order, or recovery fails.
- After the scan, a sweep reads the clocks once, maps each replayed key's write time onto the steady clock, and evicts what is past its TTL or eviction window, then down to the budget. It decides residency, never values.

## Invariants

1. **The queue is the single source of truth.**
   - Each write is sequenced once.
   - For each resident key, hot holds the result of applying that key's log effects in sequence order.
   - Cold is an independent consumer that never runs ahead of the power-durable log.
2. **No reply, to a read or a write, reflects a write that a failure in the configured durability class can lose.** In particular, a write is acknowledged only after it reaches that class and is applied to hot.
3. **Cold lag is decoupled from writes.** It reaches the write path only through bounded, observable memory backpressure, because keys cold has not drained cannot be evicted from hot. It never causes unbounded memory growth.
4. **Recovery is pure log replay.** Effects are re-applied, never re-decided. Before replay begins, the retained log is flushed, so the power-durable watermark is known.
5. **The residency invariant holds for every key.**
6. **Ordering is unified.** Per-shard sequence order, log file order and hot apply order are the same order.
7. **Failure handling splits at apply.** A write that fails before its effect is applied is rejected cleanly. Any failure after its effect is applied terminates the process.

## Impact on other ADPs and open issues

| Document | Change | Phase |
|----------|--------|-------|
| ADP-001 | Read takes a position, and positions below the oldest retained entry are out of range. Acks become committed offsets, persisted lazily in one dual-slot checkpoint. Volatile consumers are removed. Group commit and fsync policies are replaced by natural batching and durability classes. Segments move to one mapped physical log per volume. The entry taxonomy loses `Conditional` and `Resolved`. | 1a, 1b, 1c, 2 |
| ADP-002 | Hot is applied by the sequencer. Residency invariant, droppable stubs, eviction gated on cold drain. | 2 |
| ADP-004 | Consumer loop reads by position, and a poison entry pins the commit offset while reads pass it. Writes to the cold store are capped at the power-durable watermark. Recovery replay through the demultiplexing scan. Pooled workers. | 1a, 1b, 1c, #177 |
| ADP-005 | Asynchronous completion. The blocking head-of-line trade-off is removed. Conditional dispatch becomes sequenced writes. | 2, 3 |
| ADP-006 | Sequenced write path, read fence, cache fill in place of promotion, no read-consistency wait. The Flush acknowledgement no longer requires a persisted consumer offset: the Flush entry's own durability suffices, Wipe stays synced, and a replayed Flush is idempotent per shard. | 1a, 2 |
| ADP-007 | Recovery syncs the log before replay. Hot and cold replay through one demultiplexing scan per log. Resolver phase removed; the hot replayer's residency rule and sweep. | 1b, 1c, 2 |
| ADP-008 | A broker-backed log requires producer fencing so an outgoing shard owner cannot interleave effects during resharding. | 2 |
| ADP-009 | Sparse in-memory index. Format 2: a physical log per volume with per-shard streams, recycled segments with per-frame generations, commit words, batch closure by position. Effect frames. | 1a, 1c, 2 |
| ADP-011 | Superseded in full: the resolver, the entry pair, block-and-scan and the consumer RPC are removed. | 2 |
| ADP-013 | Target-to-substrate map, write probe, comparative drivers and matrix, load-driver pipelining. | 0 |

**Issues:**
- Unblocked: #67 (by Phase 0), #142 (by Phase 2).
- Absorbed or obsoleted: #125, #126, #127, #128, #137 and #88.
- Fixed by Phase 2: #163, #165, #167 and #168.
- Re-scoped: #59–#63 now trigger only if profiling shows the shard lock is the limit.

## Delivery

| Phase | Issue | Content |
|-------|-------|---------|
| 0 | #157 | Measurement: write probe with device-flush calibration, flush metrics, read-position micro-benchmark, a pipelining load driver, server-identified comparative runs. Targets in `requirements.md`. |
| 1a | #158 | Read and acknowledgement contract: consumer-owned positions, explicit out-of-range, sparse index with header-only skip, lazy dual-slot offset checkpoint. |
| 1b | #159 | Durability semantics on the existing per-shard segments: natural batching, durability classes, cold persistence ceiling, bounded durability window, fail-stop, recovery-time sync. |
| 1c | #175 | Physical log: mapped, recycled segments in one log per volume with per-shard streams, a reserve/fill/commit tail and a filled-prefix watermark; offset rings and sparse indexes; per-segment, oldest-first retention; the demultiplexing recovery pass and scan-based hot and cold rebuild. Delivers W2 under concurrency and W3, which 1b's per-shard flushes cannot, because they serialise on the device. |
| After 2 | #177 | Pooled cold workers, shard-affine. |
| 2 | #160 | Sequenced write path, residency invariant, read visibility, cross-shard atomic commands, shard-lock measurement under skew; resolver removal. Completes with stage 6, the three-way property test. |
| 3 | #161 | Asynchronous request execution and asynchronous cold I/O. |
| Later | #162, #170, #88, #178 | Blob lane, atomic multi-key reads, io_uring with several flushes in flight (watermark at the contiguous completed prefix), per-segment index footers so open need not scan the retained log, hot-table and allocator work driven by profiles. |

Every phase is measured against the Phase 0 baseline with the same instruments. A three-way property test checks hot, cold and a reference model against each other, replies included. It covers TTL boundaries, flushes at arbitrary points, Flush entries, eviction with loading, and multi-effect batches. It lands in Phase 2's stage 6, which completes the phase, and runs under the unit tier (seeded, bounded) and the fuzz harness (long).

## Trade-offs

**Why not shorten the commit window or acknowledge before the write reaches the OS?** A shorter window still adds latency at low load. Acknowledging before the OS has the data loses acknowledged writes on any process crash, the most common failure on Kubernetes.

**Why apply hot in the sequencer, and keep a replayer only for recovery?** Applying hot from a trailing consumer costs thread hops on every write. It is also the root of the conditional-reordering and partial-state defects. Applying hot as part of sequencing removes both. Recovery still has to rebuild hot from the log, so a replayer remains, fed only by recovery's scan and applying through the sequencer's own apply.

**Why locks on the calling thread rather than thread-per-core now?**
- In a shared-nothing per-core runtime (Seastar, Dragonfly), most operations hop to another core.
- Seastar is Linux-only, which would lose macOS development parity.
- It requires rewriting the network, engine and hot-store layers.
- Garnet shows that shared memory with fine-grained synchronisation competes with, and can beat, shared-nothing designs.

The executor boundary keeps the option open.

**Why keep an independent cold consumer rather than writing cold back from hot?** Writing back from hot would halve memory for recently written keys. But it would break consumer independence and the external profile, where cold is fed by a broker consumer.

**Why an atomic tail on one log per volume rather than per-shard logs?** Per-shard logs multiply device flushes by the shard count. A microbenchmark (14-core host, indicative) showed an atomic reservation tail scaling near-linearly up to the core count with realistic per-append work, and flat beyond it. A parked mutex flattened at about 1.7 M/s.

## Open decisions

- **Client-facing identity (#169).** Proposed: standalone in single-pod (with emulated cluster commands) and cluster in multi-pod.
- **An external hot tier ([#187](https://github.com/callumc34/abyss/issues/187)).** The sequencer decides against hot under the shard lock, so hot is in-process, and the server builds only the built-in store. Whether an external hot tier survives in any profile is undecided.
- **Maximum value size before the blob lane lands (#162).** Kept at 64 MiB; the head-of-line effect of large values is reported by a metric.
- **Hardware for authoritative measurement.**
