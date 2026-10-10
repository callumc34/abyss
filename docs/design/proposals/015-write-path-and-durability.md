# ADP-015: Write Path, Durability Classes and Execution Model

**Status:** Accepted
**Created:** 2026-10-01
**Supersedes:** ADP-001 §Group Commit and §Offset persistence, ADP-011 §The Resolver, §Queue Entry Taxonomy (Conditional/Resolved) and §Consumer Block-and-Scan
**Amends:** ADP-001 §Interface, ADP-002 §Hot Consumer and §Eviction, ADP-004 §Consumer Thread Loop, ADP-005 §TCP server implementation and §Write Acknowledgement, ADP-006 §Write Path, §Read Path, §Cold Hit Promotion and §Broadcast Write Path, ADP-007 §Recovery Process, ADP-008 §Resharding, ADP-009 §Read semantics, ADP-013 §Three substrates, `requirements.md` §Design Principles, §Performance Targets, §Durability Guarantees, §Concurrency Model and §Consumer Coordination

Delivery is phased (§Delivery) and tracked by epic #172. Each amended document keeps describing current behaviour, under a banner pointing here, until the phase that changes that behaviour lands and rewrites the section.

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
- **No persisted derived state runs ahead of the power-durable log.** The cold consumer absorbs only entries at or below the fdatasync watermark, whatever the acknowledgement class. Otherwise a power loss would leave cold holding writes the log no longer has.
- **The window of acknowledged-but-not-power-durable data is bounded** in bytes and time. If the device cannot keep up, writes are backpressured and a durability-lag metric reports it. The window never grows silently. The bound's configuration keys and defaults are defined with the durability pipeline (#159).

Two consequences follow from the absorption cap, and both are accepted:
- Anything whose acknowledgement waits on cold applying an entry (FLUSHDB) pays `power_loss` latency in every class.
- A flush stall delays cold, which delays hot eviction (§Residency invariant). This surfaces as bounded memory backpressure on writes, never as unbounded hot growth.

### Log durability pipeline

**Natural-batching group commit.** A flush starts as soon as the device is idle. Appends that arrive while a flush is in flight form the next batch, capped by bytes. There is no timer, and durability is signalled by callback. This is the leader/follower commit used by RocksDB, and by PostgreSQL with `commit_delay = 0`. At low load the latency is one flush; under load each flush covers more writes.

**Preallocated, zero-filled segments, flushed with `fdatasync`.** The next segment is created off the hot path, so a flush syncs data only. Recycled segments would need a per-frame epoch to stop stale frames being replayed past a torn tail (RocksDB's recyclable record format); zero-filling avoids that format change.

**Memory-mapped segments, one log for both classes.**
- Copying a frame into a shared mapping makes it process-crash durable with no system call. The mapping doubles as a zero-copy tail for readers.
- `power_loss` additionally waits for the flush covering the frame.
- Preallocation with zero-fill prevents the allocation failures (`ENOSPC`) that a mapping would raise as `SIGBUS`. Media errors can still surface, as `SIGBUS` on a fault or as an error at flush. Both are fatal.
- Whether macOS `F_FULLFSYNC` on the file writes back pages dirtied through the mapping, or whether `msync` must come first, is verified when the mapping lands. On Linux, fdatasync covers mapped dirty pages through the unified page cache.

**One physical log per data volume, carrying a logical stream per shard.**
- Flush count scales with logs, not shards, which matters on IOPS-capped cloud volumes.
- Space is reserved by an atomic tail offset. A shard's sequence number and its log offset are assigned in the same shard critical section, so per-shard sequence order equals file order.
- The log count is a power-of-two setting with a static shard-to-log mapping.
- Retention is per log segment. One stuck or poisoned shard therefore pins reclamation of every segment on its volume; the oldest-eligible-unreaped age metric reports it.

**Out-of-line large values (#162).**
- Values above a threshold of 16–64 KiB, sized by the lock-hold budget, are copied into a separate blob lane before sequencing. The log frame carries a reference.
- The main log then holds only frames filled inside the critical section, so one large write cannot stall the durable watermark for every shard. Precedent: WiscKey, RocksDB BlobDB, Badger.
- Under `power_loss` the watermark is the minimum of the log and the blob lane. Orphaned blobs are reclaimed by retention.
- Until the blob lane lands, frames above the threshold are filled after the critical section. Readers of those keys wait on the read fence, later frames wait on the contiguous watermark, and a head-of-line metric reports the stall. The maximum value size stays 64 MiB.

**Fail-stop after apply.** Failures split on whether the effect has been applied:
- **Before the effect is applied, the write is rejected cleanly** and the process continues. This covers log space reservation, including a full disk when the next preallocated segment cannot be created. Writes fail, as `requirements.md` §Backpressure requires.
- **After the effect is applied, the process terminates.** This covers filling or publishing a frame, a flush, and a mapping fault. Recovery then rebuilds from the log. After a failed flush the kernel may already have dropped the dirty pages, so continuing would let a later successful flush advance the durable watermark over lost data. Precedent: PostgreSQL after fsyncgate, RocksDB's background error, Kafka log-directory failure.

### Queue read and acknowledgement contract

**Consumers own their read position.** A read takes an explicit starting sequence, as a Kafka fetch does. The committed (acknowledged) offset is separate and governs retention only.

**Reads seek, never scan.** A sparse in-memory sequence-to-offset index, rebuilt at open and trimmed after torn-tail truncation, locates any position. The skip from an index point reads frame headers only. Recent frames come from the page cache, and from the mapping itself once the log is memory-mapped.

**Positions below the oldest retained entry are an explicit out-of-range error, never a silent clamp.**
- Hot resets to the oldest entry.
- For a retention consumer, out-of-range can only mean that data it had not committed was reclaimed. The process fail-stops, naming the consumer, shard and positions.

**Committed offsets are persisted lazily**, on a fixed cadence and at shutdown, into one dual-slot checkpoint that covers every retention consumer and shard.
- Each slot is a separately aligned block with an epoch and checksum. It is overwritten in place and flushed with one data sync: no rename and no directory sync, as with LMDB meta pages.
- Retention reclaims only below *persisted* offsets.
- After a crash, consumers resume from the last persisted offset and re-process entries at least once. Cold absorption and resolver replay are idempotent.

**Consumer reads stay bounded** under any acknowledgement policy, poison entries included. A poison entry pins the consumer's committed offset, and with it retention, visibly, while the consumer reads past it.

### Sequenced write path

A single sequencer per shard executes every write under the shard's lock, on the calling thread, in four steps:

1. Evaluate the command against the key's complete current state, computing both the reply and the effect.
2. Reserve log space. A failure here leaves nothing applied, and the write is rejected.
3. Apply the effect to hot.
4. Fill and publish the frame.

The reply is sent when the frame reaches the configured durability class. Steps 3 and 4 cannot fail without terminating the process (§Fail-stop after apply).

**The log records decided effects, not intents** ("decide-then-log").
- Conditional and read-modify-write commands (`SET NX`, `ZADD GT`, `INCR`, `SPOP`) are logged as the concrete operations they decided on.
- An expiry that a decision observed is logged as an explicit delete before the new effect.
- Precedent: Redis effects replication, which turns `SPOP` into `SREM` and `EXPIRE` into `PEXPIREAT`, and propagates lazy expiry as `DEL`.
- The sequencer and recovery replay apply effects to hot through one function, so the two cannot interpret a frame differently.

**What this removes.** The steady-state hot consumer thread, the resolver, the `Conditional` and `Resolved` entry types, block-and-scan, the apply notifier and the read-consistency wait are all removed.
- Recovery no longer re-decides anything: replay applies effects.
- ADP-011's determinism constraint on decisions no longer applies.
- Read-modify-write commands become ordinary writes.

**Why ADP-011's reasons for a separate resolver no longer hold:**
- *Cold reads on the critical path.* The client waited for the resolver's cold lookup anyway.
- *Transactions.* `MULTI`/`EXEC` executes on the sequencer like any multi-key command: its effects are logged as one batch frame. `WATCH` compares each watched key's latest sequence number at `EXEC`, loading a non-resident key first. An aborted transaction has no effects, so it logs nothing.
- *Cross-pod lookups.* The sequencer is per shard on the owning pod, exactly where the resolver ran.

**Latency and portability.**
- The critical section is constant-time: no system calls, allocation or logging inside it, and a spin-then-park lock.
- Under skewed (zipfian) load one shard lock behaves like a global lock. Lock behaviour under skew is part of this phase's acceptance measurement.
- The shard is reached through an executor boundary ("run this on shard S"), so moving to one owner thread per shard later is a scheduler change, not a rewrite.

**Cross-shard atomic commands.**
- In single-pod deployments, multi-key write commands spanning shards execute atomically. The sequencer takes the involved shard locks in a fixed order and writes one batch frame carrying every shard's effects; batch atomicity is already a property of the log format.
- Until that lands, cross-shard conditional commands are rejected (#165).
- Multi-key reads (`MGET`, multi-key `EXISTS`) are not atomic across shards. They read each shard in turn and can observe a cross-shard write half-applied. #170 makes them atomic by taking the same locks shared, in the same order.
- The client-facing identity that goes with these semantics (standalone or cluster) is decided separately (#169).

**External queue profile.** With a broker as the log, the sequencer is the single producer for its shard's partition. During resharding an outgoing owner must not be able to interleave effects with the incoming one. Producer fencing (an idempotent producer with leader-epoch fencing, or transactional producer ids) is therefore required wherever a broker is the log.

### Residency invariant

For every key, at least one of these holds:
- hot holds the key's complete state, either a live entry or a tombstone; or
- the compaction buffer plus cold hold the key's current state.

After cold drains a resident key, both hold.

**Rules:**
- **A write to a non-resident key loads the key first,** outside the lock. The loaded state is installed only if the key is still non-resident and nothing that could change it was evicted from that shard while the load ran; otherwise the load is retried. The validation mechanism (a per-shard eviction epoch, or a finer eviction filter that bounds retries under eviction pressure) is specified with #160. It must not depend on stubs, because stubs can be dropped.
- **Hot evicts a key only after cold has drained past the key's latest write.**
- **An evicted key may leave a stub** holding its type, absolute TTL and latest sequence number, so existence-only replies (`EXISTS`, `TYPE`, `DEL`, `EXPIRE`) need no full load.
  - Stubs are a bounded cache, dropped under memory pressure. Hot memory must not grow with the total number of keys ever written.
  - Without a stub, existence replies fall back to a cold probe, which bloom filters make cheap for absent keys.
- **A cold read hit fills hot directly,** under the same validation, instead of appending a promotion entry. Reads no longer write to the log.

A hot miss implies that buffer plus cold is current, so the read path needs no consistency wait.

### Read visibility

No reply, to a read or a write, reflects a write that a failure in the configured class could lose. A command whose key's latest sequence number is above the class watermark waits for the watermark to pass, without blocking a thread. This includes replies that reveal prior state, such as `SET … GET`, `INCR` and a `DEL` count.
- Under `process_crash`, frames are filled before the critical section ends, so the wait fires only for frames still being filled.
- The wait matters under `power_loss`.

### Asynchronous request execution

**Reactors never wait on durability or on cold reads.** Completions are delivered back to the reactor.

**Ordering.** Each command on a connection is sequenced before the next one on that connection is dispatched, which keeps Redis pipeline order. Replies are written in command order. Concurrent and pipelined writes therefore share flushes.

**Backpressure.** A per-connection in-flight limit applies, alongside the existing write-buffer thresholds.

**Cold I/O.** Cold reads and key loads run on a small I/O pool and complete asynchronously.

### Execution resources

Cold consumers run as a pool sized to cores, not one thread per shard.
- For the in-memory window, they walk their shards' frames through per-shard offset rings.
- Beyond the window, they use the log's sparse in-memory index, which is rebuilt at open.

Recovery is a single demultiplexing pass over each log.

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
| ADP-001 | Read takes a position, and positions below the oldest retained entry are out of range. Acks become committed offsets, persisted lazily in one dual-slot checkpoint. Volatile consumers are removed. Group commit and fsync policies are replaced by natural batching and durability classes. The entry taxonomy loses `Conditional` and `Resolved`. | 1a, 1b, 2 |
| ADP-002 | Hot is applied by the sequencer. Residency invariant, droppable stubs, eviction gated on cold drain. | 2 |
| ADP-004 | Consumer loop reads by position, and a poison entry pins the commit offset while reads pass it. Absorption is capped at the power-durable watermark. Pooled workers. | 1a, 1b |
| ADP-005 | Asynchronous completion. The blocking head-of-line trade-off is removed. Conditional dispatch becomes sequenced writes. | 2, 3 |
| ADP-006 | Sequenced write path, read fence, cache-fill promotion, no read-consistency wait. The Flush acknowledgement no longer requires a persisted consumer offset: the Flush entry's own durability suffices, Wipe stays synced, and a replayed Flush is idempotent per shard. | 1a, 2 |
| ADP-007 | Resolver phase removed. Recovery syncs the log before replay. | 1b, 2 |
| ADP-008 | A broker-backed log requires producer fencing so an outgoing shard owner cannot interleave effects during resharding. | 2 |
| ADP-009 | Sparse in-memory index. Preallocated segments, where a zero length marks the end of the log. Physical log per volume with per-shard streams. Effect frames. | 1a, 1b, 2 |
| ADP-011 | Resolver, entry pair and block-and-scan superseded. Consumer RPC is reduced to admin and flush use. | 2 |
| ADP-013 | Target-to-substrate map, write probe, comparative drivers and matrix, load-driver pipelining. | 0 |

**Issues:**
- Unblocked: #67 (by Phase 0), #142 (by Phase 2).
- Absorbed or obsoleted: #125, #126, #127, #128, #137 and #88.
- Re-scoped: #59–#63 now trigger only if profiling shows the shard lock is the limit.

## Delivery

| Phase | Issue | Content |
|-------|-------|---------|
| 0 | #157 | Measurement: write probe with device-flush calibration, flush metrics, read-position micro-benchmark, a pipelining load driver, server-identified comparative runs. Targets in `requirements.md`. |
| 1a | #158 | Read and acknowledgement contract: consumer-owned positions, explicit out-of-range, sparse index with header-only skip, lazy dual-slot offset checkpoint. |
| 1b | #159 | Durability pipeline: natural batching, durability classes, mapped preallocated segments on the per-volume log, cold absorption ceiling, bounded durability window, fail-stop, recovery-time sync. |
| 2 | #160 | Sequenced write path, residency invariant, read visibility, cross-shard atomic commands, shard-lock measurement under skew; resolver removal. |
| 3 | #161 | Asynchronous request execution and asynchronous cold I/O. |
| Later | #162, #170, #88 | Blob lane, atomic multi-key reads, io_uring with several flushes in flight (watermark at the contiguous completed prefix), hot-table and allocator work driven by profiles. |

Every phase is measured against the Phase 0 baseline with the same instruments. A three-way property test checks hot, cold and a reference model against each other, replies included. It covers TTL boundaries, flushes at arbitrary points, Flush entries, eviction with loading, and multi-effect batches. It lands with Phase 2 and runs under the unit tier (seeded, bounded) and the fuzz harness (long).

## Trade-offs

**Why not shorten the commit window or acknowledge before the write reaches the OS?** A shorter window still adds latency at low load. Acknowledging before the OS has the data loses acknowledged writes on any process crash, the most common failure on Kubernetes.

**Why keep a separate hot consumer only for recovery?** Applying hot from a trailing consumer costs thread hops on every write. It is also the root of the conditional-reordering and partial-state defects. Applying hot as part of sequencing removes both.

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
- **Maximum value size before the blob lane lands (#162).** Kept at 64 MiB; the head-of-line effect of large values is reported by a metric.
- **Hardware for authoritative measurement.**
