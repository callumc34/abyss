# ADP-006: Read and Write Paths

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-10-09

> **Amended by [ADP-015](015-write-path-and-durability.md).** A write is decided, reserved in the log and applied to hot under the shard lock, then published and acknowledged at the configured durability class. A read that misses hot never waits, and a cold hit fills hot directly; promotion through the queue and the read-consistency wait are removed. FLUSHDB acknowledges on its Flush frames' own durability. The sections below describe this. The calling reactor thread still blocks on durability until asynchronous execution lands (ADP-015 §Asynchronous request execution).

## Context

The read and write paths are the core data flows through Abyss. The write path ensures durability and consistency through the queue. The read path provides tiered access across hot, buffer, and cold, filling hot from cold hits.

## Design

### Write Path

```
Client ──▶ RESP Frontend ──▶ Sequencer (on the calling thread)
                               │
                               ├─ 1. Wait, holding no lock: WAL admission,
                               │     memory backpressure, key loads
                               ├─ 2. Lock the keys' hot shards, decide
                               ├─ 3. Reserve the effects in the log
                               ├─ 4. Apply them to hot, unlock
                               ├─ 5. Fill and publish the frames
                               └─ 6. Wait for the ack class ──▶ reply
```

Every write, conditional or not, single-key or multi-key, takes this path ([ADP-015](015-write-path-and-durability.md) §Sequenced write path).

1. The RESP frontend validates the command by parsing it, extracts its predicate flags (`NX`, `XX`, `GT`, `GET`, ...), and hands the engine the client's spelling ([ADP-005](005-resp-frontend.md)).
2. **Wait first, holding no lock.** The write waits for WAL admission (the durability window), for memory backpressure when an earlier attempt found a shard over its limit, and for the loads of any non-resident keys it needs ([ADP-002](002-hot-store.md) §Residency).
3. **Decide.** The sequencer takes the exclusive hot locks of every shard the command's keys map to, in ascending order, and decides against hot's view of each key. Decide is a pure function of the command, its predicate flags, one wall-clock instant per hold (never behind the shard's last `appended_at`) and the key views. It returns a reply and zero or more effects, or the keys it must load first. An expiry it observes becomes an explicit `DEL` ahead of the command's own effect.
4. **Reserve** the effects in the log as `Write` frames, one reservation for the whole command. An effect that alone determines its key's state is flagged `kReplacesState` ([ADP-009](009-wal-format.md)). If the reservation is refused (the durability window is full, or no spare segment is ready), the sequencer unlocks, waits, and decides again from the start; nothing has been applied.
5. **Apply** the effects to hot ([ADP-002](002-hot-store.md) §Applying Writes), raise each shard's `appended_at`, and unlock. Apply cannot fail once reserved.
6. **Publish.** The lock-hold budget of 16 KiB is cumulative per reservation: frames are filled under the lock until their total reaches 16 KiB, and the reservation's later frames are filled after the lock is released. Then the frames are published in sequence order.
7. **Reply** once the frames reach the acknowledgement class ([ADP-001](001-queue-wal.md) §Durability classes and group commit). The reply is the decided one, or what applying the command's effect returned.

**Decisions.** The log carries decided effects, not intents:

| Command | Effect logged | Reply |
|---|---|---|
| `SET` with `NX`, `XX`, `KEEPTTL`, `GET` | a `SET` with an absolute TTL, or nothing | OK, nil, or the old value |
| `SETNX`, `HSETNX` | a `SET` or `HSET`, or nothing | 1 or 0 |
| `MSETNX` | one `SET` per key, as one batch, or nothing | 1 or 0 |
| `ZADD` with `NX`, `XX`, `GT`, `LT`, `CH` | the filtered `ZADD` | the count |
| The `EXPIRE` family with `NX`, `XX`, `GT`, `LT` | a `PEXPIREAT`, or nothing | 1 or 0 |
| `RENAMENX`, `COPY` | `DEL` and recreate pairs as one batch, for every type | 1 or 0 |

A decision with no effect (`SET NX` on a present key, for example) logs nothing and applies nothing. Its reply still waits until what it observed is durable (§The fence). A syntax error or `WRONGTYPE` is decided the same way: replied, with nothing logged.

**Multi-key writes.** `MSET`, multi-key `DEL` and `UNLINK`, `MSETNX`, `RENAMENX` and `COPY` are one decision and one reservation across their shards, so they are atomic to readers and across a crash. Their keys must share a WAL log. With `queue.log_count` above 1, a write whose keys span logs is rejected with `-CROSSSLOT` before anything is decided (#169).

**Failures before apply reject the write cleanly.** Every wait is bounded by `engine.write_timeout_ms` (default 5 s), which covers admission, loads, backpressure and the durable wait. A write that cannot be admitted, find a spare segment, load its keys or get past memory backpressure in that time is rejected, and nothing was logged or applied. Memory backpressure replies `-OOM command not allowed when hot memory is over its limit and cold is behind`. Each re-decision is counted in `abyss_sequencer_redecides_total` by reason.

**After apply, a write cannot be undone.** A write whose durable wait passes `engine.write_timeout_ms` replies with an error saying it is applied and may yet become durable. A failed flush or publish terminates the process ([ADP-015](015-write-path-and-durability.md) §Fail-stop after apply).

The queue is the sole write path. There is no dual write, and nothing but the sequencer, and recovery's replay of the log, applies hot. The consumer RPC promise registry through which a hot consumer and the resolver used to fulfil writes is removed: the sequencer has the reply when it unlocks, and waits only on the queue's durable future.

### Read Path

```
Read command arrives
  │
  ├─ Check Hot Store (shared lock)
  │   ├─ HIT (live key or tombstone) → copy the answer, wait until
  │   │     what it saw is durable (the fence), return; a live key's
  │   │     value read also stamps access
  │   ├─ Under the flush floor → absent
  │   ├─ Stub answers (EXISTS, TYPE, TTL, PTTL) → return
  │   └─ MISS (or a load in flight) ──┐
  │                                    │
  │   ┌────────────────────────────────▼────────┐
  │   │ Buffer, then cold, with no wait          │
  │   ├─ GET and scans: load the key; fill hot   │
  │   │   when admitted                          │
  │   ├─ Member reads: read the members only;    │
  │   │   fill a small collection when admitted  │
  │   ├─ SCARD, ZCARD, HLEN: exact count;        │
  │   │   fill likewise                          │
  │   └─ EXISTS, TYPE, TTL: probe; fill nothing  │
  │
  └─ Record metrics: hit tier (hot/buffer/cold/miss), latency
```

**A miss needs no wait.** Hot holds every key's complete state until cold has drained it ([ADP-015](015-write-path-and-durability.md) §Residency invariant), so buffer plus cold hold the current state of any key hot does not. A load placeholder reads as a miss for the same reason: a read never waits for another's placeholder to resolve, and reads buffer plus cold itself. The read-consistency wait (and its `engine.buffer_consistency_wait_timeout_ms` setting) and the hot consumer's settled floor it waited on are removed.

**Access stamps.** A hot hit on a live key stamps its access time, which extends its eviction deadline. Metadata reads (`EXISTS`, `TYPE`, `TTL`, `PTTL`) do not count as use, as in Redis, and a read answered by a tombstone stamps nothing.

**Buffer first, then cold.** A miss reads the key's compacted delta in the buffer, then cold, and merges them: adds, removes, overwrites and a whole-key delete over cold's base. The buffer keeps a flushed entry readable until its cold write lands, so "buffer miss, then cold" is never stale. Cold reads are loads: cold answers no command itself, and the engine answers the read from what was loaded, judging TTL by the wall clock. Point reads and probes get ADP-003's 5 ms cold deadline, and whole-key loads (a scan or a full collection) 50 ms. A whole-key load that misses its deadline is an error, never a truncated answer (`abyss_cold_scan_deadline_exceeded_total`).

**The fence.** A hot hit copies its answer under the shared lock with the highest latest seq of the keys it touched, unlocks, and waits until that seq is durable at the acknowledgement class before replying. The sequencer applies a write before it publishes the frame, so without the fence a reply could show a write a failure in the class could lose. Under `process_crash` the durable end is the published end, so the wait fires only while a frame is still being filled or published; under `power_loss` it waits for the flush. A miss answered by the flush floor fences on the Flush. A write with effects is covered by its own durable wait, because its frames follow everything it observed in the same log. A write with no effect fences on what it observed. Copying first gives a valid linearisation point and cannot starve on a key written continuously.

### Cache Fill

A cold hit fills hot directly, under a load token ([ADP-002](002-hot-store.md) §Residency), instead of appending to the queue. A read writes nothing to the log.

- **Admission.** A miss fills only on the key's second miss within a window (TinyLFU's doorkeeper, `hot.fill_doorkeeper`; off, every miss fills). A fill is skipped while the shard is over its backpressure limit, under the flush floor, or when it is larger than `hot.fill_max_fraction` of the shard's budget.
- **What fills.** `GET` loads the string with one point read and fills. The scans (`SMEMBERS`, `ZRANGE`, `HGETALL` and the like) load the whole key and fill. Member reads (`SISMEMBER`, `ZSCORE`, `HGET`, `HEXISTS`, `HMGET`) read only the asked-for members, and `SCARD`, `ZCARD` and `HLEN` count exactly from cold's count corrected by the delta; both fill a collection only below `hot.fill_max_members`. `EXISTS`, `TYPE`, `TTL` and `PTTL` answer from a stub or a cold probe, and fill nothing.
- **Sharing.** Concurrent misses on one key share one load and its result. A write that overtakes a fill discards it (`abyss_hot_load_discards_total`).
- `abyss_hot_fills_total` counts fills by outcome: installed, discarded, skipped for backpressure, size or the eviction cap, and failed.

**Promotion is removed (#168).** A cold hit used to append the value it read to the queue, and the hot consumer applied it. A write that raced the read could land first, and the promotion then rolled hot and cold back to the older value. A fill installs only if its token is still the key's placeholder, so a racing write always wins, and nothing is appended.

### Hot-tier delete tombstones

A read that misses hot consults the compaction buffer and cold. That overlay is the cold consumer's view of the queue and can lag hot: a write hot has already applied is not visible to the buffer until the cold consumer absorbs it. Consulting the overlay for a key hot has deleted would break read-after-write whenever the lagging state is a *deletion*:

- `SET k v` then `DEL k` then `GET k`: if cold absorbed the `SET` but not the `DEL`, a stale overlay returns `v` instead of `nil`.
- `HSET h a 1 b 2` then `HDEL h a b` then `HGETALL h`: a stale overlay resurrects the deleted fields.

Hot therefore keeps a **delete tombstone**. When the sequencer applies a delete (`DEL`/`GETDEL`, or an `SREM`/`ZREM`/`HDEL` that empties a collection), hot keeps an authoritative "absent" marker stamped with the delete's queue seq instead of erasing the key. A read of a tombstone returns `nil`/empty directly from hot — it never consults the overlay and never waits on cold. The tombstone is reclaimed by the eviction worker once the per-shard cold consumer's drained seq passes the delete's seq, at which point the overlay and cold reflect the delete and a true miss is safe. Tombstones do not count toward `DBSIZE`. See [ADP-002](002-hot-store.md) §Delete Tombstones.

A tombstone is one case of the residency invariant: hot holds the key's complete state (deleted) until cold has drained it.

### Broadcast Write Path (FLUSHDB / FLUSHALL)

FLUSHDB and FLUSHALL clear every key. To preserve the Kappa invariant — the queue is the single source of truth and every materialised view observes events in queue order — the wipe is routed through the queue rather than executed as a side-channel operation against the stores.

```
Client ──▶ RESP Frontend ──▶ Sequencer
                               ├─ WAL admission for every shard
                               ├─ Lock every hot shard, in ascending order
                               ├─ Reserve one Flush per shard, as one reservation
                               ├─ Wipe each hot shard at its Flush's seq
                               ├─ Unlock; fill and publish
                               └─ Wait until every Flush reaches the ack class
                                          │
                                          └──▶ +OK to client
```

- **Hot** wipes each shard under its lock: entries, stubs and load placeholders. The shard's flush floor is set to the Flush's seq, so until cold's drained seq passes it, a hot miss on that shard is absent with no cold read ([ADP-002](002-hot-store.md) §Residency). Readers see the whole wipe or none of it.
- **Cold** reads its shard's Flush like any entry, drops its compaction buffer (pre-Flush writes never reach cold), and wipes **its own shard's slice** of the cold store, once the Flush is power-durable ([ADP-004](004-cold-consumer.md)).
- FLUSHDB never waits for memory backpressure. If it cannot be admitted or find a spare segment within `engine.write_timeout_ms`, it is rejected and wipes nothing.

The client sees `+OK` once every shard's Flush frame has reached the acknowledgement class. Nothing waits for a consumer: the flush floor answers reads until cold's wipe lands. A durable wait that times out replies with an error; the Flushes are applied to hot and logged, and a retry is idempotent at the wipe level.

**Atomicity.** With one log the Flushes are one batch, atomic across a crash. With `queue.log_count` above 1, each log's Flushes are one batch: the wipe is atomic to readers, who need the locks, but a crash can leave some logs flushed and not others (#169).

**Per-shard wipe isolation.** Cold's wipe of a shard deletes only the keys whose shard slot equals that shard (ADP-010 §Per-shard wipe). A single embedded RocksDB instance backs all of a pod's shards, but the shard-prefixed key encoding partitions it into disjoint per-shard slices, so a shard's wipe touches no peer's data. This closes a cross-shard data-loss race that a global wipe exposed: during parallel recovery replay (ADP-007) or an aggressive-mode early flush, a lagging shard's replayed `Flush` would re-run a global wipe and destroy data a peer shard had already flushed to cold after *its* own `Flush`. With per-shard isolation, a replayed `Flush` re-wipes only its own slice and is idempotent against it; cross-shard interleaving is irrelevant.

**Durability of the wipe.** A FLUSHDB `+OK` never runs ahead of durability:
- The reply waits until every shard's `Flush` frame is durable at the acknowledgement class, and a read answered by the flush floor fences on the Flush.
- Cold's wipe is a synced write, made only once the `Flush` is power-durable, so cold never holds a wipe the log could lose.
- A replayed `Flush` is idempotent per shard (per-shard wipe isolation), and retention is gated on persisted committed offsets, so every write after a `Flush` is always replayable.

A consumer's committed offset therefore need not have reached the `Flush` before the reply. The earlier design made FLUSHDB wait for every consumer (hot, cold and the resolver) to apply the wipe and fulfil a Flush RPC; the flush floor removed that wait, and with it the consumer RPC.

Multi-pod deployments extend this: each pod receives the broadcast at the RESP layer and flushes its owned shards. There is no cross-pod synchronisation step.

### Multi-Key Fan-Out

The command registry tags each multi-key command with its kind, and the dispatcher routes tagged commands through its fan-out path:

- **Reads** (`MGET`, `EXISTS`) — issue per-key single-key reads through the read path above, each fenced as a single read; aggregate positional array (MGET) or sum (EXISTS). They are not atomic across shards: they read each shard in turn and can observe a cross-shard write half-applied (#170).
- **Writes** (`MSET`, `DEL`, `UNLINK`) — go to the sequencer as one command: one decision under every involved shard's lock and one reservation (§Write Path). A refused one applies none of its keys.

Aggregation rules: `MGET` returns a positional array (nil on miss or WRONGTYPE per Redis); `EXISTS` returns the int count without dedup (`EXISTS k k` returns 2); `MSET` returns `+OK`; `DEL`/`UNLINK` returns the number of keys deleted.

### Metrics

Every read records which tier served the response:

- `abyss_hits_total{tier="hot"}` — hot store hit
- `abyss_hits_total{tier="buffer"}` — compaction buffer hit
- `abyss_hits_total{tier="cold"}` — cold store hit
- `abyss_misses_total` — key not found in any tier

A read that fills hot counts where the fill read it, not as a hot hit. The write path reports `abyss_sequencer_lock_hold_seconds`, `abyss_sequencer_redecides_total` and the backpressure metrics; fills report `abyss_hot_fills_total` and `abyss_hot_load_discards_total`.

### Canonical form on the write path

A write is parsed once, at the frontend, before anything reaches the engine, and the engine logs the effect it decides re-expressed canonically. Canonical means one spelling per operation: command aliases collapsed (`SETEX` and `SET ... EX` become the same `SET`), and relative TTLs already resolved to the absolute instant they denote, the instant the sequencer decides at. The contract is a round trip: re-parsing the canonical form must reproduce the operation the client's original spelling produced. That is what keeps the change invisible to the reply and to both tiers, since a reply and a tier mutation are each a pure function of the parsed operation. Commands that are distinct operations stay distinct commands even where their arguments coincide — `HMSET` acknowledges with `OK` where `HSET` returns a count, so it is not a spelling of `HSET`.

Two properties follow. Malformed input cannot be made durable, because a command that does not parse never reaches the engine; the parse *is* the validation, rather than a separate check that could drift from it. And every reader of the log — cold and recovery's replay — sees one spelling with nothing left to re-derive, so there is no opportunity for two readers to derive different meanings from the same entry. Absolute TTLs in particular are fixed at the moment of the write rather than recomputed against each entry's append timestamp, which removes replay's dependence on that timestamp agreeing across tiers.

Conditional writes are validated the same way, and the log carries their decided effects in canonical form, never the predicate (§Write Path). Commands with no parser in the build are exempt from validation entirely: that is a capability gap rather than malformed input, and the command registry remains the sole authority for them.

## Invariants

1. Reads check tiers in order: hot → buffer → cold. No tier is skipped, except that a stub or the flush floor answers in hot.
2. Hot hits on a live key refresh the eviction timer, except metadata reads. Tombstone reads, buffer hits and cold hits do not, though a fill installs the key in hot.
3. Reads never write to the log. A cold or buffer hit may fill hot directly, under a load token.
4. A write is never acknowledged until it is durable at the acknowledgement class. The sequencer applies it to hot before it is published, and a read that sees it waits until it is durable.
5. A write is decided once, against hot's complete state, under its shards' locks; the log carries the decided effects, and nothing that waits holds a lock.
6. A recent delete is an authoritative hot tombstone: reads of a deleted key (`GET`, `EXISTS`, emptied-collection reads) return `nil`/empty from hot without consulting the lagging overlay or waiting. A read that misses hot reads buffer plus cold with no wait: hot holds a key's complete state until cold has drained it. Tombstones are reclaimed once cold has drained past the delete and do not count toward `DBSIZE`.

## Trade-offs

**Why fill hot directly rather than promote through the queue?** Promotion gave a cold hit a fresh queue entry so it would survive a crash, but the key is already in cold, so the entry bought nothing, and it raced writes (#168). A fill changes residency only, never a value: losing it in a crash costs one cold read.

**Why a doorkeeper on fills?** A scan of keys read once would otherwise push the working set out of hot. Filling on the second miss within a window admits keys that are read again, as TinyLFU does.

**Why block the write on durability and apply?** The client expects that after receiving OK, a subsequent read returns the written value. The sequencer applies the write to hot before replying, so a read after the reply sees it; and the fence keeps any reply from showing a write before it reaches the class.

**Why decide under the lock rather than in a separate resolver?** The resolver decided against a cache and cold, off the write path, and logged a `Conditional` and later a `Resolved`; the effect then landed at the `Resolved` position, after writes it had not seen (#167). Deciding under the shard lock against hot's complete state logs each decision once, at its own position ([ADP-015](015-write-path-and-durability.md) §Sequenced write path).

## Amendment: the settled floor (removed)

An earlier amendment (finding HOTC-7) defined the hot consumer's *settled floor*: the highest seq that was both applied to hot and not behind a pending conditional. The read-consistency gate waited for cold's drained seq to reach it. ADP-015 removed the hot consumer, pending conditionals and the gate, so the floor is gone with them. A decision is applied at its own position under the shard lock, and a read that misses hot never waits.
