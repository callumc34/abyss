# ADP-002: Hot Store

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-10-09

> **Amended by [ADP-015](015-write-path-and-durability.md).** Hot is applied by the per-shard sequencer as part of each write; no consumer trails the log to apply it. It is subject to the residency invariant: writes to non-resident keys load them first, eviction waits for cold to drain the key's latest write, and evicted keys leave droppable stubs. The sections below describe this.

## Context

The hot store is the in-memory tier of Abyss. It serves the lowest-latency reads and is the first tier checked on every read. The sequencer applies each write to it as part of the write, and I/O threads read from it concurrently.

## Design

### Interface

The hot store accepts typed operations rather than raw RESP commands. Reads arrive as typed read operations (e.g. string get, set membership check, sorted set range query), and writes as typed write operations (e.g. string set, set add, sorted set add). This decouples RESP parsing from storage — the RESP frontend parses once, and stores execute typed results without string-matching on command names.

A read hit on a live key stamps its access time, which extends its eviction deadline. Metadata reads (`EXISTS`, `TYPE`, `TTL`, `PTTL`) do not count as use, as in Redis, and a read that finds a tombstone stamps nothing. A write links the key as just written, with the eviction configured for the key's prefix.

The hot store is not pluggable. Since ADP-015's Phase 2 it is in-process, and the sequencer decides each write against it under the shard lock, so the server always builds the built-in sharded store and the engine's components take that type directly. The built-in store provides what the sequencer needs: a view of each key under the shard lock, one function that applies decided effects, load placeholders, and stubs. An abstract hot store interface still exists in the code (`abyss/core/hot_store.h`), but nothing selects among implementations. Whether an external hot tier survives at all is an open decision, [#187](https://github.com/callumc34/abyss/issues/187).

The built-in hot store supports strings, sets, sorted sets and hashes. Lists, bitmaps, hyperloglog, geo and streams are later candidates.

### Built-in Concurrent Hash Map

The Phase 1 hot store is a concurrent hash map with sharded locks.

**Lock striping:** The keyspace is divided into N lock shards (fixed at deployment). Each key maps to a shard through its CRC16 slot ([ADP-014](014-slot-routing-and-topology.md)). I/O threads acquire a shared lock on the relevant shard for reads. The sequencer acquires the shard's exclusive lock to decide, reserve and apply a write, and a multi-key write takes its shards' locks in ascending order.

The number of lock shards should equal the planned horizontal shard count. Phase 1's internal sharding boundaries then match Phase 2's pod boundaries, easing migration.

**Data structures per key:**
- Value storage (string, set members, sorted set members + scores)
- Latest seq: the queue sequence id of the last write applied to the key. Eviction waits until cold has drained it (§Residency).
- Eviction deadline: the later of the last write and the last read, plus the key's eviction (steady clock). A read stamps the time the eviction worker last published, so at most one tick old, without an exclusive lock.
- Absolute TTL deadline (wall clock timestamp, not refreshed)

### Eviction

Keys live in the hot store for their `eviction` duration, which is refreshed on every read hit on the key's value (§Interface). When a key's eviction deadline passes without a read, it is evicted from hot, once cold has drained its latest write. The key remains available in buffer plus cold — eviction is a tier transition, not a deletion.

Eviction duration is configured globally or per-prefix:

```yaml
hot:
  default_eviction_seconds: 86400
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300
```

The resolved per-key eviction is computed once at apply time (longest-prefix wins, default if no match) and **cached on the per-key store entry**. Read-driven refresh extends the deadline using that cached value; the per-read path performs no prefix scan. This makes refresh O(1) and keeps the multi-key write path (e.g. `MSET` across mixed prefixes) honest — each entry resolves independently. Configuration is immutable post-startup, so the cached value is canonical for the key's hot residency. The `min_retention_seconds` constraint coupling the WAL retention to `max(default_eviction, max(eviction_overrides))` is enforced at startup by the config validator (see [Requirements §Consumer Coordination](../requirements.md#consumer-coordination)).

**LRU eviction under memory pressure:** If the hot store's memory usage exceeds its configured maximum, keys are evicted in LRU order (by last write, a read since giving a key a second chance) regardless of their eviction deadline. Only keys cold has drained are evicted, so an evicted key is already readable from buffer plus cold. Reads for prematurely evicted keys fall through to the buffer and cold store, and may fill hot again ([ADP-006](006-read-write-paths.md) §Read Path).

**Absolute TTL:** If a key has an absolute `ttl` set (via Redis `SET ... EX`, `EXPIRE`, etc.), the hot store tracks this separately. When the absolute TTL expires, the key is deleted — not just evicted. Hot drops it without a stub. A write that finds it expired logs an explicit `DEL` first ([ADP-015](015-write-path-and-durability.md) §Sequenced write path), and cold deletes it by its log clock ([ADP-003](003-cold-store.md) §TTL Expiry). This is different from eviction: TTL expiry means the data is gone.

### Delete Tombstones

A delete is not the same as an eviction. When the sequencer applies a delete — `DEL`/`GETDEL`, or an `SREM`/`ZREM`/`HDEL` that empties a collection — it does not simply erase the key: it retains a **tombstone** stamped with the delete's queue sequence id. A `DEL` of a key hot does not hold leaves one too, so a write's new state never lives only in a droppable stub. The tombstone is hot's complete state for the key. A read of a tombstoned key returns `nil`/empty directly from hot, so it never falls through to the compaction buffer or cold, which may still hold the pre-delete value until the cold consumer absorbs the delete. See [ADP-006](006-read-write-paths.md) §Hot-tier delete tombstones.

A tombstone is reclaimed by the eviction worker once the per-shard cold consumer's drained sequence id has passed the delete's seq: at that point the buffer and cold reflect the delete, so a plain miss is safe. The worker reads the cold consumers' drained seq through a narrow horizon function injected at construction, so the hot store stays unaware of the consumer layer. Tombstone memory is therefore bounded by the delete rate times the hot→cold lag, surfaced via `abyss_hot_tombstones_reclaimed_total`. A tombstone never counts toward `DBSIZE` or the key count, and a subsequent write to a tombstoned key revives it as a live key.

TTL expiry is **not** tombstoned: hot, the compaction buffer, and cold each apply the same absolute TTL deterministically, so a TTL-expired hot miss is safe to serve from cold without a tombstone.

### Applying Writes

The sequencer applies every write to hot, on the calling thread, under the shard's exclusive lock ([ADP-015](015-write-path-and-durability.md) §Sequenced write path). It decides the command against hot's view of each key, reserves the decided effects in the log, applies them, and unlocks; the frames are then filled and published. Per shard, sequence order, log order and hot apply order are therefore the same order, and every decision sees every earlier write.

**Behaviour:**
- Effects are applied by one function, which recovery's hot replayer also uses ([ADP-007](007-recovery.md) §Hot replay), so the write path and replay cannot interpret an effect differently.
- Applying stamps each written key with its effect's seq, links it as just written with the eviction for its prefix, and drops its stub and any load in flight, which the write has made stale.
- It judges expiry only at the effects' own `appended_at`, the instant they were decided at, and reads no clock. A key past its TTL then is absent to them, so replay sees what the decision saw, including a key cold's sweep deleted with no `DEL` logged. Decide still logs a `DEL` for every expired key it read.
- Decided values are moved into hot, not copied. What an apply replaces is freed after the lock is released.
- Apply cannot fail once the effects are reserved. A decided effect that does not parse is fatal, because decide produced it. Errors such as `WRONGTYPE` are decided, replied, and logged as nothing.

Each shard is independent: one shard's writes never take another's lock, except a multi-key write's own shards. Steady state has no hot consumer thread, no promise to fulfil and no lag budget: a write's reply waits only for its frame to reach the acknowledgement class.

**Eviction refresh vs queue retention:** Read refreshes extend a key's life in the hot store indefinitely, but the key's queue entries are subject to normal retention (`min_retention_seconds`). Hot evicts nothing cold has not drained, and cold's commit gates retention, so a key whose frames were reclaimed is already in cold. After a restart the hot replayer rebuilds a key only from a frame that determines it whole, so a key whose retained frames start mid-history stays non-resident, and the sweep after replay evicts keys whose last write is past their eviction window ([ADP-007](007-recovery.md) §Hot replay). The first read of such a key after recovery reads it from buffer plus cold, and may fill hot; nothing is written to the log. Cost: one cold-path read per such key after recovery.

**Eviction worker:** A dedicated maintenance thread, the eviction worker, publishes the time read hits stamp, then each tick runs the maintenance passes: keys idle past their eviction, keys past their TTL, LRU down to the memory budget, and tombstone reclamation. The tick interval is operator-configurable (`hot.eviction_tick_ms`, default 1s). It starts once recovery completes.

**Bounded maintenance:** Every pass holds a shard exclusively for at most 64 keys examined or 1 ms, whichever comes first, then releases it and resumes. No pass scans the keyspace:
- Idle eviction walks each eviction class's LRU list from its cold end. A write relinks its key at the warm end; a read only stamps it, so a key read since it was linked gets a second chance (CLOCK, as SIEVE) and is relinked at its read. The walk stops at the first key not due by its link time.
- Because a read relinks its key only when the walk reaches it, deadline eviction is lazy in one direction: a key never leaves hot before its deadline, and leaves within one eviction window after it. That bound excludes two waits: up to one eviction tick for the pass to run, and however long cold takes to drain the key's latest write (§Residency). Memory-pressure eviction bounds memory either way.
- TTL expiry takes keys from an index of one-second buckets holding the entries themselves, earliest first; the current second's bucket is checked key by key, so no key expires early. The pass gets a quarter of the tick and takes another hold while more than a quarter of the keys it examined had expired.
- A key that is due but not yet drained by cold waits in a heap ordered by its last write's seq, with delete tombstones in one of their own, and is taken when the drain horizon passes it. Nothing is examined twice per horizon.
- What a pass removes is freed after the hold.

### Residency

The residency invariant ([ADP-015](015-write-path-and-durability.md) §Residency invariant): for every key, hot holds its complete state, live or tombstoned, or the compaction buffer plus cold hold its current state. A hot miss therefore needs no wait (ADP-006).

**Eviction is gated on cold's drain (#126).** A key is evicted (by deadline, by TTL, by LRU, or to make room for a write or a fill), and a tombstone is reclaimed, only once its latest seq is at or below the shard's drained seq. The drained seq means absorbed into the compaction buffer, where the key is readable.

**Writes to non-resident keys load first.** A write that needs a key's state, and finds no entry that answers it, unlocks, loads what it needs off the lock, and decides again.
- Plain `SET` and `MSET` need nothing.
- A predicate that asks only whether the key exists (`SET NX`, `SETNX`, `MSETNX`, `DEL`'s count, a `RENAMENX` or `COPY` destination) is answered by a stub, or by an existence load that reads no members.
- Collection writes and the `EXPIRE` family load the key whole: buffer first, then cold, merged.

**Load tokens.**
- Under the lock, a loader installs a placeholder for the key carrying a fresh token, then loads with no lock held.
- It installs what it read only if that same token is still the key's placeholder and the key has no entry. The check is exact: no collisions, no livelock under eviction pressure.
- Readers treat a placeholder as a miss and read buffer plus cold themselves: a read never waits for a placeholder to resolve.
- A blind write replaces the placeholder, and the load then discards its result (`abyss_hot_load_discards_total`). A non-blind write waits for the placeholder to resolve, within its deadline.
- Eviction never touches placeholders. A failed load removes its placeholder and wakes its waiters. A Flush clears placeholders, so a load that began before a FLUSHDB can never install pre-flush state.

**Stubs.**
- An evicted live key leaves a stub holding its type, absolute TTL and latest seq. A key expired by TTL leaves none.
- Stubs answer existence without a load: `EXISTS`, `TYPE`, `TTL` and `PTTL` reads, and the existence predicates above. Without a stub, existence falls back to a cold probe, which bloom filters make cheap for absent keys.
- The stub cache is bounded per shard and drops its oldest stub, so hot memory does not grow with the number of keys ever written. It is sized by `hot.stub_memory_fraction` of `hot.max_memory_bytes` (default 2%) at about 80 B a stub, and its bytes count toward hot memory.
- A write to the key drops its stub, and a Flush clears the shard's stubs. Nothing depends on a stub being there.

**Absent loads.** A load that finds a key absent leaves a tombstone at seq 0, which every drain horizon covers, so repeated misses on an absent key stay in hot. These are bounded by `hot.negative_max_entries` across shards, oldest dropped first.

**The flush floor.** Applying a Flush wipes the shard's entries, stubs and placeholders, and records the Flush's seq. Until cold's drained seq passes it, a miss on that shard is absent, with no cold read. FLUSHDB therefore acknowledges without waiting for cold's wipe ([ADP-006](006-read-write-paths.md) §Broadcast Write Path).

**Memory backpressure.** Hot may exceed `hot.max_memory_bytes` only by what cold has not drained. Backpressure is per shard: each shard's budget is `hot.max_memory_bytes` ÷ `hot.shard_count`, and once a shard is over its budget × `hot.backpressure_ratio` (default 1.25), a write to it that can grow memory waits, with every lock released, for cold to drain and hot to evict. At `engine.write_timeout_ms` it fails with `-OOM`, having logged and applied nothing. A write's memory is judged before the keys it loads go in: evicting those would only load them again, so an entry larger than a shard's limit does not refuse the writes that need it. Under skew one hot shard can reject writes while hot's total memory is under `hot.max_memory_bytes`. `abyss_hot_backpressure_waits_total`, `abyss_hot_backpressure_rejections_total` and `abyss_hot_unevictable_bytes` report it.

**Cache fills.** A read miss may install the loaded key in hot ([ADP-006](006-read-write-paths.md) §Read Path), under the same token check. Hot refuses a fill larger than `hot.fill_max_fraction` of the shard's budget, any fill while the shard is over its backpressure limit, and any fill under the flush floor.

### Configuration

```yaml
hot:
  backend: builtin_hashmap
  max_memory_bytes: 4294967296        # 4 GiB
  default_eviction_seconds: 86400     # 24 hours
  eviction_tick_ms: 1000              # eviction worker's maintenance interval
  shard_count: 64                     # lock shards; the queue and cold use the same count
  stub_memory_fraction: 0.02          # share of max_memory_bytes for stubs
  backpressure_ratio: 1.25            # writes that grow a shard wait past this multiple of its budget
  fill_doorkeeper: true               # a read miss fills on the key's second miss
  fill_max_members: 1024              # member reads fill collections below this size
  fill_max_fraction: 0.0625           # largest fill, as a share of a shard's budget
  negative_max_entries: 65536         # absent loads held, across shards
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300
```

The `hot_consumer` section is removed: the sequencer applies each write to hot, and recovery's scan rebuilds it. A configuration that still has the section fails to load, naming it and what replaced it.

## Invariants

1. A read hit on a live key stamps its access time, which extends its eviction deadline. Metadata reads and tombstone reads stamp nothing.
2. Applying a write links its key as just written, with the eviction configured for its prefix.
3. A key evicted from hot is never deleted — it remains available in buffer plus cold. Hot evicts a key only after cold has drained its latest write.
4. A key whose absolute TTL has expired is deleted from hot (and cold, via the cold store's expiry mechanism).
5. A delete leaves a tombstone in hot (stamped with the delete's queue seq), not an erasure: reads of a deleted key are authoritatively absent from hot until the cold consumer has drained past the delete, at which point the eviction worker reclaims the tombstone. Tombstones do not count toward the key count.
6. Hot is applied in-process by the sequencer, under the shard's lock, in sequence order. A reply that shows an applied effect waits until the effect's frame is durable at the acknowledgement class ([ADP-006](006-read-write-paths.md) §The fence).
7. For every key, hot holds its complete state, or buffer plus cold hold its current state. A load installs only under its own token; stubs are never needed for correctness.

## Trade-offs

**Why sharded locks instead of lock-free structures?** Lock-free concurrent hash maps are complex, harder to reason about, and harder to extend with eviction tracking. Sharded locks give good concurrency (contention is proportional to 1/N where N is shard count) and are straightforward to implement correctly. The shard count can be tuned to match core count.

**Why not thread-per-core (Seastar model) from the start?** Thread-per-core eliminates locks entirely by partitioning data across cores. It's the right end state (Phase 4) but requires a custom runtime or Seastar dependency. Phase 1's sharded lock model is simpler to build, debug, and reason about while we validate the architecture. The shard-aligned design means migration to thread-per-core changes the concurrency model but not the data model.

**Why does the sequencer apply hot, rather than a consumer tailing the log?** A trailing consumer cost thread hand-offs and a promise on every write, and it was the root of two defects: conditional effects applied at a later position than they were decided (#167), and writes to evicted keys built partial state (#163). Applying under the shard lock makes hot apply order the log order, and lets every decision see the key's complete state ([ADP-015](015-write-path-and-durability.md) §Trade-offs).

**Why load tokens rather than an eviction epoch or filter?** An epoch or a counter shared by many keys makes a load retry whenever any key in its bucket is evicted, which livelocks under eviction pressure. A token belongs to one key and one load, so the check is exact, and a load is discarded only when its own key was written or flushed.

**Why are stubs droppable?** Hot memory must not grow with the total number of keys ever written. A stub only saves a load; without one, existence costs a cold probe.
