# ADP-003: Cold Store

**Status:** Accepted
**Created:** 2026-04-09

## Context

The cold store is the durable on-disk tier of Abyss. It holds data that has been evicted from the hot store and serves reads that miss both hot and the compaction buffer. It must handle batch writes efficiently (from the cold consumer's compaction flushes) and support TTL-based expiry.

## Design

### Interface

Like the hot store, the cold store interface accepts typed operations. Reads arrive as a `ReadOp` variant, batch writes arrive as a span of `WriteOp` variants. The cold store never sees raw RESP commands — the typed operation layer centralises parsing in core, keeping storage backends free of protocol concerns.

`Exec` handles reads that miss both hot and the compaction buffer. `ApplyBatch` is the only write path — the cold consumer always flushes compacted state in batches. `Stats` reports disk usage and key counts. `Compact` triggers manual RocksDB compaction.

See `include/abyss/core/cold_store.h` and `include/abyss/core/ops.h` for the current interface.

### Built-in RocksDB Store

The Phase 1 cold store is backed by RocksDB on a PVC.

**Write pattern:** The cold store receives batch writes from the cold consumer. These batches are already compacted — the compaction buffer has collapsed intermediate writes into the final state per key. RocksDB's write batch API maps naturally to this pattern.

**Read pattern:** Cold reads are the fallback path. A cold read means the key was not in hot and not in the compaction buffer. Cold read latency target is < 5ms p99. RocksDB with bloom filters achieves this comfortably for point lookups.

**Compaction:** RocksDB has its own internal compaction. Abyss exposes the `Compact()` method for operator-triggered compaction but relies on RocksDB's automatic compaction for steady-state operation.

### TTL Expiry

The cold store applies the log as it was decided. It changes state only by the log's clock, never by the wall clock.

**Reads:** a read checks the key's absolute TTL against the wall clock, and answers nil for an expired key. It never deletes, so a read never writes.

**The log clock:** a shard's log clock is the `appended_at` of the oldest write its compaction buffer still holds unflushed. With nothing pending, it is the newest `appended_at` the buffer has absorbed. The cold consumer keeps it ([ADP-004](004-cold-consumer.md) §Expiry), and it never moves backwards.
- **Why deletion by it is safe.** `appended_at` is monotonic per shard, so every write not yet in cold has an `appended_at` at or after the clock. The write path decides each write against the key's live state, and logs an observed expiry as a DEL ([ADP-015](015-write-path-and-durability.md)). Every TTL a pending write relied on is therefore later than the clock. Deleting a key whose TTL is at or below the clock can never race a write that needed it.
- **Why not the wall clock.** A PERSIST or SADD decided just before the TTL can still be in the buffer when the wall clock passes it. A delete by the wall clock would land first, and the write would then apply to a missing key: the PERSIST is lost, and the SADD builds a fresh set without the members it merged into.
- **It is not persisted.** After a restart it starts at 0, so nothing expires until replay or new writes advance it.

**Active expiry:** a background sweeper periodically samples random keys from the cold store, and deletes those whose TTL is at or below their shard's log clock. The sampling rate adapts to the observed expired ratio:

```
loop:
    sample N random keys (default N=20)
    delete any that are expired
    expired_ratio = expired_count / N

    if expired_ratio > high_threshold (default 0.25):
        increase sample rate (shorter sleep, larger N)
    elif expired_ratio < low_threshold (default 0.05):
        decrease sample rate (longer sleep, smaller N)

    if cold_store_disk_usage > disk_pressure_threshold:
        force maximum sample rate
```

This ensures the cold store doesn't accumulate expired keys indefinitely while keeping CPU usage minimal under normal conditions. Under disk pressure, the scanner becomes more aggressive to reclaim space.

**Cost.** The log clock trails the wall clock by up to the buffer's hold time, and an idle shard's clock stops at its last write. An expired key therefore stays on disk until its shard's clock passes its TTL: after the shard's next write, on an idle shard. That costs space only, since reads already answer nil ([failure-modes.md](../../operations/failure-modes.md)).

#### Ownership and execution model

Active expiry is **a property of the cold-store backend**, not of the server. Each backend implements (or omits) it according to its topology:

- The embedded RocksDB backend owns a `TtlScanner` instance internally and runs it on a single low-priority thread spawned at server start.
- External backends (KVRocks, DragonflyDB, Redis-compatible stores) ship with their own native expiry mechanism and do **not** spawn an Abyss-side scanner.
- A future shared-nothing per-core deployment owns one `TtlScanner` per cold partition, driven as a cooperative task from the per-core reactor instead of a thread.

The server is unaware of which mechanism is in use. It calls `Start()` on the cold store after recovery completes and `Stop()` during shutdown; whether that activates zero, one, or many internal workers is the backend's decision.

#### Sampling

Random sampling is restricted to the type prefixes that carry a TTL — string records and collection meta records (see [ADP-010](010-cold-key-encoding.md) §TTL Encoding Summary). Member, hash field, and score-index entries inherit their parent collection's TTL; sampling them directly would waste cycles. The implementation uses a random byte-prefix iterator seek rather than a uniform-distribution scheme; the bias is acceptable because the rate is driven by an estimated ratio rather than a uniform per-key guarantee.

#### Concurrency: CAS-safe deletion

A delete that depends on read state — "this key was expired when I sampled it, so I will delete it" — must be linearisable with concurrent unconditional writes from the cold consumer. Without a concurrency control, a race between the sampler's read and a writer's re-set of the same key with a fresh TTL would clobber the new value. Each backend satisfies the invariant according to its model:

- The embedded RocksDB backend opens its DB as an `OptimisticTransactionDB` and runs the scanner's delete inside a transaction with `GetForUpdate` on the affected meta or string key. A concurrent write to the same key causes the commit to abort; the scanner counts this as a `conflicts` outcome and moves on. The scanner is the only path that deletes by TTL.
- External backends serialise all writes through their server-side protocol; the contract holds without any client-side coordination.
- A shared-nothing partition is single-threaded over its data; the contract holds trivially.

The contract does not hold by accident: every component that could delete an expired record must funnel through the backend's CAS-safe primitive. Bypassing it is an upstream bug that violates the active-expiry invariant ("never delete a key that hasn't expired").

### Configuration

```yaml
cold:
  backend: builtin_rocksdb
  data_path: /data/cold
  compaction_style: level
  write_buffer_size_bytes: 67108864   # 64 MiB
  max_write_buffer_number: 4
  bloom_filter_bits_per_key: 10
  ttl_expiry:
    active_enabled: true
    sample_size: 20
    base_interval_ms: 1000
    high_threshold: 0.25
    low_threshold: 0.05
    disk_pressure_threshold: 0.9      # Fraction of PVC capacity
    max_cpu_percent: 10               # Cap CPU budget for active expiry
```

## Invariants

1. `Exec` answers nil for a key expired by the wall clock, and never writes.
2. `ApplyBatch` is the only write path. Individual writes do not occur; the cold consumer always flushes in batches. It applies each effect as logged and judges no TTL.
3. The cold store is never read during recovery. Recovery is pure queue replay.
4. Active expiry deletes a key only once its TTL is at or below its shard's log clock. The sampling is probabilistic (which keys are checked) but the expiry check itself is deterministic.
5. Applying the same log leaves the same state whatever the wall clock reads.

## Trade-offs

**Why RocksDB?** It's the industry standard for embedded LSM-based storage. Write-optimised (batched writes map to its strengths), mature, Apache 2.0 licensed, widely deployed. The main downside is binary size and build complexity, which are acceptable for this use case.

**Why not delete on read, as Redis does?** In Redis the reader and the writer share one clock and one thread. Here a read runs on a reactor thread by the wall clock, while writes reach cold later, through the buffer. A read's delete could overtake a write decided while the key was live (see §TTL Expiry). Reads answer nil instead, and only the scanner deletes. Keys that are never read again are reclaimed all the same, and the adaptive sampling rate keeps CPU cost proportional to the expired key density.

**Why adaptive sampling instead of a fixed scan rate?** A fixed rate either under-scans (expired keys accumulate) or over-scans (wasted CPU). Adaptive sampling converges to the right rate for the workload. Under disk pressure, it forces maximum scan rate to reclaim space regardless of the normal adaptive pacing.
