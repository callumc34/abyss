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

The cold store implements a dual expiry model matching the industry-standard approach used by Redis.

**Lazy expiry:** On every cold store read, check the key's absolute TTL. If expired, delete it and return nil. This is zero-cost when keys are not being read.

**Active expiry:** A background thread periodically samples random keys from the cold store and deletes expired ones. The sampling rate ramps up adaptively based on the hit ratio (fraction of sampled keys that were expired):

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

1. `Exec` performs lazy TTL expiry — expired keys return nil and are deleted.
2. `ApplyBatch` is the only write path. Individual writes do not occur; the cold consumer always flushes in batches.
3. The cold store is never read during recovery. Recovery is pure queue replay.
4. Active expiry never deletes a key that hasn't expired. The sampling is probabilistic (which keys are checked) but the expiry check itself is deterministic.

## Trade-offs

**Why RocksDB?** It's the industry standard for embedded LSM-based storage. Write-optimised (batched writes map to its strengths), mature, Apache 2.0 licensed, widely deployed. The main downside is binary size and build complexity, which are acceptable for this use case.

**Why dual expiry instead of just lazy?** Lazy-only means keys that are never read again accumulate indefinitely on disk. For workloads with many short-TTL keys that are written once and never read, disk usage would grow unboundedly. Active expiry puts a cap on expired key accumulation. The adaptive sampling rate keeps CPU cost proportional to the actual expired key density.

**Why adaptive sampling instead of a fixed scan rate?** A fixed rate either under-scans (expired keys accumulate) or over-scans (wasted CPU). Adaptive sampling converges to the right rate for the workload. Under disk pressure, it forces maximum scan rate to reclaim space regardless of the normal adaptive pacing.
