# Deployment

## Kubernetes

Abyss is designed for StatefulSet deployment with PVCs. It exposes three ports:

| Port | Name | Purpose |
|------|------|---------|
| 6379 | resp | Redis protocol (client connections) |
| 9090 | metrics | Prometheus scrape target |
| 8080 | admin | Health endpoints, status API |

### Single-Pod StatefulSet (Embedded Profile)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: abyss
spec:
  serviceName: abyss
  replicas: 1
  template:
    spec:
      containers:
        - name: abyss
          image: abyss:latest
          ports:
            - containerPort: 6379
              name: resp
            - containerPort: 9090
              name: metrics
            - containerPort: 8080
              name: admin
          resources:
            requests:
              memory: "4Gi"
              cpu: "2"
            limits:
              memory: "8Gi"
              cpu: "4"
          volumeMounts:
            - name: cold-storage
              mountPath: /data/cold
            - name: queue-wal
              mountPath: /data/wal
          env:
            - name: ABYSS_PROFILE
              value: "embedded"
            - name: ABYSS_CONFIG_PATH
              value: "/etc/abyss/config.yaml"
          livenessProbe:
            tcpSocket:
              port: resp
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: admin
            periodSeconds: 5
          startupProbe:
            httpGet:
              path: /ready
              port: admin
            failureThreshold: 60
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata:
        name: cold-storage
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 50Gi
    - metadata:
        name: queue-wal
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
```

### Multi-Pod (Phase 2+, External Profile)

Multi-pod deployment uses a StatefulSet with one pod per shard group, fronted by a headless Service for direct pod addressing. Clients use the Redis Cluster protocol to discover and route to the correct pod. See [ADP-008](../design/proposals/008-horizontal-scaling.md).

### Probes

**Liveness:** TCP check on the RESP port. If the process is alive and the port is bound, the pod is live.

**Readiness:** HTTP check on `/ready`. Returns 200 only after recovery is complete. During recovery, the pod returns 503 and the RESP port returns `LOADING` errors.

**Startup:** Same as readiness, but with a higher failure threshold (60 attempts at 10s intervals = 10 minutes). Recovery may take significant time for large queues.

## Configuration Reference

### Embedded Profile

```yaml
profile: embedded                     # the only profile the server starts with

hot:
  backend: builtin_hashmap
  max_memory_bytes: 4294967296        # split evenly across shards
  default_eviction_seconds: 86400
  eviction_tick_ms: 1000
  shard_count: 64              # queue and cold consumers use the same count (max 65536)
  stub_memory_fraction: 0.02   # share of max_memory_bytes for evicted keys' stubs (0-0.5)
  backpressure_ratio: 1.25     # per shard: writes that grow it wait, then OOM, past this x its budget (1-10)
  fill_doorkeeper: true        # a read miss fills hot on the key's second miss
  fill_max_members: 1024       # SISMEMBER/HGET... fill only smaller collections
  fill_max_fraction: 0.0625    # never fill a key over this share of a shard's budget, in (0, 1]
  negative_max_entries: 65536  # keys held as known absent, across shards
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300

cold:
  backend: builtin_rocksdb
  data_path: /data/cold
  write_buffer_size_bytes: 67108864
  ttl_scanner:                        # active deletion of expired keys
    enabled: true
    base_sample_size: 20
    min_sample_size: 5
    max_sample_size: 200
    base_interval_ms: 1000
    min_interval_ms: 100
    max_interval_ms: 60000
    high_threshold: 0.25              # expired ratio above this ramps up
    low_threshold: 0.05               # expired ratio below this ramps down
    rate_increase_factor: 1.5
    rate_decrease_factor: 0.7
    disk_pressure_threshold: 0.9      # fs fraction at data_path
    disk_pressure_release_threshold: 0.855
    max_cpu_fraction: 0.10            # scanner CPU cap (EWMA)
    cpu_ewma_window_seconds: 30

queue:
  backend: builtin_wal
  wal_path: /data/wal
  segment_size_bytes: 134217728       # per log segment
  max_value_size_bytes: 67108864      # largest single value (up to 512 MiB, below the segment)
  log_count: 1                        # physical logs, power of two <= shard count
  ring_entries: 65536                 # per-shard offset ring (power of two, 4096-2^24)
  min_retention_seconds: 86400
  offset_fsync_interval_ms: 1000      # committed-offset checkpoint cadence (10-60000)
  durability: process_crash           # process_crash | power_loss
  durability_window_bytes: 67108864   # unflushed WAL bytes across shards (1 MiB-4 GiB)
  durability_window_ms: 1000          # oldest unflushed entry per log (10-60000)

cold_consumer:
  quiet_threshold_seconds: 30
  safety_margin_seconds: 300
  jitter_fraction: 0.1
  buffer_high_water_bytes: 536870912
  buffer_low_water_bytes: 0             # 0 = auto, 3/4 of high_water
  max_flush_batch_size: 10000
  queue_read_max_count: 1024
  queue_read_timeout_ms: 50
  retry_initial_backoff_ms: 50
  retry_max_backoff_ms: 30000
  checkpoint_max_flushes: 32            # cold fsync at most every N applied batches...
  checkpoint_min_interval_ms: 50        # ...or this interval, whichever comes first
  drain_grace_seconds: 15               # per-shard graceful drain budget on SIGTERM

engine:
  write_timeout_ms: 5000              # a write's whole budget, durable wait included

recovery:
  replay_parallelism: 4               # workers the one recovery scan replays shards on

net:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300
  io_threads: 0                       # 0 = auto: min(hardware_concurrency, 16)
  shutdown_grace_seconds: 30

metrics:
  enabled: true
  bind: 0.0.0.0
  port: 9090

admin:
  enabled: true
  bind: 0.0.0.0
  port: 8080

log:
  level: info                         # trace | debug | info | warn | error | critical | off
  format: json                        # json | text
  sink: stdout                        # stdout | stderr
```

The parser is strict: an unknown section or key fails the load, naming it. [`config/abyss.example.yaml`](../../config/abyss.example.yaml) lists every accepted key.

**Removed settings.** A config that still names one fails to load, and the error names the setting and what replaced it:
- the `hot_consumer` section: the sequencer applies each write to hot, and recovery's one log scan rebuilds it;
- the `consumer_rpc` section: a write replies once it is durable, with no consumer apply to wait for;
- `recovery.hot_replay_batch_size` and `recovery.cold_replay_batch_size`: recovery reads the log once, in the scan's batches.

### WAL sizing

**Disk.** Each log holds its retained segments, one active segment, two prepared spares, and up to two reclaimed segments waiting for reuse. Size the WAL volume for `min_retention` at the peak write rate, plus five segments per log.

**Warm-up bandwidth.** Until retention first reclaims a segment (`min_retention`, 24 h by default), every new segment is zero-filled before use, so the WAL writes each byte twice.
- A sustained write rate above about half the volume's bandwidth can run the spares out during that period.
- Appends then wait for a spare, and are rejected at their deadline.
- `abyss_wal_spare_segments`, `abyss_wal_spare_waits_total` and `abyss_wal_segments_grown_total` show it.
- After warm-up, reclaimed segments are recycled and each byte is written once.

**Memory.**
- **Sparse index:** one 16-byte point per 64 KiB of retained log, about 0.025% of the retained WAL. For example, 10 MB/s with 24 h retention retains about 864 GB and indexes it in about 210 MB. `abyss_wal_index_bytes` reports the live figure.
- **Hot entry map buckets:** each shard's map is sized at startup for `hot.max_memory_bytes` at 512 bytes per key, 8 bytes a bucket: about 1.6% of `hot.max_memory_bytes` (67 MiB at the 4 GiB default), outside it. It removes the stall of rehashing a whole shard under its lock; `abyss_hot_rehash_seconds` shows any rehash that still happens.
- **Offset ring:** `ring_entries` slots of 16 bytes per shard, allocated at start. That is 1 MiB per shard at the default, 64 MiB at 64 shards, outside `hot.max_memory_bytes`. `abyss_wal_ring_bytes` and the `WAL opened` log line report it. The ring must cover how far consumers normally trail the head; reads further back fall back to the sparse index.

> **`queue.log_count` above 1 narrows atomicity ([#169](https://github.com/callumc34/abyss/issues/169)).** A cross-shard batch is atomic only within one log.
> - A multi-key write (`MSET`, `MSETNX`, multi-key `DEL` and `UNLINK`, `RENAMENX`, `COPY`) whose keys span logs is rejected with `-CROSSSLOT`, and nothing is logged. With one log every multi-key write is accepted and atomic across shards.
> - FLUSHDB across logs is not crash-atomic: each log's Flushes are a batch of their own, so a crash can leave some logs flushed and others not. Retrying FLUSHDB completes it.
>
> The validator logs a warning at startup whenever `log_count` is above 1. Keep the default unless a measurement justifies the change and clients can live with both limits.

**Log count.** All logs live under `wal_path`, on one volume. The default of one log gives one sequential write stream and one flush per batch. Raise `log_count` only if measurements show a single flusher is the limit.
- On macOS a second log makes things worse. `F_FULLFSYNC` flushes the whole drive cache, so two logs' flushes serialise and each write waits for both.
- Measured on an Apple SSD at `power_loss`: one log held p99 under 10 ms to 30K writes/s. Two logs went over 13 ms at every rate.
- On Linux, concurrent `fdatasync` calls on separate files can be merged by the block layer, so more logs may help. That is unmeasured until an authoritative run on Linux hardware.

### Hot sizing

**Stubs.** An evicted key leaves a stub (its type, absolute TTL and latest seq) so `EXISTS`, `TYPE` and `DEL`'s count need no cold load. Each costs about 80 bytes plus the key, inside `hot.max_memory_bytes`.
- `hot.stub_memory_fraction` (default 0.02, at most 0.5) caps the stub count at `max_memory_bytes` × fraction ÷ 80, split evenly across shards: about 1.07 M stubs at the 4 GiB default. `0` keeps no stubs.
- Past the cap the least recently written stub is dropped; `abyss_hot_stub_drops_total` counts drops and `abyss_hot_stub_entries` the stubs held. Dropping one never loses data: those replies load the key from cold instead.
- Raise the fraction when the keyspace far exceeds what hot holds and existence checks on evicted keys are common.

**Memory backpressure.** Hot evicts only keys cold has drained, so it can pass `hot.max_memory_bytes` by what cold has not yet absorbed. `hot.backpressure_ratio` (default 1.25, 1 to 10) bounds that, per shard: each shard's budget is `hot.max_memory_bytes` ÷ `hot.shard_count`, and once a shard is over its budget × the ratio, writes to it that grow memory wait for cold, then fail with `-OOM` (see [failure-modes.md](failure-modes.md#hot-memory-over-its-limit)).
- Under skewed load one hot shard can reject writes while total hot memory is under `hot.max_memory_bytes`. Keys that share a hash tag share a shard.
- Leave room in the pod's memory limit for `max_memory_bytes` × the ratio, on top of the compaction buffer (`cold_consumer.buffer_high_water_bytes`) and the WAL memory above.
- Recovery replay stays within the same per-shard limit, apart from at most one scan batch's growth, and fails rather than pass twice the limit on any shard.

**Cache fills.** `hot.fill_max_fraction` (default 0.0625, in (0, 1]) is the largest key a read miss may fill into hot, as a share of a shard's budget. With the defaults a shard's budget is 64 MiB, so no fill is over 4 MiB.

### External Profile

Not implemented: the server refuses to start with any `profile` but `embedded`, and the parser accepts none of an external backend's settings (endpoints, brokers, topics, cluster membership).

The plan is for the queue and the cold store to delegate to external systems, such as a Kafka or NATS JetStream log and a KVRocks cold store, with shard ownership spread across pods ([ADP-008](../design/proposals/008-horizontal-scaling.md)). The hot store stays in-process, because the sequencer decides each write against it under the shard lock; whether an external hot tier survives at all is an open decision ([#187](https://github.com/callumc34/abyss/issues/187)).
