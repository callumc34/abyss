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
profile: embedded

cluster:
  enabled: false

hot:
  backend: builtin_hashmap
  max_memory_bytes: 4294967296
  default_eviction_seconds: 86400
  eviction_tick_ms: 1000
  eviction_policy: lru
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300

cold:
  backend: builtin_rocksdb
  data_path: /data/cold
  compaction_style: level
  write_buffer_size_bytes: 67108864
  max_write_buffer_number: 4
  bloom_filter_bits_per_key: 10
  ttl_expiry:
    active_enabled: true
    sample_size: 20
    base_interval_ms: 1000
    high_threshold: 0.25
    low_threshold: 0.05
    disk_pressure_threshold: 0.9
    max_cpu_percent: 10

queue:
  backend: builtin_wal
  wal_path: /data/wal
  segment_size_bytes: 134217728
  min_retention_seconds: 86400
  offset_fsync_interval_ms: 1000      # committed-offset checkpoint cadence (10-60000)
  durability: process_crash           # process_crash | power_loss
  durability_window_bytes: 67108864   # unflushed WAL bytes across shards (1 MiB-4 GiB)
  durability_window_ms: 1000          # oldest unflushed entry per shard (10-60000)

hot_consumer:
  read_batch_size: 256
  read_timeout_ms: 100

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

recovery:
  replay_parallelism: 4
  hot_replay_batch_size: 10000
  cold_replay_batch_size: 50000

resp:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300

metrics:
  bind: 0.0.0.0
  port: 9090

admin:
  bind: 0.0.0.0
  port: 8080
```

### External Profile

```yaml
profile: external

cluster:
  enabled: true
  shard_count: 64
  hash_function: xxhash
  this_pod_shards: [0, 1, 2, 3]

hot:
  backend: redis_client
  endpoint: "dragonfly.svc:6379"
  pool_size: 32
  default_eviction_seconds: 86400

cold:
  backend: redis_client
  endpoint: "kvrocks.svc:6666"
  pool_size: 16

queue:
  backend: kafka
  brokers: "kafka.svc:9092"
  topic: "abyss-log"
  partitions_per_shard: 1
  retention_ms: 86400000
```
