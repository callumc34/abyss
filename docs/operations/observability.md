# Observability

## Prometheus Metrics

### Latency Histograms

- `abyss_hot_op_duration_seconds{cmd="..."}` — hot store operation latency per command
- `abyss_cold_op_duration_seconds{cmd="..."}` — cold store operation latency per command
- `abyss_buffer_op_duration_seconds{cmd="..."}` — compaction buffer read latency per command
- `abyss_resp_request_duration_seconds{cmd="..."}` — end-to-end request latency per command
- `abyss_queue_append_duration_seconds` — queue append latency (includes fsync for group commit)

### RESP Frontend

- `abyss_resp_requests_total{cmd="...",status="ok|error|loading|unknown|arity|noproto"}` — request outcomes per command
- `abyss_resp_parse_errors_total` — malformed RESP inputs rejected at the parser
- `abyss_resp_protocol_version_total{proto="2|3"}` — HELLO handshakes by negotiated protocol version

### Consumer Lag

- `abyss_hot_consumer_lag_entries` — entries between hot consumer position and queue head
- `abyss_cold_consumer_lag_entries` — entries between cold consumer position and queue head
- `abyss_cold_buffer_oldest_entry_age_seconds` — age of the oldest un-flushed buffer entry. **This is the most critical metric.** It directly indicates cold gap risk.
- `abyss_hot_consumer_seq` — hot consumer's current sequence position
- `abyss_cold_consumer_seq` — cold consumer's current sequence position (reflects latest entry read into buffer, not latest entry flushed to cold)

### Counters

- `abyss_hits_total{tier="hot|buffer|cold"}` — read hits by tier
- `abyss_misses_total` — read misses (key not found in any tier)
- `abyss_queue_appended_total` — total entries appended to queue
- `abyss_cold_flush_total{status="success|failure"}` — cold consumer flush operations
- `abyss_cold_flush_reason_total{reason="quiet|deadline|pressure"}` — flush trigger reason
- `abyss_cold_flush_batch_size` (histogram) — number of keys per flush batch
- `abyss_ttl_expired_total{tier="hot|cold"}` — TTL expirations by tier
- `abyss_evicted_total` — keys evicted from hot (moved to cold-only)
- `abyss_promotions_total` — cold hits promoted back to hot

### Cold-store active TTL expiry

The cold-store backend owns an internal sweeper thread that proactively
finds and deletes expired keys (see [ADP-003](../design/proposals/003-cold-store.md)
§TTL Expiry). The scanner self-throttles based on observed expired ratio,
disk pressure, and a CPU budget cap.

- `abyss_cold_ttl_samples_total{subject="string|collection"}` — random samples drawn per cycle
- `abyss_cold_ttl_with_ttl_total{subject="string|collection"}` — sampled records that carried a TTL flag
- `abyss_cold_ttl_expired_total{subject="string|collection"}` — sampled records past their TTL at sample time
- `abyss_cold_ttl_deleted_total{subject="string|collection"}` — records the scanner successfully deleted
- `abyss_cold_ttl_conflicts_total{subject="string|collection"}` — CAS commits aborted by a concurrent writer

Gauges:

- `abyss_cold_ttl_interval_ms` — current sleep interval between scanner ticks
- `abyss_cold_ttl_sample_size` — current per-tick sample size
- `abyss_cold_ttl_rate_multiplier` — adaptive rate factor
- `abyss_cold_ttl_cpu_fraction` — EWMA of scanner-thread CPU as a fraction of wall time
- `abyss_cold_ttl_disk_pressure_fraction` — fraction of the cold-store filesystem in use
- `abyss_cold_ttl_disk_pressure_active` — `1` when the scanner is in disk-pressure mode, `0` otherwise

### Gauges

- `abyss_hot_memory_bytes` — hot store memory usage
- `abyss_hot_keys` — number of keys in hot store
- `abyss_cold_disk_bytes` — cold store disk usage
- `abyss_cold_keys` — number of keys in cold store
- `abyss_queue_depth` — number of entries in queue
- `abyss_queue_disk_bytes` — queue WAL disk usage
- `abyss_cold_buffer_entries` — number of keys in compaction buffer
- `abyss_cold_buffer_bytes` — estimated memory usage of compaction buffer

### TCP server

- `abyss_net_connections_active` (gauge) — currently open TCP connections
- `abyss_net_connections_accepted_total` (counter) — connections accepted since startup
- `abyss_net_connections_closed_total{reason}` (counter) — connections closed; `reason` ∈ `client | idle | oversize | backpressure | server_shutdown`
- `abyss_net_connections_rejected_total{reason}` (counter) — accepts rejected before becoming a connection; `reason` ∈ `max_connections | bind_family`
- `abyss_net_bytes_in_total` / `abyss_net_bytes_out_total` (counters) — wire-level traffic
- `abyss_net_read_buffer_high_water_bytes` (gauge) — largest read-buffer size observed across active connections
- `abyss_net_backpressure_active` (gauge) — connections currently paused for write back-pressure
- `abyss_net_backpressure_entered_total` / `abyss_net_backpressure_exited_total` (counters) — back-pressure transitions

## Health Endpoints

Two HTTP listeners run alongside the RESP server. Both default to `0.0.0.0` and can be disabled via `admin.enabled` / `metrics.enabled` in config. Pass `0` for either port to request an OS-assigned ephemeral port (the bound port is reported on the `--ready-fd` JSON line and in `/status`).

| Endpoint | Port | Purpose |
|----------|------|---------|
| `GET /healthz` | 8080 | Liveness probe |
| `GET /ready` | 8080 | Readiness probe |
| `GET /status` | 8080 | Operational state snapshot (JSON) |
| `GET /metrics` | 9090 | Prometheus scrape target |

All endpoints accept `GET` and `HEAD` only. `POST`/`PUT`/`DELETE` return `405 Method Not Allowed` with an `Allow: GET, HEAD` header. Unknown paths on either port return `404`. Connections are per-request (no keep-alive); each handler completes within a hard read/write deadline so a slow client cannot pin a worker thread.

### `/healthz` — Liveness

Always returns `200 OK` with body `ok\n` once the listener is up. The fact that the process generated a response is itself the liveness signal — Kubernetes SIG-Node convention. Failing this probe means the kubelet should restart the container, so it is deliberately permissive: TCP-bound state, recovery progress, and consumer health belong in `/ready`, not here.

### `/ready` — Readiness

Returns `200 OK` only when **all** of the following hold:

- The RESP TCP listener is bound and accepting traffic.
- Recovery is complete (queue is no longer in `IsRecovering` state and every hot consumer has caught up to its ready watermark).
- The server is not in `shutting_down` state.

Otherwise returns `503 Service Unavailable`. The body is plain text key:value pairs naming each condition's truth value, so an operator can see at a glance which check failed:

```
tcp_bound: true
recovery_complete: false
not_shutting_down: true
```

Returning `503` during shutdown grace removes the pod from Kubernetes Service endpoints before the data plane stops, allowing in-flight clients to drain via the configured `net.shutdown_grace_seconds`.

### `/metrics` — Prometheus scrape

Returns the Prometheus exposition format payload (`text/plain; version=0.0.4; charset=utf-8`) from the process-wide registry. Empty body when the registry is empty or `metrics.enabled = false` — Prometheus tolerates an empty body. The endpoint always returns `200`.

### `/status` — Operational state snapshot

Returns a JSON document with the live operational state of the process. Content-Type is `application/json; charset=utf-8`. The schema is **foundational** — extended only by addition, never by rename or type change.

```json
{
  "schema_version": 1,
  "abyss":   { "version": "0.1.0", "build": { "commit": "abc1234", "date": "2026-05-03T12:34:56Z" } },
  "server":  { "node_id": "...", "started_at_unix_ms": 1714742400000,
               "uptime_seconds": 3600, "process_id": 12345,
               "ready": true, "loading": false, "shutting_down": false,
               "mode": "standalone", "role": "master" },
  "config":  { "profile": "embedded", "shard_count": 64,
               "fsync_policy": "group_commit", "default_eviction_seconds": 86400 },
  "endpoints": {
    "resp":    { "bind": "0.0.0.0", "port": 6379 },
    "admin":   { "bind": "0.0.0.0", "port": 8080, "enabled": true },
    "metrics": { "bind": "0.0.0.0", "port": 9090, "enabled": true }
  },
  "queue": { "backend": "builtin_wal", "head_seq": 0, "tail_seq": 0,
             "total_entries": 0, "total_bytes": 0 },
  "hot":   { "backend": "builtin_hashmap", "key_count": 0, "memory_bytes": 0 },
  "cold":  { "backend": "builtin_rocksdb", "key_count": 0,
             "buffer": { "entries": 0, "bytes": 0 } },
  "consumers": {
    "hot":      { "highest_settled_seq_min": 0, "highest_settled_seq_max": 0 },
    "cold":     { "last_ack_seq_min": 0, "last_ack_seq_max": 0 },
    "resolver": { "last_ack_seq_min": 0, "last_ack_seq_max": 0,
                  "cache_entries": 0, "cache_bytes": 0 }
  },
  "lag":     { "hot_max_entries": 0, "cold_max_entries": 0,
               "resolver_max_entries": 0 },
  "connections": { "active": 0 },
  "cluster": null
}
```

#### Schema-stability invariants

These invariants govern any change to the `/status` payload across releases. Bumping `schema_version` is the only signal that one of them has been broken.

- **Append-only.** New fields may be added to any object. Existing fields are never renamed, retyped, or removed.
- **Reserved nulls.** Keys reserved for capabilities not yet present (e.g. `cluster` in Phase 1) appear as `null`. They are not omitted.
- **Neutral values for unset numerics.** Counters and gauges default to `0` when not yet observable; never missing.
- **Per-tier name stability.** `queue.backend`, `hot.backend`, `cold.backend` are free-form strings whose values may change with the deployment profile. Their *keys* are stable.
- **Counters belong in `/metrics`, not `/status`.** Anything that monotonically increases — flushes, decisions, accepted connections — is exposed as a Prometheus counter, not a JSON field. Trend and rate observation belong on the metrics path; `/status` is a state snapshot.

#### Field semantics

- `schema_version` — bumps only when an invariant above is broken. Today: `1`.
- `abyss.build.commit` / `abyss.build.date` — captured at configure time. `unknown` outside a git checkout.
- `server.mode` — `"standalone"` or `"cluster"`. Phase 1 always emits `"standalone"`.
- `server.role` — `"master"` or `"replica"`. Phase 1 always emits `"master"`.
- `consumers.*.{seq}_min` / `_max` — per-shard min and max of the corresponding sequence positions. Equal values mean uniform progress across shards; divergence indicates shard skew.
- `lag.*_max_entries` — worst-case lag across shards (`tail_seq − consumer_seq`), in queue entries.
- `cluster` — reserved for Phase 2; populated with `slots_owned`, `peers`, `epoch` when cluster mode lands.



## Logging

JSON-Lines structured logs via spdlog, through the `abyss::log` facade (see ADP-012).

### Schema

Every record carries, at minimum:

| Field | Meaning |
|-------|---------|
| `ts` | Wall-clock timestamp, ISO 8601 UTC. |
| `level` | One of `trace`, `debug`, `info`, `warn`, `error`, `critical`. |
| `component` | The logger name (e.g. `queue.segment`, `cold.flush`). |
| `msg` | A short, static, low-cardinality phrase. |

Dynamic values are emitted as additional top-level JSON fields supplied at the call site. `msg` itself is always static — dynamic values belong in fields, never interpolated into `msg`. This keeps `msg` useful as a filter term in log aggregation.

### Level guidance

- **`info` and above** — state transitions and errors only. Recovery milestones, consumer lag crossings, flush cycles, component start/stop, disk warnings, TTL scan summaries, cluster topology changes.
- **`debug`** — per-request or per-key tracing. Compiled into the binary but filtered out at runtime unless the debug level is set.
- **`trace`** — highest-volume instrumentation; use sparingly.

### Key events at INFO and above

- Cold consumer flush cycles (reason, batch size, latency).
- Consumer lag transitions (normal → warning → critical).
- Queue and disk space warnings.
- Recovery progress (entries replayed, estimated time remaining).
- TTL expiry scan results (keys scanned, keys expired, adaptive rate changes).
- Cluster topology changes (shard assignments, MOVED redirects).

### No raw keys

Log records **must not** contain raw Redis keys or values at any level. Keys are PII-equivalent. When a record needs to identify a key, emit `key_hash` — produced via `abyss::log::KeyHash(key)` — instead. The helper is part of the facade so the rule is trivial to follow at the call site.

## Metric naming and cardinality

### Catalogue

Every metric name is declared in `include/abyss/metrics/names.h` as a `constexpr` descriptor. The metrics registry API accepts only descriptors — there is no overload that takes a free-form name string. Adding a metric is therefore a change to the catalogue, which forces deliberate review of name, help, labels, and cardinality.

### Label whitelist

The only permitted label keys are:

| Key | Meaning | Value space |
|-----|---------|-------------|
| `tier` | Hot / buffer / cold tier. | `hot`, `buffer`, `cold`. |
| `shard` | Shard identifier. | Bounded by shard count (single shard in the non-sharded profile). |
| `cmd` | RESP command name. | Bounded by the server's command registry plus the `unknown` sentinel. |
| `reason` | Operation trigger reason. | Enumerated per metric. |
| `status` | Operation outcome. | Enumerated per metric. |
| `op` | Generic operation discriminator. | Reserved; enumerated per metric when used. |
| `proto` | Negotiated RESP protocol version. | `2`, `3`. |

Label values **must not** be raw Redis keys, client identifiers, IP addresses, or any other unbounded cardinality source.

## How to instrument a component

A component registers its metrics and acquires its logger in its constructor and caches both as members. The hot path then does an atomic metric update and, optionally, a level-gated structured log.

```cpp
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::example {

class FlushEngine {
 public:
  FlushEngine()
      : logger_(log::Get("cold.flush")),
        flushes_success_(metrics::Registry::Instance().Counter(
            metrics::names::kColdFlushTotal, metrics::FlushStatus::kSuccess)),
        batch_size_(metrics::Registry::Instance().Histogram(
            metrics::names::kColdFlushBatchSize)) {}

  void Flush(size_t batch_size) {
    batch_size_.Observe(static_cast<double>(batch_size));
    flushes_success_.Increment();
    ABYSS_LOG_INFO(logger_, "cold flush complete",
                   {"reason", "quiet"},
                   {"batch_size", static_cast<int64_t>(batch_size)});
  }

 private:
  log::Logger logger_;
  metrics::CounterHandle flushes_success_;
  metrics::HistogramHandle batch_size_;
};

}  // namespace abyss::example
```

## Alerting Guidance

### Critical

- `abyss_cold_buffer_oldest_entry_age_seconds > default_eviction` — the cold consumer has fallen behind the eviction window. Data may be inaccessible between hot eviction and cold flush.
- Cold store disk usage > 95% — cold consumer will stall soon, cascading to queue growth and write failures.
- Queue WAL disk usage > 90% — writes will fail when the WAL fills.

### Warning

- `abyss_cold_buffer_oldest_entry_age_seconds > (default_eviction * 0.8)` — cold consumer is approaching the danger zone.
- `abyss_cold_flush_reason_total{reason="pressure"}` increasing — buffer memory pressure is forcing early flushes, reducing compaction efficiency.
- `abyss_cold_flush_reason_total{reason="deadline"}` dominating over `{reason="quiet"}` — keys are being written continuously without quiet windows. This may be normal for the workload, or it may indicate the quiet threshold needs tuning.
- `abyss_cold_ttl_disk_pressure_active == 1` — the cold-store filesystem has crossed `disk_pressure_threshold` and the TTL scanner has switched to maximum aggression. Sustained pressure means provisioning is underspec'd or the cold consumer is producing more than active expiry can reclaim.
- `rate(abyss_cold_ttl_deleted_total[5m]) == 0 AND abyss_cold_keys > 0` — scanner is alive but reclaiming nothing. Either the workload genuinely has no expiring keys (benign) or the scanner is failing silently (investigate logs at component `abyss.cold.ttl_scanner`).
