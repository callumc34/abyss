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

| Endpoint | Port | Purpose |
|----------|------|---------|
| `GET /healthz` | 8080 | Liveness: process alive, RESP port bound |
| `GET /ready` | 8080 | Readiness: recovery complete, serving traffic |
| `GET /metrics` | 9090 | Prometheus scrape target |
| `GET /status` | 8080 | JSON: component stats, consumer positions, lag |

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
