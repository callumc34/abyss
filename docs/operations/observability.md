# Observability

## Prometheus Metrics

### Latency Histograms

- `abyss_hot_op_duration_seconds{cmd="..."}` — hot store operation latency per command
- `abyss_cold_op_duration_seconds{cmd="..."}` — cold store operation latency per command
- `abyss_buffer_op_duration_seconds{cmd="..."}` — compaction buffer read latency per command
- `abyss_resp_request_duration_seconds{cmd="..."}` — end-to-end request latency per command
- `abyss_wal_flush_duration_seconds` — duration of one WAL group-commit flush (fdatasync on Linux, `F_FULLFSYNC` on macOS, `FlushFileBuffers` on Windows). This is the device floor that `power_loss` acknowledgements and cold persistence wait on. It includes the walk that advances each shard's durable end after the sync. Segment preparation and offset persists are not included. Flushes per write is `rate(abyss_wal_flush_duration_seconds_count[1m]) / rate(abyss_queue_appended_total[1m])`. Near 1.0 under concurrent load, flushes are not batching.
- `abyss_queue_offset_persist_duration_seconds` — duration of one durable persist of committed consumer offsets. The rate of persists (`_count`) shows how much flush capacity offset bookkeeping consumes.
- `abyss_wal_flush_batch_entries` — WAL entries covered by one flush that covers at least one entry; a rising value under load shows batching is absorbing concurrency. A failed flush terminates the process, so both histograms record successful flushes only.
- `abyss_wal_fill_wait_seconds` — time an append waited for earlier reservations in its log to be filled before it could be acknowledged. Recorded only when the wait outlasted a short spin (about 2 µs), so it counts the waits behind a large value or a preempted filler (the head-of-line effect the blob lane, #162, removes), not a neighbour mid-copy.

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
- `abyss_wal_backpressure_waits_total` — appends that waited for a WAL flush because the durability window was full
- `abyss_wal_backpressure_rejections_total` — appends rejected after waiting `engine.write_timeout` for the durability window
- `abyss_queue_offset_persist_failures_total` — committed-offset checkpoint writes that failed; retried on the next round
- `abyss_queue_read_out_of_range_total` — queue reads below the first retained entry
- `abyss_wal_spare_waits_total` — appends that found no prepared segment ready and waited for one (a full disk, or the preparer behind)
- `abyss_wal_segment_prepare_failures_total` — attempts to prepare a spare segment that failed and will be retried. A rising rate is the direct signal of a full or failing WAL volume, ahead of appends waiting.
- `abyss_wal_segments_grown_total` — segments created by zero-filling rather than recycled. It rises during warm-up and whenever retention pins more segments than the free pool holds.
- `abyss_wal_scan_bytes_total` — log bytes walked by recovery scans. A hot and cold rebuild should read about the retained log once.
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
- `abyss_queue_disk_bytes` — queue WAL disk usage: retained, active, spare and free-pool segments
- `abyss_wal_unflushed_bytes` — WAL bytes published but not yet power-durable, across shards. Bounded by `queue.durability_window_bytes`; under `process_crash` this is what a power loss would lose.
- `abyss_wal_durability_lag_seconds` — age bound of the oldest WAL entry not yet power-durable, worst log. Bounded by `queue.durability_window_ms`.
- Both are set each snapshot interval by the metrics snapshotter, from the current time on its own thread, so they keep rising while a flush is stalled. They read 0 until recovery has finished.
- `abyss_wal_spare_segments` — prepared segments ready for the next rotation, summed over logs. Two per log is healthy; zero means appends will wait at the next rotation.
- `abyss_wal_free_segments` — reclaimed segments waiting to be recycled, summed over logs (at most two per log).
- `abyss_wal_index_bytes` — memory held by the per-shard sparse indexes (about 0.025% of the retained WAL).
- `abyss_wal_ring_bytes` — memory held by the per-shard offset rings, allocated at start: 16 bytes × `queue.ring_entries` × shard count. The `WAL opened` log line states the same figure.
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
  "schema_version": 3,
  "abyss":   { "version": "0.1.0", "build": { "commit": "abc1234", "date": "2026-05-03T12:34:56Z" } },
  "server":  { "node_id": "...", "started_at_unix_ms": 1714742400000,
               "uptime_seconds": 3600, "process_id": 12345,
               "ready": true, "loading": false, "shutting_down": false,
               "mode": "standalone", "role": "master" },
  "config":  { "profile": "embedded", "shard_count": 64,
               "durability": "process_crash", "default_eviction_seconds": 86400 },
  "endpoints": {
    "resp":    { "bind": "0.0.0.0", "port": 6379 },
    "admin":   { "bind": "0.0.0.0", "port": 8080, "enabled": true },
    "metrics": { "bind": "0.0.0.0", "port": 9090, "enabled": true }
  },
  "queue": { "backend": "builtin_wal", "head_seq": 0, "first_seq": 0,
             "total_entries": 0, "total_bytes": 0,
             "unflushed_bytes": 0, "durability_lag_ms": 0 },
  "hot":   { "backend": "builtin_hashmap", "key_count": 0, "memory_bytes": 0 },
  "cold":  { "backend": "builtin_rocksdb", "key_count": 0,
             "buffer": { "entries": 0, "bytes": 0 } },
  "consumers": {
    "hot":      { "highest_settled_seq_min": 0, "highest_settled_seq_max": 0 },
    "cold":     { "last_commit_seq_min": 0, "last_commit_seq_max": 0 },
    "resolver": { "last_commit_seq_min": 0, "last_commit_seq_max": 0,
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

- `schema_version` — bumps only when an invariant above is broken. Today: `3`.
  - Version 3 replaced `config.fsync_policy` with `config.durability` (`process_crash` or `power_loss`).
  - Version 2 renamed `queue.tail_seq` to `queue.first_seq`, which now reports the lowest readable sequence across shards. It also renamed `consumers.{cold,resolver}.last_ack_seq_{min,max}` to `last_commit_seq_{min,max}`, following the queue's move from acknowledgements to committed offsets.
- `queue.unflushed_bytes` / `queue.durability_lag_ms` — the same values as `abyss_wal_unflushed_bytes` and `abyss_wal_durability_lag_seconds`, at snapshot time.
- `abyss.build.commit` / `abyss.build.date` — captured at configure time. `unknown` outside a git checkout.
- `server.mode` — `"standalone"` or `"cluster"`. Phase 1 always emits `"standalone"`.
- `server.role` — `"master"` or `"replica"`. Phase 1 always emits `"master"`.
- `consumers.*.{seq}_min` / `_max` — per-shard min and max of the corresponding sequence positions. Equal values mean uniform progress across shards; divergence indicates shard skew.
- `lag.*_max_entries` — worst-case lag across shards (newest assigned seq − consumer seq), in queue entries.
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
- `increase(abyss_wal_backpressure_rejections_total[5m]) > 0` — writes are failing because the WAL device cannot flush fast enough to keep acknowledged-but-not-power-durable data inside the durability window. Check `abyss_wal_flush_duration_seconds` and the volume's IOPS limit.

### Warning
- `abyss_wal_spare_segments == 0` for more than a minute, or `increase(abyss_wal_spare_waits_total[5m]) > 0` — appends are waiting for a WAL segment. Check free space on the WAL volume and, during warm-up, whether the write rate exceeds about half the volume's bandwidth (`abyss_wal_segments_grown_total` rising).

- `abyss_cold_buffer_oldest_entry_age_seconds > (default_eviction * 0.8)` — cold consumer is approaching the danger zone.
- `abyss_wal_durability_lag_seconds > 0.5 * durability_window_ms / 1000` — the WAL is flushing slower than it should. Under `process_crash` the power-loss exposure is growing, and writes will start to wait when the lag reaches the window.
- `abyss_cold_flush_reason_total{reason="pressure"}` increasing — buffer memory pressure is forcing early flushes, reducing compaction efficiency.
- `abyss_cold_flush_reason_total{reason="deadline"}` dominating over `{reason="quiet"}` — keys are being written continuously without quiet windows. This may be normal for the workload, or it may indicate the quiet threshold needs tuning.
- `abyss_cold_ttl_disk_pressure_active == 1` — the cold-store filesystem has crossed `disk_pressure_threshold` and the TTL scanner has switched to maximum aggression. Sustained pressure means provisioning is underspec'd or the cold consumer is producing more than active expiry can reclaim.
- `rate(abyss_cold_ttl_deleted_total[5m]) == 0 AND abyss_cold_keys > 0` — scanner is alive but reclaiming nothing. Either the workload genuinely has no expiring keys (benign) or the scanner is failing silently (investigate logs at component `abyss.cold.ttl_scanner`).
- `increase(abyss_queue_offset_persist_failures_total[5m]) > 0` — the committed-offset checkpoint could not be written. Persisted offsets stay where they were, so the next restart replays further and retention cannot advance; nothing acknowledged is lost. Check the WAL volume for space and I/O errors.
- A pod restarting repeatedly (Kubernetes `CrashLoopBackOff`) after a CRITICAL `fatal invariant breach; terminating` log line means an unrecoverable invariant breach. The process aborts deliberately, so no metric survives to be scraped; the log line names the cause. A retention consumer reading below the first retained WAL entry is one such breach: entries above its persisted offset were reclaimed, so the process stops instead of skipping data, and the line names the consumer, shard and positions. `abyss_queue_read_out_of_range_total` counts the out-of-range reads a live process survives, which are hot-consumer resets to the oldest entry, expected after a restart.
- `increase(abyss_queue_reaper_failures_total[15m]) > 0` — the segment reaper could not delete a sealed segment it was entitled to reclaim. One failure is usually a transient filesystem error and the reaper retries; a sustained rate means WAL disk will grow without bound even though every retention consumer's persisted committed offset is past those segments. Check filesystem permissions and free inodes on the WAL volume. Pair this with the age gauge below — failures alone do not say how much reclamation is being lost.
- `abyss_queue_oldest_eligible_unreaped_age_seconds > 3 * min_retention_seconds` — a segment has been eligible for reclamation for far longer than the retention floor and is still on disk. This is the symptom that matters for disk exhaustion; the failure counter above is one cause. Reclamation is oldest-first per log, so if this climbs while the failure counter is flat, an earlier segment is pinned: some shard on that log has a retention consumer (cold or the resolver) whose persisted offset is not moving. Find it from per-shard consumer lag and the poison counters before restarting anything; a restart does not unpin it.
- `increase(abyss_cold_unsupported_op_total[1h]) > 0` — the log contains write entries this build has no parser for. Live traffic cannot produce these (see [failure-modes.md](failure-modes.md) §poison quarantine), so a non-zero value means the data directory carries entries from a binary with a wider command surface: a downgrade, a mixed-version rollout, or a restore from a newer node. Those writes are absent from both tiers. Treat as a correctness investigation, not a capacity one.
