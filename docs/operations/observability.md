# Observability

## Prometheus Metrics

### Latency Histograms

- `abyss_hot_op_duration_seconds{cmd="..."}` — hot store operation latency per command
- `abyss_cold_op_duration_seconds{cmd="..."}` — cold store operation latency per command
- `abyss_buffer_op_duration_seconds{cmd="..."}` — compaction buffer read latency per command
- `abyss_resp_request_duration_seconds{cmd="..."}` — end-to-end request latency per command
- `abyss_queue_append_duration_seconds` — queue append latency (includes fsync for group commit)

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

## Health Endpoints

| Endpoint | Port | Purpose |
|----------|------|---------|
| `GET /healthz` | 8080 | Liveness: process alive, RESP port bound |
| `GET /ready` | 8080 | Readiness: recovery complete, serving traffic |
| `GET /metrics` | 9090 | Prometheus scrape target |
| `GET /status` | 8080 | JSON: component stats, consumer positions, lag |

## Logging

JSON structured logs via spdlog. Key events:

- Cold consumer flush cycles (reason, batch size, latency)
- Consumer lag transitions (normal → warning → critical)
- Queue/disk space warnings
- Recovery progress (entries replayed, estimated time remaining)
- TTL expiry scan results (keys scanned, keys expired, adaptive rate changes)
- Cluster topology changes (shard assignments, MOVED redirects)

## Alerting Guidance

### Critical

- `abyss_cold_buffer_oldest_entry_age_seconds > default_eviction` — the cold consumer has fallen behind the eviction window. Data may be inaccessible between hot eviction and cold flush.
- Cold store disk usage > 95% — cold consumer will stall soon, cascading to queue growth and write failures.
- Queue WAL disk usage > 90% — writes will fail when the WAL fills.

### Warning

- `abyss_cold_buffer_oldest_entry_age_seconds > (default_eviction * 0.8)` — cold consumer is approaching the danger zone.
- `abyss_cold_flush_reason_total{reason="pressure"}` increasing — buffer memory pressure is forcing early flushes, reducing compaction efficiency.
- `abyss_cold_flush_reason_total{reason="deadline"}` dominating over `{reason="quiet"}` — keys are being written continuously without quiet windows. This may be normal for the workload, or it may indicate the quiet threshold needs tuning.
