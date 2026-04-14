# Failure Modes

## Failure Scenarios

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Pod crash (embedded) | Hot store lost. WAL + cold store intact on PVC. | Pod restarts. Queue replay rebuilds hot store. Cold consumer catches up from its last ack point. |
| Pod crash (external) | In-process orchestrator lost. External stores (Redis, Kafka, KVRocks) retain data. | Pod restarts, resumes queue consumption. |
| Cold store PVC full | Cold consumer's `apply_batch()` fails. Cold consumer stalls. Queue grows. Eventually queue fills and writes fail. | Provision more cold storage. |
| Queue WAL PVC full | Queue `Append()` fails. Writes return Redis errors to clients. | Provision more WAL storage or speed up cold consumer (allows segment cleanup). |
| Cold consumer lag > eviction | Reads may miss hot (evicted) and cold (not yet flushed). Data is in the queue/buffer. Buffer serves reads during the gap. | Cold consumer catches up. No data loss — buffer reads bridge the gap. |
| Hot store memory pressure | LRU evicts keys before their eviction deadline. Reads for evicted keys fall through to buffer then cold. | Provision more hot store memory or reduce eviction durations. Data is safe in queue and eventually in cold. |

## Backpressure Cascade

The backpressure model is intentionally simple and cascading:

```
Cold store disk full
  → cold consumer stalls
    → queue entries accumulate (not acked by cold consumer)
      → queue WAL grows
        → WAL PVC fills
          → Append() fails
            → writes return errors to clients
```

There is no magic. Each stage is visible in metrics. Operators must provision resources or tune configuration to resolve the cascade.

## Write Failures

A write that fails at the queue level (disk full, I/O error) returns a Redis error to the client. The write was never committed to the queue, so no state is inconsistent.

A write that succeeds at the queue level but whose hot consumer promise times out returns a Redis error to the client. The write IS durable in the queue and WILL be applied eventually. The client received an error, so it may retry — the retry will be a duplicate write, which is safe because last-write-wins is the default semantic.

## Recovery After Crash

See [ADP-007](../design/proposals/007-recovery.md) for the full recovery design. Key points:

1. Recovery is pure queue replay. No external coordination.
2. During recovery, the RESP port returns `LOADING` errors.
3. The readiness probe (`/ready`) returns 503 until recovery is complete.
4. Recovery time is bounded by queue depth and replay batch sizes.

## Cold Consumer Stall

If the cold consumer stalls (cold store I/O errors, bugs, resource exhaustion):

- Hot store continues serving reads normally.
- The compaction buffer continues serving buffer-hit reads.
- The queue grows because the cold consumer isn't acking entries.
- Write throughput is unaffected until the queue fills.
- **Metric to watch:** `abyss_cold_buffer_oldest_entry_age_seconds` and `abyss_cold_consumer_lag_entries`.

The cold consumer stall is the most insidious failure because it has no immediate client-visible impact. Writes succeed, reads work (from hot + buffer). The danger is delayed: if the buffer eventually exceeds its high-water mark, it switches to aggressive flush mode. If the stall persists long enough, the queue fills and writes fail.

## Hot Consumer Stall

If the hot consumer stalls:

- Write promises time out. Clients receive Redis errors.
- Writes are still durable in the queue and will be applied when the consumer recovers.
- This is self-regulating: as promises time out, clients back off, reducing write pressure.
- **Metric to watch:** `abyss_hot_consumer_lag_entries`.

Hot consumer stalls have immediate client impact (write errors), which makes them easy to detect and respond to.
