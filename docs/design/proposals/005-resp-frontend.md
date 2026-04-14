# ADP-005: RESP Frontend

**Status:** Accepted
**Created:** 2026-04-09

## Context

Abyss exposes a TCP listener implementing the Redis wire protocol (RESP2). Any standard Redis client library connects without modification — no custom SDKs, no protocol extensions. The frontend is responsible for parsing commands, classifying them, and routing them to the appropriate subsystem.

## Design

### Wire Protocol

RESP2 (Redis Serialisation Protocol version 2). This is the protocol spoken by every Redis client library in every language. Abyss does not implement RESP3 in Phase 1.

### Command Classification

Every command is classified into one of three categories:

| Class | Routing | Examples |
|-------|---------|---------|
| Read | Hot → Buffer → Cold | `GET`, `SMEMBERS`, `ZRANGEBYSCORE`, `EXISTS`, `TTL` |
| Write | Append to Queue | `SET`, `DEL`, `SADD`, `ZADD`, `EXPIRE` |
| Admin | Handled by Abyss directly | `PING`, `INFO`, `DBSIZE`, `CLUSTER SLOTS` |

Unknown commands default to **write** classification. This is the safe default — an unknown command that modifies state will be captured in the queue. An unknown read command classified as a write will fail (the stores won't know how to execute it), but data integrity is preserved.

### Multi-Key Commands

Commands like `MGET` and `MSET` that span multiple keys may target different shards.

**Single-pod (Phase 1):** All keys are local. `MGET` fans out across hot, buffer, and cold per key and assembles the response. `MSET` decomposes into per-key queue entries. There is no cross-key atomicity guarantee — a crash mid-decomposition may persist some keys but not others. This matches DragonflyDB's behaviour under its internal sharding model and is consistent with how Redis Cluster clients handle cross-slot fan-out.

**Multi-pod (Phase 2+):** If keys in a multi-key command span pods, Abyss responds with `CROSSSLOT` error, matching Redis Cluster semantics. Clients that support Redis Cluster handle this by fanning out per-slot and assembling results client-side. Users can use hash tags (e.g., `{user123}.name`, `{user123}.email`) to co-locate related keys on the same shard.

### Write Acknowledgement

A write is acknowledged to the client only after two things have happened:

1. The queue `Append()` completes (write is durable — for group commit, the batch fsync has completed).
2. The hot consumer has applied the write to the hot store and acknowledged it.

These happen in parallel: the hot consumer reads from the in-memory buffer while the fsync is in flight. The promise is fulfilled when both complete.

```
Write handler                    Hot consumer (in-process thread)
     │                                  │
     ├─ append to queue ───────────────▶│
     │   (blocks until fsync)           │
     ├─ register promise(seq_id)        │ (reads from buffer concurrently)
     ├─ await promise                   │
     │                                  ├─ apply to hot store
     │                                  ├─ fulfil promise(seq_id)
     │◀─────────────────────────────────┤
     ├─ return OK to client             │
```

The queue is the sole write path — there is no dual write. The hot consumer ACK is an in-process synchronisation: the write handler registers a `std::promise` keyed by the queue sequence ID, then awaits it. The hot consumer fulfils the promise after applying.

**Timeout:** The promise has a configurable timeout (default 5s). If the hot consumer fails to ACK within this window, the write handler returns a Redis error to the client. The write is still durable in the queue and will eventually be applied.

See [ADP-006](006-read-write-paths.md) for the full read and write path details.

### Connection Handling

- Async I/O via `io_uring` (Linux) with `epoll` fallback.
- Max concurrent connections: configurable, default 1024.
- Idle timeout: configurable, default 300s.

### Configuration

```yaml
resp:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300
```

## Invariants

1. Every write command is routed through the queue. There is no path that writes directly to a store.
2. Unknown commands are classified as writes.
3. A write is not acknowledged until both the queue append and the hot consumer apply are complete.
4. The promise timeout returns an error to the client but does not discard the write from the queue.
5. RESP2 protocol compliance — any standard Redis client library must be able to connect and issue commands.

## Trade-offs

**Why RESP2 and not RESP3?** RESP2 is universally supported. Every Redis client library speaks it. RESP3 adds features (client-side caching hints, attribute types) that Abyss doesn't need in Phase 1. RESP3 support can be added later without breaking existing clients.

**Why `io_uring` with `epoll` fallback?** `io_uring` offers the best async I/O performance on modern Linux kernels. `epoll` is the fallback for older kernels or environments where `io_uring` is unavailable (some container runtimes restrict it). macOS development uses `kqueue` under the hood via the abstraction layer.

**Why default unknown commands to write?** Safety. If an unknown command modifies state and we classify it as a read, the modification would bypass the queue and be lost on crash. Classifying it as a write means it goes to the queue, and the stores will return a "command not supported" error when the consumer tries to apply it. The data integrity invariant is preserved at the cost of a confusing error message for genuinely unknown read commands.

**Why no cross-key atomicity?** Redis itself doesn't guarantee atomicity for multi-key commands across slots in cluster mode. `MSET` is syntactic sugar for multiple `SET` commands. Providing atomicity would require distributed transactions, which contradicts Abyss's design principle of simplicity. Partial failures on `MSET` are explicitly acceptable — the client can retry the failed keys.
