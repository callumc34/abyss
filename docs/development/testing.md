# Testing

## Running Tests

```bash
# Build and run all unit tests
cmake --preset default
cmake --build build/default
ctest --preset default

# Run with address sanitizer
cmake --preset asan
cmake --build build/asan
ctest --test-dir build/asan

# Run with thread sanitizer
cmake --preset tsan
cmake --build build/tsan
ctest --test-dir build/tsan
```

## Test Infrastructure

### Mock Implementations

The `tests/support/` directory provides googlemock implementations of the core interfaces:

- `MockQueue` — mock of `core::Queue`
- `MockHotStore` — mock of `core::HotStore`
- `MockColdStore` — mock of `core::ColdStore`

These are linked via the `abyss::test_support` target. Any test binary can depend on it.

### Parameterized Interface Tests

Each interface implementation (built-in and external) is tested via parameterized test suites. The same tests run against every implementation, ensuring behavioural equivalence. Tests are parameterized by implementation factory, not by concrete type.

## Unit Tests

### Core Types
- `Result<T>` success and error paths
- `RespValue` construction and type checking
- `RespCommand` argument access

### RESP Parser
- Protocol conformance vectors (valid RESP2 messages)
- Malformed input handling (truncated, invalid type markers)
- Bulk string with various lengths including zero and large
- Array nesting

### Hot Store
- SET/GET round-trip
- Eviction timer refresh on read
- LRU eviction under memory pressure
- Absolute TTL expiry (distinct from eviction)
- Concurrent read/write under sharded locks

### Cold Store
- Batch apply and read-back
- Lazy TTL expiry on read
- Active TTL expiry (sampling, adaptive rate)

### Compaction Buffer
- Scalar last-write-wins: SET overwrites SET, SET overwrites DEL
- Set merge-accumulate: SADD/SREM interleaving produces correct net state
- Sorted set merge: ZADD/ZREM, latest score wins for duplicate members
- DEL resets all prior accumulated state
- Buffer read returns current compacted state

### Flush Strategy
- Quiet window detection: key goes quiet → flush triggers after threshold
- Deadline flush: key written continuously → flush triggers before eviction deadline
- Deadline jitter: flush times for keys with similar first_seen are spread across the jitter range
- Memory pressure: buffer exceeds high-water mark → aggressive flush mode

### Consumers
- Hot consumer applies writes in sequence order
- Hot consumer fulfils write promises after apply
- Cold consumer drains queue into compaction buffer
- Cold consumer flushes buffer entries to cold store

### Tiering Engine
- Read routing: hot hit → returns from hot, refreshes eviction
- Read routing: hot miss, buffer hit → returns from buffer, no promotion
- Read routing: hot miss, buffer miss, cold hit → returns from cold, promotes via queue
- Read routing: all miss → returns nil
- Write routing: append to queue, register promise

### Write Promise
- Promise-based write ACK lifecycle
- Timeout returns error, write remains durable

### Shard Routing
- xxHash consistency: same key always maps to same shard
- Even distribution across shard range

### Consumer Lag
- Lag detection with synthetic clock
- Warning and critical threshold transitions

## Integration Tests

- End-to-end: Redis client → Abyss → verify read-after-write
- Recovery: write, kill, restart, verify all non-expired keys available
- Cold consumer quiet window: write rapidly, stop, verify flush after threshold
- Cold consumer deadline: write continuously, verify deadline flush fires
- Cold consumer compaction: write same key 10K times, verify single cold write
- Buffer reads: verify reads hit buffer for keys evicted from hot but not yet cold
- WAL segment rotation: verify old segments cleaned up after ack
- Profile switching: same suite against embedded, external, hybrid
- Promotion: cold hit generates queue entry, hot consumer applies, key survives restart
- Multi-key fan-out: MGET across hot/buffer/cold tiers

## Performance Tests

- Sustained throughput at target ops/s for 1 hour
- Cold consumer batching efficiency under various write rates
- Recovery time vs queue depth
- Memory stability over 24 hours
- Hot-key workload: measure tail latency under lock contention

## Chaos Tests

- Kill pod mid-flush, verify recovery correctness
- Kill pod during compaction buffer flush
- Corrupt WAL segment, verify graceful skip to next valid segment
- Fill cold store PVC, verify error propagation
- Fill queue WAL PVC, verify error propagation
- Slow disk on cold store, verify lag metrics and buffer growth
- Memory pressure: push buffer past high-water, verify aggressive flush
