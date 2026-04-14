# ADP-001: Queue and WAL

**Status:** Accepted
**Created:** 2026-04-09

## Context

The queue is the single source of truth in Abyss. Every write is committed to the queue before it is applied to any store. Both consumers (hot and cold) read from the queue independently and rebuild their state from it on recovery. The queue's durability and ordering guarantees are the foundation of Abyss's correctness.

The queue interface must be shard-aware from the start to support horizontal scaling in Phase 2, even though Phase 1 runs on a single pod.

## Design

### Interface

```cpp
class Queue {
 public:
  virtual ~Queue() = default;

  // Append a command to the log for a given shard.
  // This is the commit point — once this returns OK, the write is durable.
  virtual Result<SequenceId> Append(ShardId shard, RespCommand cmd) = 0;
  virtual Result<SequenceId> AppendBatch(
      ShardId shard, std::span<const RespCommand> cmds) = 0;

  // Consumer interface. Each consumer maintains its own cursor per shard.
  virtual Result<std::vector<LogEntry>> Read(
      ConsumerId consumer, ShardId shard,
      size_t max_count, Duration timeout) = 0;

  // Acknowledge processing up to a sequence ID.
  virtual Result<void> Ack(
      ConsumerId consumer, ShardId shard, SequenceId seq) = 0;

  // Queue retains entries until all consumers have acked.
  virtual Result<SequenceId> OldestRetained(ShardId shard) = 0;

  virtual Result<QueueStats> Stats() = 0;
};
```

Supporting types:

```cpp
struct LogEntry {
  SequenceId seq;
  RespCommand cmd;
  SteadyTime appended_at;
};

inline constexpr ConsumerId kHotConsumer = 0;
inline constexpr ConsumerId kColdConsumer = 1;
```

### Embedded WAL

The built-in queue implementation is an append-only WAL on the PVC.

**Segment management:** The WAL is composed of fixed-size segments per shard. Each segment is a file named by shard and base offset:

```
/data/wal/shard-00/00000000000000000000.wal
/data/wal/shard-00/00000000000000065536.wal
```

- Segment size: configurable, default 64 MiB.
- A background thread periodically deletes segments fully acknowledged by all consumers.
- Consumer offsets are persisted to a metadata file on the WAL PVC.

**Retention:** The queue retains entries until both consumers have acknowledged. Minimum retention is:

```
minimum_queue_retention = max(default_eviction, max(eviction_overrides))
```

This must fit on the WAL PVC.

### Group Commit

The embedded WAL's fsync strategy determines the trade-off between write throughput and durability.

Abyss batches all appends within a configurable window into a single fsync. Write handlers block until their batch is fsynced. The hot consumer can read from the in-memory buffer immediately, but the client promise is not fulfilled until both the fsync completes and the hot consumer applies — these happen in parallel.

```
Writer A ──append──┐
Writer B ──append──┤──▶ [batch buffer] ──fsync──▶ batch complete
Writer C ──append──┘         │
                             ├──▶ hot consumer reads from buffer concurrently
                             │
                      promise fulfilled when BOTH fsync + hot apply done
```

Three fsync policies are supported:

| Policy | Throughput | Max Data Loss on Crash | Use Case |
|--------|-----------|----------------------|----------|
| `fsync_per_write` | ~1K ops/s | 0 | Safety-critical |
| `group_commit` (default) | ~50-100K ops/s | Up to `group_commit_interval` of un-ACKed writes | Most workloads |
| `fsync_none` | ~500K+ ops/s | All un-flushed WAL data | Ephemeral data |

The key guarantee: any write the client received OK for is durable. Group commit only risks losing writes that were in the batch buffer at crash time and hadn't been fsynced or ACKed to the client yet. The client never saw OK for those, so it can retry.

### External Broker Mapping

For external queue profiles:

- **Kafka:** One partition per shard. Retention time-based (`min_retention_seconds`). Consumer offsets managed by Kafka.
- **NATS JetStream:** One subject per shard (`abyss.shard.{N}`). Durable consumers track ack position.

Since we always partition by key hash, ordering within a key is guaranteed by every broker.

### Configuration

```yaml
queue:
  backend: builtin_wal
  wal_path: /data/wal
  segment_size_bytes: 67108864        # 64 MiB
  min_retention_seconds: 86400        # 24 hours
  offset_fsync_interval_ms: 1000
  wal_fsync_policy: group_commit
  group_commit_interval_us: 1000      # 1ms batch window
  group_commit_max_bytes: 1048576     # Or flush at 1 MiB, whichever first
```

## Invariants

1. `Append` returning OK means the write is durable (for group commit: the batch containing this write has been fsynced).
2. `Read` returns entries in sequence order. No gaps, no reordering.
3. The queue retains all entries until both consumers have acknowledged them.
4. Each consumer's cursor is independent. The hot consumer and cold consumer never interfere with each other's read position.
5. Sequence IDs are monotonically increasing per shard.

## Trade-offs

**Why an append-only log instead of direct writes to stores?** A single ordered log eliminates dual-write consistency problems. Recovery is trivial: replay the log. The downside is write amplification, data is written to the WAL, then to hot (in memory), then eventually to cold (on disk). But the cold consumer's compaction buffer mitigates this by collapsing intermediate writes before they hit disk.

**Why group commit as default?** Per-write fsync limits throughput to ~1K ops/s (bounded by disk latency). Group commit batches multiple writes into a single fsync, achieving 50-100K ops/s while maintaining the guarantee that acknowledged writes are durable. The trade-off is latency: writes wait up to the batch interval (default 1ms). For most workloads this is acceptable.
