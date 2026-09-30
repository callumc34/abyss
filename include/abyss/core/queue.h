#pragma once

#include <span>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/pending_append.h"

namespace abyss::core {

struct QueueStats {
  uint64_t total_entries = 0;
  uint64_t total_bytes = 0;
  SequenceId head_seq = 0;
  SequenceId tail_seq = 0;
};

// Two-phase append (BeginAppend → Publish) is the race-free path for callers
// that register per-seq state. The one-shot Append is for fire-and-forget.
class Queue {
 public:
  Queue() = default;
  virtual ~Queue() = default;
  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;
  Queue(Queue&&) = delete;
  Queue& operator=(Queue&&) = delete;

  virtual Result<queue::PendingAppend> BeginAppend(ShardId shard, QueueEntry entry) = 0;
  virtual Result<queue::PendingBatchAppend> BeginAppendBatch(
      ShardId shard, std::span<const QueueEntry> entries) = 0;

  virtual Result<queue::AppendResult> Append(ShardId shard, QueueEntry entry) = 0;
  virtual Result<queue::AppendBatchResult> AppendBatch(ShardId shard,
                                                       std::span<const QueueEntry> entries) = 0;

  virtual Result<std::vector<QueueEntry>> Read(ConsumerId consumer, ShardId shard, size_t max_count,
                                               Duration timeout) = 0;

  // Ack a consumer's processed offset. For a RETENTION consumer this is
  // fail-closed: it returns kFailedPrecondition if seq > DurableSeq(shard), so
  // a persisted offset can never outrun the durable WAL tail. Volatile-consumer
  // Ack is unaffected.
  virtual Result<void> Ack(ConsumerId consumer, ShardId shard, SequenceId seq) = 0;

  // Highest seq on `shard` whose group-commit fsync has completed; 0 = none
  // durable. Monotonic per shard. The floor every retention Ack is clamped to.
  virtual Result<SequenceId> DurableSeq(ShardId shard) = 0;
  // True iff DurableSeq(shard) >= seq within `timeout`.
  virtual Result<bool> AwaitDurable(ShardId shard, SequenceId seq, Duration timeout) = 0;

  virtual Result<SequenceId> OldestRetained(ShardId shard) = 0;
  virtual Result<SequenceId> TailSeq(ShardId shard) = 0;
  // Last ack offset for `consumer` on `shard`; 0 if never acked. Persisted for
  // retention consumers; in-memory-only for volatile consumers.
  virtual Result<SequenceId> AckOffset(ConsumerId consumer, ShardId shard) = 0;
  virtual Result<QueueStats> Stats() = 0;
};

}  // namespace abyss::core
