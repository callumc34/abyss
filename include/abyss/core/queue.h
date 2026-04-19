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

  virtual Result<void> Ack(ConsumerId consumer, ShardId shard, SequenceId seq) = 0;
  virtual Result<SequenceId> OldestRetained(ShardId shard) = 0;
  virtual Result<QueueStats> Stats() = 0;
};

}  // namespace abyss::core
