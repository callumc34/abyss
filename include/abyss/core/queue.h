#pragma once

#include <optional>
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
  // Highest next-seq-to-assign across shards.
  SequenceId head_seq = 0;
  // Lowest first retained seq across shards.
  SequenceId first_seq = 0;
};

// Two-phase append (BeginAppend → Publish) is the race-free path for callers
// that register per-seq state. The one-shot Append is for fire-and-forget.
//
// Read position and committed offset are separate: each consumer owns its
// read cursor and passes it to Read; CommitOffset only records where it is
// safe to resume and, for retention, how far the WAL may be reclaimed.
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

  // Up to `max_count` contiguous published entries with seq >= `from_seq`.
  // Waits up to `timeout` for a publish when `from_seq` is at the head.
  // kOutOfRange if `from_seq` < FirstSeq(shard): those were reclaimed.
  virtual Result<std::vector<QueueEntry>> Read(ShardId shard, SequenceId from_seq, size_t max_count,
                                               Duration timeout) = 0;

  // Records `seq` as `consumer`'s committed offset on `shard`: effective
  // in memory at once, persisted lazily. Fail-closed: kFailedPrecondition
  // if seq > DurableSeq(shard), so a committed offset never outruns the
  // durable WAL tail. kInvalidArgument for a non-retention consumer or a
  // regression.
  virtual Result<void> CommitOffset(ConsumerId consumer, ShardId shard, SequenceId seq) = 0;

  // Last committed offset; nullopt if `consumer` never committed on `shard`.
  virtual Result<std::optional<SequenceId>> CommittedOffset(ConsumerId consumer, ShardId shard) = 0;

  // Highest seq on `shard` whose group-commit fsync has completed; 0 = none
  // durable. Monotonic per shard. The ceiling CommitOffset is gated on.
  virtual Result<SequenceId> DurableSeq(ShardId shard) = 0;
  // True iff DurableSeq(shard) >= seq within `timeout`.
  virtual Result<bool> AwaitDurable(ShardId shard, SequenceId seq, Duration timeout) = 0;

  // Lowest seq still readable on `shard`.
  virtual Result<SequenceId> FirstSeq(ShardId shard) = 0;
  // Min persisted committed offset across retention consumers (the retention
  // floor); FirstSeq when any retention consumer has none persisted.
  virtual Result<SequenceId> OldestRetained(ShardId shard) = 0;
  // Highest assigned seq on `shard`; 0 when empty.
  virtual Result<SequenceId> TailSeq(ShardId shard) = 0;
  virtual Result<QueueStats> Stats() = 0;
};

}  // namespace abyss::core
