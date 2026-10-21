#pragma once

#include <optional>
#include <span>
#include <vector>

#include "abyss/core/durability.h"
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
// An append's durability future resolves at AckDurability(). Admission
// to the durability window waits at most until `admit_by`; a deadline
// already past checks without waiting.
//
// Read position and committed offset are separate: each consumer owns its
// read cursor and passes it to Read; CommitOffset only records where it is
// safe to resume and, for retention, how far the WAL may be reclaimed.
//
// Durable ends are exclusive: seqs below DurableEnd(shard, d) are durable
// at d; 0 means none is.
class Queue {
 public:
  Queue() = default;
  virtual ~Queue() = default;
  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;
  Queue(Queue&&) = delete;
  Queue& operator=(Queue&&) = delete;

  virtual Result<queue::PendingAppend> BeginAppend(ShardId shard, QueueEntry entry,
                                                   SteadyTime admit_by) = 0;
  virtual Result<queue::PendingBatchAppend> BeginAppendBatch(ShardId shard,
                                                             std::span<const QueueEntry> entries,
                                                             SteadyTime admit_by) = 0;

  virtual Result<queue::AppendResult> Append(ShardId shard, QueueEntry entry,
                                             SteadyTime admit_by) = 0;
  virtual Result<queue::AppendBatchResult> AppendBatch(ShardId shard,
                                                       std::span<const QueueEntry> entries,
                                                       SteadyTime admit_by) = 0;

  // Up to `max_count` contiguous entries with seq >= `from_seq` that are
  // durable at `visible`. Waits up to `timeout` when none is yet.
  // kOutOfRange if `from_seq` < FirstSeq(shard): those were reclaimed.
  virtual Result<std::vector<QueueEntry>> Read(ShardId shard, SequenceId from_seq, size_t max_count,
                                               Duration timeout, Durability visible) = 0;

  // Records `seq` as `consumer`'s committed offset on `shard`: effective
  // in memory at once, persisted lazily. Fail-closed: kFailedPrecondition
  // unless seq < DurableEnd(shard, kPowerLoss), so no persisted offset
  // outruns the power-durable log. kInvalidArgument for a non-retention
  // consumer or a regression.
  virtual Result<void> CommitOffset(ConsumerId consumer, ShardId shard, SequenceId seq) = 0;

  // Last committed offset; nullopt if `consumer` never committed on `shard`.
  virtual Result<std::optional<SequenceId>> CommittedOffset(ConsumerId consumer, ShardId shard) = 0;

  // The class append futures resolve at, and replies are allowed to show.
  virtual Durability AckDurability() const = 0;
  // Monotonic per shard and class.
  virtual Result<SequenceId> DurableEnd(ShardId shard, Durability durability) = 0;
  // True iff `seq` is durable at `durability` within `timeout`.
  virtual Result<bool> AwaitDurable(ShardId shard, SequenceId seq, Durability durability,
                                    Duration timeout) = 0;

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
