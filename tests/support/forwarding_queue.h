#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"

namespace abyss::testing {

// Forwards every call to `queue`, for a test to override the few it
// reorders or interleaves with.
class ForwardingQueue : public core::Queue {
 public:
  explicit ForwardingQueue(core::Queue& queue) : queue_(queue) {}

  core::Result<queue::PendingAppend> BeginAppend(core::ShardId shard, core::QueueEntry entry,
                                                 core::SteadyTime admit_by) override {
    return queue_.BeginAppend(shard, std::move(entry), admit_by);
  }
  core::Result<queue::PendingBatchAppend> BeginAppendBatch(
      core::ShardId shard, std::span<const core::QueueEntry> entries,
      core::SteadyTime admit_by) override {
    return queue_.BeginAppendBatch(shard, entries, admit_by);
  }
  core::Result<queue::AppendResult> Append(core::ShardId shard, core::QueueEntry entry,
                                           core::SteadyTime admit_by) override {
    return queue_.Append(shard, std::move(entry), admit_by);
  }
  core::Result<queue::AppendBatchResult> AppendBatch(core::ShardId shard,
                                                     std::span<const core::QueueEntry> entries,
                                                     core::SteadyTime admit_by) override {
    return queue_.AppendBatch(shard, entries, admit_by);
  }
  bool Admissible(core::ShardId shard) override { return queue_.Admissible(shard); }
  std::optional<core::Result<void>> WhenReady(core::QueueWait wait, core::ShardId shard,
                                              core::Waiter& waiter, bool first) override {
    return queue_.WhenReady(wait, shard, waiter, first);
  }
  bool CancelWait(core::QueueWait wait, core::ShardId shard,
                  core::Waiter& waiter) noexcept override {
    return queue_.CancelWait(wait, shard, waiter);
  }
  void FinishWait(core::QueueWait wait, core::ShardId shard,
                  core::Waiter& waiter) noexcept override {
    queue_.FinishWait(wait, shard, waiter);
  }
  core::Error AdmissionTimedOut(core::ShardId shard) override {
    return queue_.AdmissionTimedOut(shard);
  }
  core::Result<queue::Reservation> Reserve(std::span<const queue::ShardEntries> parts) override {
    return queue_.Reserve(parts);
  }
  core::Result<queue::Reservation> ReserveFlush(
      std::span<const queue::ShardEntries> parts) override {
    return queue_.ReserveFlush(parts);
  }
  uint32_t LogOf(core::ShardId shard) const override { return queue_.LogOf(shard); }
  std::vector<queue::ReservedRange> Complete(queue::Reservation&& reservation) override {
    return queue_.Complete(std::move(reservation));
  }
  core::Result<std::vector<core::QueueEntry>> Read(core::ShardId shard, core::SequenceId from_seq,
                                                   size_t max_count, core::Duration timeout,
                                                   core::Durability visible) override {
    return queue_.Read(shard, from_seq, max_count, timeout, visible);
  }
  core::Result<void> CommitOffset(core::ConsumerId consumer, core::ShardId shard,
                                  core::SequenceId seq) override {
    return queue_.CommitOffset(consumer, shard, seq);
  }
  core::Result<std::optional<core::SequenceId>> CommittedOffset(core::ConsumerId consumer,
                                                                core::ShardId shard) override {
    return queue_.CommittedOffset(consumer, shard);
  }
  core::Durability AckDurability() const override { return queue_.AckDurability(); }
  core::Result<core::SequenceId> DurableEnd(core::ShardId shard,
                                            core::Durability durability) override {
    return queue_.DurableEnd(shard, durability);
  }
  core::Result<bool> AwaitDurable(core::ShardId shard, core::SequenceId seq,
                                  core::Durability durability, core::Duration timeout) override {
    return queue_.AwaitDurable(shard, seq, durability, timeout);
  }
  std::optional<core::Result<void>> WhenDurable(core::ShardId shard, core::Durability durability,
                                                core::SeqWaiterNode& node) override {
    return queue_.WhenDurable(shard, durability, node);
  }
  bool CancelDurable(core::ShardId shard, core::Durability durability,
                     core::SeqWaiterNode& node) noexcept override {
    return queue_.CancelDurable(shard, durability, node);
  }
  core::Result<core::SequenceId> FirstSeq(core::ShardId shard) override {
    return queue_.FirstSeq(shard);
  }
  core::RetentionPin PinRetention() override { return queue_.PinRetention(); }
  core::Result<core::SequenceId> OldestRetained(core::ShardId shard) override {
    return queue_.OldestRetained(shard);
  }
  core::Result<core::SequenceId> TailSeq(core::ShardId shard) override {
    return queue_.TailSeq(shard);
  }
  core::Result<core::QueueStats> Stats() override { return queue_.Stats(); }

  core::Result<void> Scan(std::span<const core::SequenceId> from,
                          std::span<const core::SequenceId> end, uint32_t parallelism,
                          const ScanSink& sink, const std::atomic<bool>& cancel) override {
    return queue_.Scan(from, end, parallelism, sink, cancel);
  }

 protected:
  core::Queue& inner() { return queue_; }

 private:
  core::Queue& queue_;
};

}  // namespace abyss::testing
