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
#include "abyss/core/types.h"

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
  core::Result<void> Admit(core::ShardId shard, core::SteadyTime admit_by) override {
    return queue_.Admit(shard, admit_by);
  }
  bool WaitForSpare(core::ShardId shard, core::SteadyTime deadline) override {
    return queue_.WaitForSpare(shard, deadline);
  }
  core::Result<queue::Reservation> Reserve(std::span<const queue::ShardEntries> parts) override {
    return queue_.Reserve(parts);
  }
  core::Result<queue::Reservation> ReserveFlush(
      std::span<const queue::ShardEntries> parts) override {
    return queue_.ReserveFlush(parts);
  }
  uint32_t LogOf(core::ShardId shard) const override { return queue_.LogOf(shard); }
  queue::DurableFutures Complete(queue::Reservation&& reservation) override {
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
