#pragma once

#include <gmock/gmock.h>

#include <span>
#include <vector>

#include "abyss/core/queue.h"

namespace abyss::testing {

class MockQueue : public core::Queue {
 public:
  MOCK_METHOD(core::Result<queue::PendingAppend>, BeginAppend,
              (core::ShardId shard, core::QueueEntry entry), (override));
  MOCK_METHOD(core::Result<queue::PendingBatchAppend>, BeginAppendBatch,
              (core::ShardId shard, std::span<const core::QueueEntry> entries), (override));
  MOCK_METHOD(core::Result<queue::AppendResult>, Append,
              (core::ShardId shard, core::QueueEntry entry), (override));
  MOCK_METHOD(core::Result<queue::AppendBatchResult>, AppendBatch,
              (core::ShardId shard, std::span<const core::QueueEntry> entries), (override));
  MOCK_METHOD((core::Result<std::vector<core::QueueEntry>>), Read,
              (core::ConsumerId consumer, core::ShardId shard, size_t max_count,
               core::Duration timeout),
              (override));
  MOCK_METHOD(core::Result<void>, Ack,
              (core::ConsumerId consumer, core::ShardId shard, core::SequenceId seq), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, OldestRetained, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, TailSeq, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, AckOffset,
              (core::ConsumerId consumer, core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::QueueStats>, Stats, (), (override));
};

// No-op publisher used by tests that want a PendingAppend without a real WAL.
class NoopAppendPublisher : public queue::AppendPublisher {
 public:
  void Publish() noexcept override {}
};

}  // namespace abyss::testing
