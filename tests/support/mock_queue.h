#pragma once

#include <gmock/gmock.h>

#include <limits>
#include <span>
#include <vector>

#include "abyss/core/queue.h"

namespace abyss::testing {

class MockQueue : public core::Queue {
 public:
  MockQueue() {
    // Permissive durability defaults so tests that don't care about the A1
    // watermark keep compiling and passing: DurableSeq reports "everything
    // durable" and AwaitDurable always succeeds. Tests exercising durability
    // override these with EXPECT_CALL/ON_CALL.
    using ::testing::_;
    using ::testing::Return;
    ON_CALL(*this, DurableSeq(_))
        .WillByDefault(
            Return(core::Result<core::SequenceId>(std::numeric_limits<core::SequenceId>::max())));
    ON_CALL(*this, AwaitDurable(_, _, _)).WillByDefault(Return(core::Result<bool>(true)));
  }

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
  MOCK_METHOD(core::Result<core::SequenceId>, DurableSeq, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<bool>, AwaitDurable,
              (core::ShardId shard, core::SequenceId seq, core::Duration timeout), (override));
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
