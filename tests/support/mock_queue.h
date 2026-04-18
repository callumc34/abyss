#pragma once

#include <gmock/gmock.h>

#include "abyss/core/queue.h"

namespace abyss::testing {

class MockQueue : public core::Queue {
 public:
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
  MOCK_METHOD(core::Result<core::QueueStats>, Stats, (), (override));
};

}  // namespace abyss::testing
