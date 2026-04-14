#pragma once

#include <gmock/gmock.h>

#include "abyss/core/queue.h"

namespace abyss::testing {

class MockQueue : public core::Queue {
 public:
  MOCK_METHOD(core::Result<core::SequenceId>, Append, (core::ShardId shard, core::RespCommand cmd),
              (override));
  MOCK_METHOD(core::Result<core::SequenceId>, AppendBatch,
              (core::ShardId shard, std::span<const core::RespCommand> cmds), (override));
  MOCK_METHOD((core::Result<std::vector<core::LogEntry>>), Read,
              (core::ConsumerId consumer, core::ShardId shard, size_t max_count,
               core::Duration timeout),
              (override));
  MOCK_METHOD(core::Result<void>, Ack,
              (core::ConsumerId consumer, core::ShardId shard, core::SequenceId seq), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, OldestRetained, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::QueueStats>, Stats, (), (override));
};

}  // namespace abyss::testing
