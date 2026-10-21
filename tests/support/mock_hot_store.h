#pragma once

#include <gmock/gmock.h>

#include "abyss/core/hot_store.h"

namespace abyss::testing {

class MockHotStore : public core::HotStore {
 public:
  MOCK_METHOD(core::Result<core::RespValue>, Exec,
              (const core::ops::ReadOp& op, std::optional<core::Duration> deadline), (override));
  MOCK_METHOD(core::Result<core::RespValue>, Apply,
              (const core::ops::WriteOp& op, core::SequenceId seq), (override));
  MOCK_METHOD(core::Result<void>, ApplyBatch,
              (std::span<const core::ops::WriteOp> ops, core::SequenceId seq), (override));
  MOCK_METHOD(core::HotKeyPresence, Probe, (std::string_view key), (override));
  MOCK_METHOD(core::Result<core::MemoryStats>, Stats, (), (override));
  MOCK_METHOD(core::Result<void>, Wipe, (core::ShardId shard), (override));
};

}  // namespace abyss::testing
