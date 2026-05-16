#pragma once

#include <gmock/gmock.h>

#include "abyss/core/hot_store.h"

namespace abyss::testing {

class MockHotStore : public core::HotStore {
 public:
  MOCK_METHOD(core::Result<core::RespValue>, Exec,
              (const core::ops::ReadOp& op, std::optional<core::Duration> deadline), (override));
  MOCK_METHOD(core::Result<core::RespValue>, Apply, (const core::ops::WriteOp& op), (override));
  MOCK_METHOD(core::Result<void>, ApplyBatch, (std::span<const core::ops::WriteOp> ops),
              (override));
  MOCK_METHOD(core::Result<core::MemoryStats>, Stats, (), (override));
  MOCK_METHOD(core::Result<void>, Flush, (), (override));
};

}  // namespace abyss::testing
