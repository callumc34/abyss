#pragma once

#include <gmock/gmock.h>

#include "abyss/core/hot_store.h"

namespace abyss::testing {

class MockHotStore : public core::HotStore {
 public:
  MOCK_METHOD(core::Result<core::RespValue>, Exec, (const core::RespCommand& cmd), (override));
  MOCK_METHOD(core::Result<void>, Apply, (const core::RespCommand& cmd, core::EvictionTTL eviction),
              (override));
  MOCK_METHOD(core::Result<void>, ApplyBatch,
              (std::span<const core::RespCommand> cmds, core::EvictionTTL eviction), (override));
  MOCK_METHOD(core::Result<core::MemoryStats>, Stats, (), (override));
  MOCK_METHOD(core::Result<void>, Flush, (), (override));
};

}  // namespace abyss::testing
