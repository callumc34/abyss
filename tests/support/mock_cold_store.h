#pragma once

#include <gmock/gmock.h>

#include <optional>
#include <string_view>

#include "abyss/core/cold_store.h"

namespace abyss::testing {

class MockColdStore : public core::ColdStore {
 public:
  MOCK_METHOD(core::Result<core::RespValue>, Exec, (const core::ops::ReadOp& op), (override));
  MOCK_METHOD(core::Result<void>, ApplyBatch, (std::span<const core::ops::WriteOp> ops),
              (override));
  MOCK_METHOD(core::Result<core::StorageStats>, Stats, (), (override));
  MOCK_METHOD(core::Result<void>, Compact, (), (override));
  MOCK_METHOD((core::Result<std::optional<core::RespCommand>>), GetPromotionCommand,
              (std::string_view key), (override));
};

}  // namespace abyss::testing
