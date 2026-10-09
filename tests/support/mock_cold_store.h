#pragma once

#include <gmock/gmock.h>

#include <optional>
#include <span>
#include <string_view>

#include "abyss/core/cold_store.h"

namespace abyss::testing {

class MockColdStore : public core::ColdStore {
 public:
  MockColdStore() {
    // Permissive durability default so tests that don't exercise the A6
    // checkpoint keep compiling and passing: Checkpoint always succeeds.
    using ::testing::_;
    using ::testing::Return;
    ON_CALL(*this, Checkpoint(_, _)).WillByDefault(Return(core::Result<void>{}));
  }

  MOCK_METHOD(core::Result<core::RespValue>, Exec,
              (const core::ops::ReadOp& op, std::optional<core::Duration> deadline), (override));
  MOCK_METHOD(core::Result<void>, ApplyBatch,
              (std::span<const core::ops::WriteOp> ops, core::SequenceId highest_wal_seq),
              (override));
  MOCK_METHOD(core::Result<void>, Checkpoint, (core::ShardId shard, core::SequenceId up_to_wal_seq),
              (override));
  MOCK_METHOD(core::Result<void>, Wipe, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::StorageStats>, Stats, (), (override));
  MOCK_METHOD(core::Result<void>, Compact, (), (override));
  MOCK_METHOD((core::Result<std::optional<core::RespCommand>>), GetPromotionCommand,
              (std::string_view key), (override));
  MOCK_METHOD((core::Result<std::optional<core::ColdKeyState>>), LoadKey,
              (std::string_view key, core::SteadyTime deadline), (override));
  MOCK_METHOD((core::Result<std::optional<core::KeyMeta>>), ProbeKey,
              (std::string_view key, core::SteadyTime deadline), (override));
  MOCK_METHOD((core::Result<std::optional<core::MemberValue>>), LoadMember,
              (std::string_view key, core::KeyType type, std::string_view member,
               core::SteadyTime deadline),
              (override));
};

}  // namespace abyss::testing
