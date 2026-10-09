#pragma once

#include <gmock/gmock.h>

#include <optional>
#include <span>
#include <string_view>
#include <vector>

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
    // Unless a test says otherwise, cold holds no member asked for.
    ON_CALL(*this, LoadMembers(_, _, _, _))
        .WillByDefault([](std::string_view, core::KeyType,
                          std::span<const std::string_view> members, core::SteadyTime) {
          return core::Result<std::vector<std::optional<core::MemberValue>>>(members.size());
        });
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
  MOCK_METHOD((core::Result<std::optional<core::ColdKeyState>>), LoadKey,
              (std::string_view key, core::SteadyTime deadline), (override));
  MOCK_METHOD((core::Result<std::optional<core::LoadedAs>>), LoadKeyAs,
              (std::string_view key, core::KeyType type, core::SteadyTime deadline), (override));
  MOCK_METHOD((core::Result<std::optional<core::KeyMeta>>), ProbeKey,
              (std::string_view key, core::SteadyTime deadline), (override));
  MOCK_METHOD((core::Result<std::vector<std::optional<core::MemberValue>>>), LoadMembers,
              (std::string_view key, core::KeyType type, std::span<const std::string_view> members,
               core::SteadyTime deadline),
              (override));
};

}  // namespace abyss::testing
