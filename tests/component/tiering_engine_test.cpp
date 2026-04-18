#include "abyss/engine/tiering_engine.h"

#include <gtest/gtest.h>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/ops.h"
#include "mock_cold_store.h"
#include "mock_hot_store.h"
#include "mock_queue.h"

namespace abyss::engine {
namespace {

using ::testing::_;
using ::testing::Return;

class TieringEngineTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::MockQueue queue_;
  testing::MockHotStore hot_;
  testing::MockColdStore cold_;
  consumer::CompactionBuffer buffer_;
  core::ConsumerRpc rpc_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
  static constexpr uint32_t kShardCount = 16;

  TieringEngine MakeEngine() { return {queue_, hot_, cold_, buffer_, rpc_, kShardCount}; }

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }
};

// --- Read path ---

TEST_F(TieringEngineTest, ReadHotHitReturnsValue) {
  auto engine = MakeEngine();
  auto expected = core::RespValue::BulkString("value");

  EXPECT_CALL(hot_, Exec(_)).WillOnce(Return(expected));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_EQ(result->AsString(), "value");
}

TEST_F(TieringEngineTest, ReadHotMissBufferHitReturnsBufferValue) {
  auto engine = MakeEngine();

  buffer_.Absorb("key", core::ops::WriteOp{core::ops::StringSet{.key = "key", .value = "buffered"}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "buffered");
}

TEST_F(TieringEngineTest, ReadHotMissBufferMissColdHitReturnsColdValue) {
  auto engine = MakeEngine();
  auto cold_value = core::RespValue::BulkString("cold_value");

  EXPECT_CALL(hot_, Exec(_))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  EXPECT_CALL(cold_, Exec(_)).WillOnce(Return(cold_value));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "cold_value");
}

TEST_F(TieringEngineTest, ReadAllTiersMissReturnsColdError) {
  auto engine = MakeEngine();

  EXPECT_CALL(hot_, Exec(_))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  EXPECT_CALL(cold_, Exec(_))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "missing"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(TieringEngineTest, ReadHotErrorPropagates) {
  auto engine = MakeEngine();

  EXPECT_CALL(hot_, Exec(_))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kWrongType, "wrong type"))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(TieringEngineTest, ReadBufferTombstoneReturnsNull) {
  auto engine = MakeEngine();

  buffer_.Absorb("key", core::ops::WriteOp{core::ops::StringSet{.key = "key", .value = "v"}},
                 core::EvictionTTL{86400});
  buffer_.Absorb("key", core::ops::WriteOp{core::ops::Del{.keys = {"key"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// --- Write path ---

TEST_F(TieringEngineTest, WriteAppendsToQueueAndReturnsOk) {
  auto engine = MakeEngine();

  EXPECT_CALL(queue_, Append(_, _)).WillOnce([](core::ShardId, core::QueueEntry) {
    std::promise<core::Result<void>> p;
    p.set_value(core::Result<void>{});
    return queue::AppendResult{.seq = 1, .durable = p.get_future()};
  });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
}

TEST_F(TieringEngineTest, WriteQueueFailureReturnsError) {
  auto engine = MakeEngine();

  EXPECT_CALL(queue_, Append(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kResourceExhausted, "full"))));

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
}

}  // namespace
}  // namespace abyss::engine
