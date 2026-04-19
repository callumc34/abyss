#include "abyss/engine/tiering_engine.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/ops.h"
#include "mock_cold_store.h"
#include "mock_hot_store.h"
#include "mock_queue.h"

namespace abyss::engine {
namespace {

using ::testing::_;
using ::testing::Return;
using namespace std::chrono_literals;

class SingleBufferRouter : public consumer::CompactionBufferRouter {
 public:
  explicit SingleBufferRouter(consumer::CompactionBuffer& buffer) : buffer_(buffer) {}
  core::Result<core::RespValue> Read(std::string_view key) const override {
    return buffer_.Read(std::string(key));
  }

 private:
  consumer::CompactionBuffer& buffer_;
};

class TieringEngineTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::MockQueue queue_;
  testing::MockHotStore hot_;
  testing::MockColdStore cold_;
  consumer::CompactionBuffer buffer_;
  SingleBufferRouter router_{buffer_};
  core::ConsumerRpc rpc_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
  static constexpr uint32_t kShardCount = 16;
  static constexpr std::chrono::milliseconds kWriteTimeout = 1s;

  TieringEngine MakeEngine() {
    return {
        queue_, hot_,
        cold_,  router_,
        rpc_,   TieringEngineConfig{.shard_count = kShardCount, .write_timeout = kWriteTimeout}};
  }

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }

  static queue::PendingAppend MakePending(core::SequenceId seq, bool fsync_ok) {
    std::promise<core::Result<void>> p;
    if (fsync_ok) {
      p.set_value(core::Result<void>{});
    } else {
      p.set_value(std::unexpected(core::Error{core::ErrorCode::kInternal, "fsync failed"}));
    }
    return queue::PendingAppend{seq, p.get_future(),
                                std::make_unique<testing::NoopAppendPublisher>()};
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

TEST_F(TieringEngineTest, WriteSuccessReturnsConsumerResult) {
  auto engine = MakeEngine();

  constexpr core::SequenceId kSeq = 42;
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(kSeq, true); });

  std::thread fulfiller([this]() {
    while (!rpc_.Fulfill(kSeq, core::RespValue::SimpleString("OK"))) {
      std::this_thread::sleep_for(1ms);
    }
  });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteConsumerErrorPropagates) {
  auto engine = MakeEngine();

  constexpr core::SequenceId kSeq = 43;
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(kSeq, true); });

  std::thread fulfiller([this]() {
    while (!rpc_.Fulfill(kSeq, core::RespValue::Error(core::ErrorPrefix::kWrongType,
                                                      "operation against wrong type"))) {
      std::this_thread::sleep_for(1ms);
    }
  });

  auto result = engine.DispatchWrite("SADD", MakeCmd({"SADD", "key", "m"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
  EXPECT_EQ(result->AsString(), "WRONGTYPE operation against wrong type");
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteQueueFailureReturnsError) {
  auto engine = MakeEngine();

  EXPECT_CALL(queue_, BeginAppend(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kResourceExhausted, "full"))));

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteFsyncFailureCancelsRpcAndPropagates) {
  auto engine = MakeEngine();

  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(99, false); });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteTimeoutReturnsErrorAndCancelsRpc) {
  TieringEngineConfig fast{.shard_count = kShardCount, .write_timeout = 50ms};
  TieringEngine engine(queue_, hot_, cold_, router_, rpc_, fast);

  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(100, true); });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
  EXPECT_TRUE(result->AsString().starts_with("ERR "));
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

// C5: a slow durable wait must not leave the RPC wait starved. The guaranteed
// min RPC budget is write_timeout * min_rpc_wait_fraction. Without C5, a
// durable completion near the original deadline would give the RPC ~0 budget.
TEST_F(TieringEngineTest, SlowDurableDoesNotStarveRpcBudget) {
  TieringEngineConfig cfg{
      .shard_count = kShardCount,
      .write_timeout = 200ms,
      .min_rpc_wait_fraction = 0.5,  // ≥100ms of RPC budget, even if durable is slow.
  };
  TieringEngine engine(queue_, hot_, cold_, router_, rpc_, cfg);

  constexpr core::SequenceId kSeq = 201;
  std::promise<core::Result<void>> durable_p;
  auto durable_fut = durable_p.get_future();
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([&durable_fut](core::ShardId, core::QueueEntry) {
        return queue::PendingAppend{kSeq, std::move(durable_fut),
                                    std::make_unique<testing::NoopAppendPublisher>()};
      });

  // Durable completes at ~150ms — close to the original 200ms deadline.
  std::thread durable_releaser([&durable_p]() {
    std::this_thread::sleep_for(150ms);
    durable_p.set_value(core::Result<void>{});
  });
  // RPC fulfilled at ~220ms — past the original deadline but inside the
  // guaranteed RPC floor (150ms + 100ms = 250ms).
  std::thread rpc_releaser([this]() {
    std::this_thread::sleep_for(220ms);
    while (!rpc_.Fulfill(kSeq, core::RespValue::SimpleString("OK"))) {
      std::this_thread::sleep_for(1ms);
    }
  });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  durable_releaser.join();
  rpc_releaser.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

}  // namespace
}  // namespace abyss::engine
