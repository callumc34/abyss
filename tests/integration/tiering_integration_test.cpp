#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/queue/append_result.h"
#include "integration_harness.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

class TieringIntegrationTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;

  static constexpr core::EvictionTTL kEviction{86400};

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }
};

TEST_F(TieringIntegrationTest, HotReadThroughEngine) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k1", "hot_value"}).has_value());

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "hot_value");
}

TEST_F(TieringIntegrationTest, ColdReadThroughEngine) {
  core::ops::WriteOp op{core::ops::StringSet{.key = "k1", .value = "cold_value"}};
  auto apply = harness_.Cold().ApplyBatch(std::span{&op, 1});
  ASSERT_TRUE(apply.has_value()) << apply.error().message();

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "cold_value");
}

TEST_F(TieringIntegrationTest, BufferReadThroughEngine) {
  harness_.BufferFor("k1").Absorb(
      "k1", core::ops::WriteOp{core::ops::StringSet{.key = "k1", .value = "buf"}}, kEviction);

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "buf");
}

TEST_F(TieringIntegrationTest, HotTakesPriorityOverCold) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k1", "from_hot"}).has_value());

  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "from_cold"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1});
  ASSERT_TRUE(apply_cold.has_value());

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "from_hot");
}

TEST_F(TieringIntegrationTest, BufferTombstoneBlocksColdRead) {
  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "cold_value"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1});
  ASSERT_TRUE(apply_cold.has_value());

  harness_.BufferFor("k1").Absorb(
      "k1", core::ops::WriteOp{core::ops::StringSet{.key = "k1", .value = "v"}}, kEviction);
  harness_.BufferFor("k1").Absorb("k1", core::ops::WriteOp{core::ops::Del{.keys = {"k1"}}},
                                  kEviction);

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(TieringIntegrationTest, AbsoluteTtlExpiresInHot) {
  auto now_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          harness_.Clock().WallNow().time_since_epoch())
                                          .count());
  uint64_t ttl_ms = now_ms + 5000;

  ASSERT_TRUE(
      harness_.SeedHot({"SET", "k1", "expiring", "PXAT", std::to_string(ttl_ms)}).has_value());

  auto before = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ(before->AsString(), "expiring");

  // Drive cold to absorb-and-flush the SET so the buffer is empty for k1 and
  // cold's latest_drained_seq matches hot's settled seq. The engine's
  // buffer-consistency gate on hot-miss reads (ADP-006) requires this in
  // production where the cold consumer runs continuously; the harness's
  // cold pool is intentionally idle, so the test drives the drain itself.
  const auto shard = core::ComputeShard("k1", testing::IntegrationHarness::kShardCount);
  auto& cold_consumer = harness_.ColdPool().ConsumerFor(shard);
  cold_consumer.Drain();
  cold_consumer.FlushUnscheduled();

  harness_.Clock().Advance(6000ms);

  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "cold_fallback"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1});
  ASSERT_TRUE(apply_cold.has_value());

  auto after = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(after->AsString(), "cold_fallback");
}

TEST_F(TieringIntegrationTest, WritePathAwaitsConsumerAck) {
  // The real HotConsumerPool in the harness fulfils the RPC.
  auto result = harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "k1", "v1"}));

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(harness_.Rpc().PendingCount(), 0U);

  // Value is observable in the hot store after the consumer applied it.
  auto read = harness_.Hot().Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k1"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "v1");
}

TEST_F(TieringIntegrationTest, ColdHitStringTriggersPromotion) {
  core::ops::WriteOp set_op{core::ops::StringSet{.key = "cold_only", .value = "cv"}};
  ASSERT_TRUE(harness_.Cold().ApplyBatch(std::span{&set_op, 1}).has_value());

  bool promote_appended = false;
  ON_CALL(harness_.Queue(), Append(::testing::_, ::testing::_))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillByDefault([&promote_appended](core::ShardId, core::QueueEntry entry) {
        if (std::holds_alternative<core::entry::Write>(entry.payload)) {
          const auto& cmd = std::get<core::entry::Write>(entry.payload).cmd;
          if (cmd.args.size() >= 3 && cmd.args[0] == "SET" && cmd.args[1] == "cold_only") {
            promote_appended = true;
          }
        }
        std::promise<core::Result<void>> p;
        p.set_value(core::Result<void>{});
        return queue::AppendResult{.seq = 99, .durable = p.get_future()};
      });

  auto read = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "cold_only"}));
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "cv");
  EXPECT_TRUE(promote_appended);
}

TEST_F(TieringIntegrationTest, ColdHitTtlPreservedInPromotionCommand) {
  auto now_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          harness_.Clock().WallNow().time_since_epoch())
                                          .count());
  const uint64_t abs_ttl_ms = now_ms + 3600000;

  core::ops::WriteOp set_op{
      core::ops::StringSet{.key = "ttl_key", .value = "v", .abs_ttl_ms = abs_ttl_ms}};
  ASSERT_TRUE(harness_.Cold().ApplyBatch(std::span{&set_op, 1}).has_value());

  std::optional<core::RespCommand> promoted;
  ON_CALL(harness_.Queue(), Append(::testing::_, ::testing::_))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillByDefault([&promoted](core::ShardId, core::QueueEntry entry) {
        if (std::holds_alternative<core::entry::Write>(entry.payload)) {
          promoted = std::get<core::entry::Write>(entry.payload).cmd;
        }
        std::promise<core::Result<void>> p;
        p.set_value(core::Result<void>{});
        return queue::AppendResult{.seq = 99, .durable = p.get_future()};
      });

  auto read = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "ttl_key"}));
  ASSERT_TRUE(read.has_value());
  ASSERT_TRUE(promoted.has_value());
  ASSERT_GE(promoted->args.size(), 5U);
  EXPECT_EQ(promoted->args[0], "SET");
  EXPECT_EQ(promoted->args[1], "ttl_key");
  EXPECT_EQ(promoted->args[2], "v");
  EXPECT_EQ(promoted->args[3], "PXAT");
  EXPECT_EQ(promoted->args[4], std::to_string(abs_ttl_ms));
}

TEST_F(TieringIntegrationTest, ColdDeleteRemovesKey) {
  core::ops::WriteOp set_op{core::ops::StringSet{.key = "k1", .value = "v1"}};
  auto apply = harness_.Cold().ApplyBatch(std::span{&set_op, 1});
  ASSERT_TRUE(apply.has_value());

  core::ops::WriteOp del_op{core::ops::Del{.keys = {"k1"}}};
  auto del = harness_.Cold().ApplyBatch(std::span{&del_op, 1});
  ASSERT_TRUE(del.has_value());

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// Synchronous Drain()/Flush() — running the thread would race the test clock.

TEST_F(TieringIntegrationTest, DrainFlushPersistsWriteToColdStoreAfterQuietWindow) {
  // Test drives the cold consumer synchronously; stop the hot pool so its
  // background Reads don't race our EXPECT_CALL.
  harness_.HotPool().Stop();

  constexpr core::ShardId kShard = 0;
  const std::string key = "drain_key";

  std::vector<core::QueueEntry> entries;
  entries.push_back(core::QueueEntry{
      .seq = 1,
      .appended_at = harness_.Clock().WallNow(),
      .payload =
          core::entry::Write{
              .cmd = core::RespCommand{.args = {"SET", key, "persisted"}},
          },
  });

  EXPECT_CALL(harness_.Queue(), Read(core::kColdConsumer, ::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(entries))
      .WillRepeatedly(::testing::Return(std::vector<core::QueueEntry>{}));

  auto& consumer = harness_.ColdPool().ConsumerFor(kShard);
  consumer.Drain();
  consumer.Flush();
  harness_.Clock().Advance(60s);
  consumer.Drain();
  consumer.Flush();

  core::ops::ReadOp read_op{core::ops::StringGet{.key = key}};
  auto from_cold = harness_.Cold().Exec(read_op);
  ASSERT_TRUE(from_cold.has_value()) << from_cold.error().message();
  EXPECT_EQ(from_cold->AsString(), "persisted");
  EXPECT_EQ(consumer.Buffer().Size(), 0);
}

TEST_F(TieringIntegrationTest, TenThousandWritesToSameKeyProduceOneColdWrite) {
  // Test drives the cold consumer synchronously; stop the hot pool so its
  // background Reads don't race our EXPECT_CALL.
  harness_.HotPool().Stop();

  constexpr core::ShardId kShard = 0;
  const std::string key = "burst_key";
  constexpr int kWrites = 10000;

  std::vector<core::QueueEntry> entries;
  entries.reserve(kWrites);
  for (int i = 0; i < kWrites; ++i) {
    entries.push_back(core::QueueEntry{
        .seq = static_cast<core::SequenceId>(i + 1),
        .appended_at = harness_.Clock().WallNow(),
        .payload =
            core::entry::Write{
                .cmd = core::RespCommand{.args = {"SET", key, "v" + std::to_string(i)}},
            },
    });
  }

  EXPECT_CALL(harness_.Queue(), Read(core::kColdConsumer, ::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(entries))
      .WillRepeatedly(::testing::Return(std::vector<core::QueueEntry>{}));

  auto& consumer = harness_.ColdPool().ConsumerFor(kShard);
  consumer.Drain();
  consumer.Flush();
  ASSERT_EQ(consumer.Buffer().Size(), 1U);

  harness_.Clock().Advance(60s);
  consumer.Drain();
  consumer.Flush();

  EXPECT_EQ(consumer.Snapshot().ops_flushed, 1U);

  core::ops::ReadOp read_op{core::ops::StringGet{.key = key}};
  auto from_cold = harness_.Cold().Exec(read_op);
  ASSERT_TRUE(from_cold.has_value());
  EXPECT_EQ(from_cold->AsString(), "v" + std::to_string(kWrites - 1));
}

TEST_F(TieringIntegrationTest, MultipleKeysTieredAcrossStores) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "hot_key", "hv"}).has_value());

  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "cold_key", .value = "cv"}};
  ASSERT_TRUE(harness_.Cold().ApplyBatch(std::span{&cold_op, 1}).has_value());

  harness_.BufferFor("buf_key").Absorb(
      "buf_key", core::ops::WriteOp{core::ops::StringSet{.key = "buf_key", .value = "bv"}},
      kEviction);

  auto r1 = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "hot_key"}));
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(r1->AsString(), "hv");

  auto r2 = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "cold_key"}));
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->AsString(), "cv");

  auto r3 = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "buf_key"}));
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(r3->AsString(), "bv");
}

// C6: a failed promotion Append must not silently disappear.
TEST_F(TieringIntegrationTest, PromotionQueueFailureIncrementsCounter) {
  core::ops::WriteOp set_op{core::ops::StringSet{.key = "cold_only", .value = "cv"}};
  ASSERT_TRUE(harness_.Cold().ApplyBatch(std::span{&set_op, 1}).has_value());

  ON_CALL(harness_.Queue(), Append(::testing::_, ::testing::_))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillByDefault([](core::ShardId, core::QueueEntry) {
        return core::Result<queue::AppendResult>(
            std::unexpected(core::Error{core::ErrorCode::kResourceExhausted, "queue full"}));
      });

  auto read = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "cold_only"}));
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "cv");
  EXPECT_EQ(harness_.Engine().Snapshot().promotion_append_failures, 1U);
}

// Hot-miss reads must not consult the buffer overlay against cold while the
// cold consumer lags hot: a not-yet-absorbed delete leaves the overlay
// reporting state hot has already revoked. The wait gate parks the read
// until cold catches up to hot's settled seq. Covered for hash, string, and
// existence-probe paths.
TEST_F(TieringIntegrationTest, HashReadWithStaleBufferWaitsForColdConsumer) {
  // SeedHot dispatches a Write through the engine: hot applies and fulfils the
  // RPC, hot's HighestSettledSeq advances. The cold consumer is not running in
  // the harness, so cold's latest_drained_seq stays at 0 until we Drain.
  ASSERT_TRUE(harness_.SeedHot({"HSET", "h", "a", "1", "b", "2"}).has_value());
  const auto shard = core::ComputeShard("h", testing::IntegrationHarness::kShardCount);
  // Drain only HSET into the buffer, leaving the buffer "stale" — overlay will
  // report fields={a:1,b:2} until cold catches up to the HDEL.
  harness_.ColdPool().ConsumerFor(shard).Drain();
  ASSERT_EQ(harness_.SeedHot({"HDEL", "h", "a", "b"}).value().AsInteger(), 2);

  // Without the wait gate, HGETALL would see overlay.fields={a:1,b:2}, merge
  // with empty cold, and return a non-empty array. The peer thread drains the
  // queued HDEL after a short delay so the gate succeeds within its budget.
  std::thread advancer([&] {
    std::this_thread::sleep_for(20ms);
    harness_.ColdPool().ConsumerFor(shard).Drain();
  });
  auto result = harness_.Engine().DispatchRead("HGETALL", MakeCmd({"HGETALL", "h"}));
  advancer.join();

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_TRUE(result->IsArray());
  EXPECT_TRUE(result->AsArray().empty());
}

// When the cold consumer is wedged the engine surfaces a timeout rather than
// serving a stale merge — failing closed is the documented contract.
TEST(TieringIntegrationTimeoutTest, HashReadTimesOutWhenColdConsumerWedged) {
  // Builds a harness with a tight wait budget specifically to exercise the
  // timeout path. The default harness widens the budget so the Stale-buffer
  // tests don't race the production timeout on slower runners.
  constexpr auto kTimeout = std::chrono::milliseconds{100};
  testing::IntegrationHarness harness{
      testing::IntegrationHarness::Config{.buffer_consistency_wait_timeout = kTimeout}};
  const auto cmd = [](std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  };

  ASSERT_TRUE(harness.SeedHot({"HSET", "h", "a", "1"}).has_value());
  const auto shard = core::ComputeShard("h", testing::IntegrationHarness::kShardCount);
  harness.ColdPool().ConsumerFor(shard).Drain();
  ASSERT_EQ(harness.SeedHot({"HDEL", "h", "a"}).value().AsInteger(), 1);

  // No advancer — cold stays behind. The configured timeout fires.
  const auto t0 = std::chrono::steady_clock::now();
  auto result = harness.Engine().DispatchRead("HGETALL", cmd({"HGETALL", "h"}));
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_TRUE(result->IsError());
  EXPECT_GE(elapsed, kTimeout);
  EXPECT_EQ(harness.Engine().Snapshot().read_buffer_wait_timeouts, 1U);
}

// Same race as the hash case but for a scalar: SET then DEL through hot, with
// cold having only absorbed the SET, leaves the buffer holding the deleted
// value. Without the gate a GET would surface it; with the gate the read
// waits, the buffer absorbs the DEL (tombstone), and GET returns nil.
TEST_F(TieringIntegrationTest, StringReadWithStaleBufferWaitsForColdConsumer) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k", "v"}).has_value());
  const auto shard = core::ComputeShard("k", testing::IntegrationHarness::kShardCount);
  harness_.ColdPool().ConsumerFor(shard).Drain();  // buffer now holds k=v.
  ASSERT_EQ(harness_.SeedHot({"DEL", "k"}).value().AsInteger(), 1);

  std::thread advancer([&] {
    std::this_thread::sleep_for(20ms);
    harness_.ColdPool().ConsumerFor(shard).Drain();
  });
  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k"}));
  advancer.join();

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(result->IsNull()) << "expected nil, got: " << result->AsString();
}

// EXISTS probe must observe the same gate — a stale buffer reporting kPresent
// after hot already DEL'd the key would resurrect the count.
TEST_F(TieringIntegrationTest, ExistsProbeWaitsForColdConsumer) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k", "v"}).has_value());
  const auto shard = core::ComputeShard("k", testing::IntegrationHarness::kShardCount);
  harness_.ColdPool().ConsumerFor(shard).Drain();
  ASSERT_EQ(harness_.SeedHot({"DEL", "k"}).value().AsInteger(), 1);

  std::thread advancer([&] {
    std::this_thread::sleep_for(20ms);
    harness_.ColdPool().ConsumerFor(shard).Drain();
  });
  auto result =
      harness_.Engine().DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k"}));
  advancer.join();

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_TRUE(result->IsInteger());
  EXPECT_EQ(result->AsInteger(), 0);
}

}  // namespace
}  // namespace abyss::engine
