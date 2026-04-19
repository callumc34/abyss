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
  core::ops::WriteOp op{core::ops::StringSet{.key = "k1", .value = "hot_value"}};
  auto apply = harness_.Hot().Apply(op, kEviction);
  ASSERT_TRUE(apply.has_value()) << apply.error().message();

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
  core::ops::WriteOp hot_op{core::ops::StringSet{.key = "k1", .value = "from_hot"}};
  auto apply_hot = harness_.Hot().Apply(hot_op, kEviction);
  ASSERT_TRUE(apply_hot.has_value());

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

  core::ops::WriteOp op{
      core::ops::StringSet{.key = "k1", .value = "expiring", .abs_ttl_ms = ttl_ms}};
  auto apply = harness_.Hot().Apply(op, kEviction);
  ASSERT_TRUE(apply.has_value());

  auto before = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ(before->AsString(), "expiring");

  harness_.Clock().Advance(6000ms);

  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "cold_fallback"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1});
  ASSERT_TRUE(apply_cold.has_value());

  auto after = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(after->AsString(), "cold_fallback");
}

TEST_F(TieringIntegrationTest, WritePathAwaitsConsumerAck) {
  // Simulate the hot consumer: fulfil the RPC for the seq about to be used.
  const core::SequenceId expected_seq = harness_.PeekNextSeq();
  std::thread fulfiller([this, expected_seq]() {
    while (!harness_.Rpc().Fulfill(expected_seq, core::RespValue::SimpleString("OK"))) {
      std::this_thread::sleep_for(1ms);
    }
  });

  auto result = harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "k1", "v1"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(harness_.Rpc().PendingCount(), 0U);
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

  const ::testing::InSequence seq;
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
  core::ops::WriteOp hot_op{core::ops::StringSet{.key = "hot_key", .value = "hv"}};
  ASSERT_TRUE(harness_.Hot().Apply(hot_op, kEviction).has_value());

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

}  // namespace
}  // namespace abyss::engine
