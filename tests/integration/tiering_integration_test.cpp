#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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
  auto apply = harness_.Cold().ApplyBatch(std::span{&op, 1}, 0);
  ASSERT_TRUE(apply.has_value()) << apply.error().message();

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "cold_value");
}

TEST_F(TieringIntegrationTest, BufferReadThroughEngine) {
  harness_.BufferFor("k1").Absorb(
      "k1", core::ops::WriteOp{core::ops::StringSet{.key = "k1", .value = "buf"}}, kEviction, 1, 1,
      0);

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "buf");
}

TEST_F(TieringIntegrationTest, HotTakesPriorityOverCold) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k1", "from_hot"}).has_value());

  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "from_cold"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1}, 0);
  ASSERT_TRUE(apply_cold.has_value());

  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "from_hot");
}

TEST_F(TieringIntegrationTest, BufferTombstoneBlocksColdRead) {
  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "cold_value"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1}, 0);
  ASSERT_TRUE(apply_cold.has_value());

  harness_.BufferFor("k1").Absorb(
      "k1", core::ops::WriteOp{core::ops::StringSet{.key = "k1", .value = "v"}}, kEviction, 1, 1,
      0);
  harness_.BufferFor("k1").Absorb("k1", core::ops::WriteOp{core::ops::Del{.keys = {"k1"}}},
                                  kEviction, 1, 1, 0);

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

  const auto shard = core::ComputeShard("k1", testing::IntegrationHarness::kShardCount);
  auto& cold_consumer = harness_.ColdPool().ConsumerFor(shard);
  cold_consumer.Drain();
  cold_consumer.FlushUnscheduled();

  harness_.Clock().Advance(6000ms);

  core::ops::WriteOp cold_op{core::ops::StringSet{.key = "k1", .value = "cold_fallback"}};
  auto apply_cold = harness_.Cold().ApplyBatch(std::span{&cold_op, 1}, 0);
  ASSERT_TRUE(apply_cold.has_value());

  // Hot still holds k1, past its TTL: its latest state is absent,
  // whatever cold holds.
  auto after = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k1"}));
  ASSERT_TRUE(after.has_value());
  EXPECT_TRUE(after->IsNull());
}

TEST_F(TieringIntegrationTest, WriteIsInHotWhenItReplies) {
  auto result = harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "k1", "v1"}));

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");

  // The sequencer applied it before replying.
  auto read = harness_.Hot().Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k1"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "v1");
}

TEST_F(TieringIntegrationTest, ColdDeleteRemovesKey) {
  core::ops::WriteOp set_op{core::ops::StringSet{.key = "k1", .value = "v1"}};
  auto apply = harness_.Cold().ApplyBatch(std::span{&set_op, 1}, 0);
  ASSERT_TRUE(apply.has_value());

  core::ops::WriteOp del_op{core::ops::Del{.keys = {"k1"}}};
  auto del = harness_.Cold().ApplyBatch(std::span{&del_op, 1}, 0);
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

  EXPECT_CALL(harness_.Queue(),
              Read(kShard, ::testing::_, ::testing::_, ::testing::_, ::testing::_))
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

  EXPECT_CALL(harness_.Queue(),
              Read(kShard, ::testing::_, ::testing::_, ::testing::_, ::testing::_))
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
  ASSERT_TRUE(harness_.Cold().ApplyBatch(std::span{&cold_op, 1}, 0).has_value());

  harness_.BufferFor("buf_key").Absorb(
      "buf_key", core::ops::WriteOp{core::ops::StringSet{.key = "buf_key", .value = "bv"}},
      kEviction, 1, 1, 0);

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

// A recent delete becomes an authoritative hot tombstone, so reads of a deleted
// key answer from hot without consulting — or waiting on — the lagging overlay.

// A key on the same shard as `key`, for a write that leaves the shard's
// cold consumer behind without touching `key` itself.
namespace {
std::string SameShardPrimer(std::string_view key) {
  const auto shard = core::ComputeShard(key, testing::IntegrationHarness::kShardCount);
  for (int i = 0;; ++i) {
    std::string candidate = "primer" + std::to_string(i);
    if (core::ComputeShard(candidate, testing::IntegrationHarness::kShardCount) == shard) {
      return candidate;
    }
  }
}
}  // namespace

TEST_F(TieringIntegrationTest, DeletedScalarReadsNilGateFree) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k", "v"}).has_value());
  const auto shard = core::ComputeShard("k", testing::IntegrationHarness::kShardCount);
  harness_.ColdPool().ConsumerFor(shard).Drain();                    // cold absorbs the SET only.
  ASSERT_EQ(harness_.SeedHot({"DEL", "k"}).value().AsInteger(), 1);  // hot tombstone; cold lags it.

  const auto t0 = std::chrono::steady_clock::now();
  auto result = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "k"}));
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(result->IsNull());
  EXPECT_LT(elapsed, 500ms) << "tombstone read must not wait on the cold consumer";
}

TEST_F(TieringIntegrationTest, DeletedKeyExistsReturnsZeroGateFree) {
  ASSERT_TRUE(harness_.SeedHot({"SET", "k", "v"}).has_value());
  const auto shard = core::ComputeShard("k", testing::IntegrationHarness::kShardCount);
  harness_.ColdPool().ConsumerFor(shard).Drain();
  ASSERT_EQ(harness_.SeedHot({"DEL", "k"}).value().AsInteger(), 1);

  const auto t0 = std::chrono::steady_clock::now();
  auto result =
      harness_.Engine().DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k"}));
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsInteger(), 0);
  EXPECT_LT(elapsed, 500ms) << "tombstone EXISTS must not wait on the cold consumer";
}

TEST_F(TieringIntegrationTest, EmptiedHashReadsEmptyGateFree) {
  ASSERT_TRUE(harness_.SeedHot({"HSET", "h", "a", "1", "b", "2"}).has_value());
  const auto shard = core::ComputeShard("h", testing::IntegrationHarness::kShardCount);
  harness_.ColdPool().ConsumerFor(shard).Drain();
  ASSERT_EQ(harness_.SeedHot({"HDEL", "h", "a", "b"}).value().AsInteger(),
            2);  // empties → tombstone

  const auto t0 = std::chrono::steady_clock::now();
  auto result = harness_.Engine().DispatchRead("HGETALL", MakeCmd({"HGETALL", "h"}));
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_TRUE(result->IsArray());
  EXPECT_TRUE(result->AsArray().empty());
  EXPECT_LT(elapsed, 500ms)
      << "emptied-collection tombstone read must not wait on the cold consumer";
}

// A collection hot has never held is wholly in buffer plus cold, so a
// read of it needs no wait, even with the cold consumer behind hot.
TEST_F(TieringIntegrationTest, HotAbsentCollectionReadNeedsNoWait) {
  core::ops::WriteOp h{core::ops::HashSet{.key = "ch", .fields = {{.field = "a", .value = "1"}}}};
  ASSERT_TRUE(harness_.Cold().ApplyBatch(std::span{&h, 1}, 0).has_value());
  ASSERT_TRUE(harness_.SeedHot({"SET", SameShardPrimer("ch"), "v"}).has_value());

  const auto t0 = std::chrono::steady_clock::now();
  auto result = harness_.Engine().DispatchRead("HGETALL", MakeCmd({"HGETALL", "ch"}));
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_TRUE(result->IsArray());
  EXPECT_EQ(result->AsArray().size(), 2U);
  EXPECT_LT(elapsed, 500ms) << "a miss must not wait on the cold consumer";
}

// COLD-3 coupling: ZRANGEBYLEX must return byte-identical results whether the
// zset is served from hot or, after eviction, from cold — across a battery of
// lex bounds. Guards the hot/cold view-equivalence the COLD-3 bundle requires.
TEST_F(TieringIntegrationTest, ZrangeByLexHotColdEquivalence) {
  static constexpr const char* kKey = "z";
  // Equal scores so the order is purely lexicographic.
  ASSERT_TRUE(harness_.SeedHot({"ZADD", kKey, "0", "a", "0", "b", "0", "c", "0", "d"}).has_value());

  struct Case {
    const char* min;
    const char* max;
  };
  // ZRANGEBYLEX has no REV option in Redis (REV lives on the unified ZRANGE);
  // the by_lex rev path is covered by the hot/cold unit tests.
  const std::vector<Case> cases = {
      {"[b", "(d"}, {"-", "+"}, {"(a", "[c"}, {"[a", "[d"}, {"[x", "[z"},  // empty slice
      {"(d", "[a"},                                                        // empty (min > max)
  };

  // Order-preserving projection of a ZRANGEBYLEX reply to member strings.
  auto to_members = [](const core::RespValue& v) {
    std::vector<std::string> out;
    for (const auto& e : v.AsArray()) out.push_back(e.AsString());
    return out;
  };
  auto run = [&](const Case& c) {
    return harness_.Engine().DispatchRead(
        "ZRANGEBYLEX",
        core::RespCommand{.args = std::vector<std::string>{"ZRANGEBYLEX", kKey, c.min, c.max}});
  };

  std::vector<std::vector<std::string>> hot_results;
  hot_results.reserve(cases.size());
  for (const auto& c : cases) {
    auto r = run(c);
    ASSERT_TRUE(r.has_value()) << r.error().message();
    hot_results.push_back(to_members(*r));
  }

  // Flush the zset to cold, then evict it from hot so the same reads now route
  // through the cold tier via the overlay.
  const auto shard = core::ComputeShard(kKey, testing::IntegrationHarness::kShardCount);
  auto& cold_consumer = harness_.ColdPool().ConsumerFor(shard);
  cold_consumer.Drain();
  cold_consumer.FlushUnscheduled();
  harness_.Clock().Advance(48h);
  ASSERT_GE(harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow()).Total(), 1U);

  for (size_t i = 0; i < cases.size(); ++i) {
    auto cold = run(cases[i]);
    ASSERT_TRUE(cold.has_value()) << cold.error().message();
    EXPECT_EQ(to_members(*cold), hot_results[i])
        << "hot/cold ZRANGEBYLEX divergence for [" << cases[i].min << ", " << cases[i].max << "]";
  }
}

}  // namespace
}  // namespace abyss::engine
