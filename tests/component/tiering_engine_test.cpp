#include "abyss/engine/tiering_engine.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"
#include "abyss/hot/sharded_hot_store.h"
#include "mock_cold_store.h"
#include "mock_queue.h"
#include "on_exit.h"

namespace abyss::engine {
namespace {

using ::testing::_;
using ::testing::Return;
using namespace std::chrono_literals;

class SingleBufferRouter : public consumer::CompactionBufferRouter {
 public:
  explicit SingleBufferRouter(consumer::CompactionBuffer& buffer) : buffer_(buffer) {}
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view key) const override {
    return buffer_.Snapshot(key);
  }
  // Component-level engine tests run without a cold consumer thread, so the
  // wait is a structural no-op: there is no producer to advance the seq.
  // Returning true models "already caught up", which is the steady-state
  // expectation outside the slow-cold race we cover in integration tests.
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }

 private:
  consumer::CompactionBuffer& buffer_;
};

// A real hot store, buffer and loader over a mock cold store and an
// in-memory queue: what the engine dispatches, end to end.
class TieringEngineTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kShardCount = 16;
  static constexpr std::chrono::milliseconds kWriteTimeout = 1s;

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<testing::MockQueue> queue_;
  hot::ShardedHotStore hot_{hot::ShardedHotStoreConfig{
      .max_memory_bytes = 64UL << 20,
      .shard_count = kShardCount,
  }};
  ::testing::NiceMock<testing::MockColdStore> cold_;
  consumer::CompactionBuffer buffer_;
  SingleBufferRouter router_{buffer_};
  Loader loader_{hot_, router_, cold_};
  Sequencer sequencer_{hot_, queue_, loader_, router_,
                       SequencerConfig{.write_timeout = kWriteTimeout}};
  ReadPath reads_{hot_, loader_, sequencer_, ReadPathConfig{.write_timeout = kWriteTimeout}};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  TieringEngine MakeEngine() { return {reads_, sequencer_}; }

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }

  // Writes through the engine, so hot holds the key as decided.
  void Seed(TieringEngine& engine, std::initializer_list<std::string> args) {
    auto written = engine.DispatchWrite(*args.begin(), MakeCmd(args), core::PredicateFlags::kNone);
    ASSERT_TRUE(written.has_value()) << written.error().message();
    ASSERT_FALSE(written->IsError()) << written->AsString();
  }

  core::ShardId ShardOf(std::string_view key) const { return core::ComputeShard(key, kShardCount); }
};

// --- Write path ---

TEST_F(TieringEngineTest, WriteGoesThroughTheSequencer) {
  auto engine = MakeEngine();
  auto result =
      engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}), core::PredicateFlags::kNone);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "OK");

  const auto published = queue_.Published(ShardOf("key"));
  ASSERT_EQ(published.size(), 1U);
  EXPECT_EQ(std::get<core::entry::Write>(published[0].payload).cmd.args,
            (std::vector<std::string>{"SET", "key", "value"}));
  auto read = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "value");
}

TEST_F(TieringEngineTest, WriteDecideErrorLogsNothing) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "key", "v"}));
  auto result =
      engine.DispatchWrite("SADD", MakeCmd({"SADD", "key", "m"}), core::PredicateFlags::kNone);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
  EXPECT_EQ(queue_.Published(ShardOf("key")).size(), 1U);
}

TEST_F(TieringEngineTest, WriteReserveFailureAppliesNothing) {
  auto engine = MakeEngine();
  queue_.SetReserveFault([](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
    return core::Error{core::ErrorCode::kInternal, "pwrite: I/O error"};
  });
  auto result =
      engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}), core::PredicateFlags::kNone);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  EXPECT_TRUE(queue_.Published(ShardOf("key")).empty());
  auto read = hot_.Read(core::ops::ReadOp{core::ops::StringGet{.key = "key"}});
  ASSERT_FALSE(read.result.has_value());
  EXPECT_EQ(read.result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(TieringEngineTest, WriteDurableFailurePropagates) {
  auto engine = MakeEngine();
  queue_.FailDurable(core::Error{core::ErrorCode::kInternal, "fsync failed"});
  auto result =
      engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}), core::PredicateFlags::kNone);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
}

TEST_F(TieringEngineTest, WriteDurableTimeoutRepliesWithTheTimeoutText) {
  Sequencer fast(hot_, queue_, loader_, router_, SequencerConfig{.write_timeout = 50ms});
  TieringEngine engine(reads_, fast);
  queue_.HoldDurable();
  const testing::OnExit release([this] { queue_.ReleaseDurable(); });
  auto result =
      engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}), core::PredicateFlags::kNone);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_TRUE(result.error().message().starts_with("write durable wait exceeded server timeout"))
      << result.error().message();
}

// --- Fan-out (MGET, EXISTS, MSET, DEL) ---
//
// Reads stay per key (#170); MSET and DEL are one decision and one
// reservation across their shards.

TEST_F(TieringEngineTest, MgetAggregatesAcrossTiersInPositionalOrder) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "a", "from_hot"}));
  buffer_.Absorb("b", core::ops::WriteOp{core::ops::StringSet{.key = "b", .value = "from_buf"}},
                 core::EvictionTTL{86400}, 1, 0);

  // Only c and d read cold: a hits hot, and b's delta is its whole state.
  using LoadAsResult = core::Result<std::optional<core::LoadedAs>>;
  EXPECT_CALL(cold_, LoadKeyAs(std::string_view{"c"}, core::KeyType::kString, _))
      .WillOnce(Return(LoadAsResult{core::LoadedAs{
          core::ColdKeyState{.type = core::KeyType::kString, .value = "from_cold"}}}));
  EXPECT_CALL(cold_, LoadKeyAs(std::string_view{"d"}, core::KeyType::kString, _))
      .WillOnce(Return(LoadAsResult{std::nullopt}));
  EXPECT_CALL(cold_, LoadKeyAs(std::string_view{"b"}, _, _)).Times(0);

  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kMget, MakeCmd({"MGET", "a", "b", "c", "d"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  const auto& arr = result->AsArray();
  ASSERT_EQ(arr.size(), 4U);
  EXPECT_EQ(arr[0].AsString(), "from_hot");
  EXPECT_EQ(arr[1].AsString(), "from_buf");
  EXPECT_EQ(arr[2].AsString(), "from_cold");
  EXPECT_TRUE(arr[3].IsNull());
}

TEST_F(TieringEngineTest, MgetWrongTypeInHotCollapsesToNil) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SADD", "k", "m"}));

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kMget, MakeCmd({"MGET", "k"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  ASSERT_EQ(result->AsArray().size(), 1U);
  EXPECT_TRUE(result->AsArray()[0].IsNull());
}

TEST_F(TieringEngineTest, ExistsTombstoneInBufferOverridesColdResidual) {
  auto engine = MakeEngine();
  // Buffer holds a SET then a DEL — tombstone state for "k".
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}},
                 core::EvictionTTL{86400}, 1, 0);
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, core::EvictionTTL{86400},
                 1, 0);

  // Hot has no record; cold is never consulted because the buffer's
  // tombstone decides.
  EXPECT_CALL(cold_, ProbeKey(_, _)).Times(0);

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 0);
}

TEST_F(TieringEngineTest, ExistsCountsDuplicatesRedisStyle) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "k", "v"}));

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, MsetReservesOneBatchAcrossItsShards) {
  auto engine = MakeEngine();
  int reserves = 0;
  queue_.SetReserveFault(
      [&reserves](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
        ++reserves;
        return std::nullopt;
      });

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kMset,
                                      MakeCmd({"MSET", "a", "1", "b", "2", "c", "3"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(reserves, 1);
  for (const auto& [key, value] : {std::pair{"a", "1"}, std::pair{"b", "2"}, std::pair{"c", "3"}}) {
    bool found = false;
    for (const auto& entry : queue_.Published(ShardOf(key))) {
      const auto& args = std::get<core::entry::Write>(entry.payload).cmd.args;
      found = found || args == std::vector<std::string>{"SET", key, value};
    }
    EXPECT_TRUE(found) << "key '" << key << "' is not on its own shard";
  }
}

TEST_F(TieringEngineTest, DelCountsLiveKeysInOneDecision) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "a", "1"}));
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "c", "3"}));
  // b is not resident: its existence is probed, never loaded in full.
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"b"}, _))
      .WillOnce(Return(core::Result<std::optional<core::KeyMeta>>(std::nullopt)));
  EXPECT_CALL(cold_, LoadKey(_, _)).Times(0);

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kDelete, MakeCmd({"DEL", "a", "b", "c"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, FlushAdmitsEveryShardWithinOneDeadline) {
  auto engine = MakeEngine();
  std::vector<core::SteadyTime> deadlines;
  EXPECT_CALL(queue_, Admit(_, _))
      .Times(kShardCount)
      .WillRepeatedly([&deadlines](core::ShardId, core::SteadyTime admit_by) {
        deadlines.push_back(admit_by);
        return core::Result<void>{};
      });

  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "OK");
  ASSERT_EQ(deadlines.size(), kShardCount);
  EXPECT_TRUE(std::ranges::all_of(deadlines, [&](auto d) { return d == deadlines.front(); }));
  for (core::ShardId shard = 0; shard < kShardCount; ++shard) {
    const auto published = queue_.Published(shard);
    ASSERT_EQ(published.size(), 1U) << shard;
    EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(published[0].payload));
  }
}

TEST_F(TieringEngineTest, FlushAdmissionFailureWipesNothing) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "k", "v"}));
  std::vector<core::SteadyTime> deadlines;
  ON_CALL(queue_, Admit(_, _))
      .WillByDefault([&deadlines](core::ShardId shard,
                                  core::SteadyTime admit_by) -> core::Result<void> {
        deadlines.push_back(admit_by);
        if (shard == kShardCount - 1) {
          return std::unexpected(core::Error{core::ErrorCode::kResourceExhausted, "window full"});
        }
        return {};
      });
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  ASSERT_EQ(deadlines.size(), kShardCount);
  EXPECT_TRUE(std::ranges::all_of(deadlines, [&](auto d) { return d == deadlines.front(); }));
  for (core::ShardId shard = 0; shard < kShardCount; ++shard) {
    EXPECT_EQ(queue_.Published(shard).size(), shard == ShardOf("k") ? 1U : 0U) << shard;
  }
  EXPECT_EQ(engine.DispatchRead("GET", MakeCmd({"GET", "k"}))->AsString(), "v");
}

TEST_F(TieringEngineTest, FlushReserveFailureWipesNothing) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "k", "v"}));
  queue_.SetReserveFault([](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
    return core::Error{core::ErrorCode::kInternal, "pwrite: I/O error"};
  });
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  EXPECT_EQ(queue_.Published(ShardOf("k")).size(), 1U);
  EXPECT_EQ(engine.DispatchRead("GET", MakeCmd({"GET", "k"}))->AsString(), "v");
}

TEST_F(TieringEngineTest, FlushDurableTimeoutRepliesWithTheTimeoutText) {
  Sequencer fast(hot_, queue_, loader_, router_, SequencerConfig{.write_timeout = 50ms});
  TieringEngine engine(reads_, fast);
  queue_.HoldDurable();
  const testing::OnExit release([this] { queue_.ReleaseDurable(); });
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_TRUE(result.error().message().starts_with("flush durable wait exceeded server timeout"))
      << result.error().message();
}

TEST_F(TieringEngineTest, FlushDurableFailurePropagates) {
  auto engine = MakeEngine();
  queue_.FailDurable(core::Error{core::ErrorCode::kInternal, "fsync failed"});
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
}

// A multi-key DEL admits every shard against one deadline, the write's.
TEST_F(TieringEngineTest, DelAdmitsEveryShardWithinOneDeadline) {
  auto engine = MakeEngine();
  std::vector<core::SteadyTime> deadlines;
  ON_CALL(queue_, Admit(_, _))
      .WillByDefault([&deadlines](core::ShardId, core::SteadyTime admit_by) {
        deadlines.push_back(admit_by);
        return core::Result<void>{};
      });
  const auto start = core::SteadyClock::now();
  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kDelete, MakeCmd({"DEL", "a", "b", "c", "d", "e"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_GE(deadlines.size(), 2U);
  EXPECT_TRUE(std::ranges::all_of(deadlines, [&](auto d) { return d == deadlines.front(); }));
  EXPECT_GE(deadlines.front(), start + kWriteTimeout);
  EXPECT_LT(deadlines.front(), start + kWriteTimeout + 1s);
}

// One reservation: a refused MSET applies none of its keys.
TEST_F(TieringEngineTest, MultiKeyReserveFailureAppliesNothing) {
  auto engine = MakeEngine();
  queue_.SetReserveFault([](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
    return core::Error{core::ErrorCode::kInternal, "pwrite: I/O error"};
  });

  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kMset, MakeCmd({"MSET", "a", "1", "b", "2"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  for (const char* key : {"a", "b"}) {
    auto read = hot_.Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    EXPECT_FALSE(read.result.has_value()) << key;
  }
}

}  // namespace
}  // namespace abyss::engine
