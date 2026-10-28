// Runs with the counting allocator of alloc_counter.cpp, so it is its
// own binary.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/core/types.h"
#include "abyss/engine/blocking_driver.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/wal_queue.h"
#include "alloc_counter.h"
#include "mock_cold_store.h"
#include "sequencer_fixture.h"
#include "temp_dir.h"

namespace abyss::engine {
namespace {

using abyss::testing::BigAllocs;
using abyss::testing::BigFrees;
using abyss::testing::Frees;
using abyss::testing::ThreadAllocs;

class SequencerAllocTest : public testing::SequencerFixture {};

// A large SET's value is copied once, for the log, before the lock; the
// request's own bytes move into hot.
TEST_F(SequencerAllocTest, ALargeSetCopiesItsValueOnceAndMovesIt) {
  core::RespCommand cmd{.args = {"SET", "k", std::string(1 << 20, 'v')}};
  const char* request_bytes = cmd.args[2].data();
  const std::size_t before = BigAllocs();
  auto result = RunToCompletion(*sequencer_, std::move(cmd), core::PredicateFlags::kNone);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(BigAllocs() - before, 1U) << "one copy, for the log entry";

  auto locks = hot_->LockExclusive(std::vector<core::ShardId>{ShardOf("k")});
  EXPECT_EQ(locks.View("k", 0).string_value().data(), request_bytes) << "hot holds the request's";
}

// A refused Reserve hands the copy back, so the retry makes none.
TEST_F(SequencerAllocTest, ARetryReusesTheCopy) {
  auto refused = std::make_shared<std::atomic<bool>>(false);
  queue_.SetReserveFault(
      [refused](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
        if (refused->exchange(true)) return std::nullopt;
        return core::Error{core::ErrorCode::kResourceExhausted, "window full"};
      });
  core::RespCommand cmd{.args = {"SET", "k", std::string(1 << 20, 'v')}};
  const std::size_t before = BigAllocs();
  auto result = RunToCompletion(*sequencer_, std::move(cmd), core::PredicateFlags::kNone);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(refused->load());
  EXPECT_EQ(BigAllocs() - before, 1U);
  ASSERT_EQ(Logged(ShardOf("k")).size(), 1U);
  EXPECT_EQ(Logged(ShardOf("k"))[0][2].size(), std::size_t{1} << 20);
}

// The sequencer frees what a write replaced only once it is published:
// a large free must not hold up this publish, and so the next writer.
TEST_F(SequencerAllocTest, ARetryOverSeveralShardsReusesEachCopy) {
  auto refused = std::make_shared<std::atomic<bool>>(false);
  queue_.SetReserveFault(
      [refused](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
        if (refused->exchange(true)) return std::nullopt;
        return core::Error{core::ErrorCode::kResourceExhausted, "window full"};
      });
  // Effect order is b then a; log order is by shard.
  const std::string a = KeyOn(0);
  const std::string b = KeyOn(1);
  const std::string a_value(std::size_t{1} << 20, 'a');
  const std::string b_value(std::size_t{1} << 20, 'b');
  core::RespCommand cmd{.args = {"MSET", b, b_value, a, a_value}};
  const std::size_t before = BigAllocs();
  auto result = RunToCompletion(*sequencer_, std::move(cmd), core::PredicateFlags::kNone);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(Describe(*result), "OK");
  EXPECT_TRUE(refused->load());
  EXPECT_EQ(BigAllocs() - before, 2U) << "one copy each, kept across the retry";
  ASSERT_EQ(Logged(ShardOf(a)).size(), 1U);
  ASSERT_EQ(Logged(ShardOf(b)).size(), 1U);
  EXPECT_EQ(Logged(ShardOf(a))[0], (std::vector<std::string>{"SET", a, a_value}));
  EXPECT_EQ(Logged(ShardOf(b))[0], (std::vector<std::string>{"SET", b, b_value}));
  auto locks = hot_->LockExclusive(std::vector<core::ShardId>{ShardOf(a), ShardOf(b)});
  EXPECT_EQ(locks.View(a, 0).string_value(), a_value);
  EXPECT_EQ(locks.View(b, 0).string_value(), b_value);
}

TEST_F(SequencerAllocTest, AReplacedValueIsFreedAfterComplete) {
  core::RespCommand big{.args = {"SET", "k", std::string(1 << 20, 'v')}};
  ASSERT_TRUE(
      RunToCompletion(*sequencer_, std::move(big), core::PredicateFlags::kNone).has_value());
  const std::size_t before = BigFrees();
  std::optional<std::size_t> at_complete;
  queue_.OnComplete([&at_complete] { at_complete = BigFrees(); });
  auto result = RunToCompletion(*sequencer_, core::RespCommand{.args = {"SET", "k", "small"}},
                                core::PredicateFlags::kNone);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(at_complete, before) << "freed under the lock or before Complete";
  EXPECT_EQ(BigFrees(), before + 1);
}

core::Effect EffectOf(std::vector<std::string> args, bool replaces_state) {
  core::Effect effect{.key = args.at(1), .replaces_state = replaces_state};
  effect.cmd.args = std::move(args);
  return effect;
}

// What an apply replaces or removes is freed after the hold, not in it.
TEST_F(SequencerAllocTest, NothingAnApplyReplacesIsFreedUnderTheLock) {
  const core::ShardId shard = ShardOf("big");
  const std::vector<core::ShardId> held{shard};
  core::SequenceId seq = 1;
  {
    auto locks = hot_->LockExclusive(held);
    std::vector<core::Effect> set;
    set.push_back(EffectOf({"SET", "big", std::string(1 << 20, 'a')}, true));
    locks.ApplyEffects(shard, set, seq++, Wall());
  }

  {
    SCOPED_TRACE("a SET over a 1 MiB string");
    auto locks = hot_->LockExclusive(held);
    std::vector<core::Effect> set;
    set.push_back(EffectOf({"SET", "big", "small"}, true));
    const std::size_t before = BigFrees();
    locks.ApplyEffects(shard, set, seq++, Wall());
    EXPECT_EQ(BigFrees(), before) << "freed under the lock";
    hot::Graveyard replaced = locks.Unlock();
    EXPECT_EQ(BigFrees(), before) << "freed before its holder let it go";
    replaced = {};
    EXPECT_EQ(BigFrees(), before + 1);
  }

  const std::string set_key = KeyOn(shard, 0, "set");
  {
    auto locks = hot_->LockExclusive(held);
    std::vector<core::Effect> add;
    std::vector<std::string> args{"SADD", set_key};
    for (int i = 0; i < 100'000; ++i) args.push_back("member-" + std::to_string(i));
    add.push_back(EffectOf(std::move(args), true));
    locks.ApplyEffects(shard, add, seq++, Wall());
  }
  {
    SCOPED_TRACE("a DEL of a 100k-member set");
    auto locks = hot_->LockExclusive(held);
    std::vector<core::Effect> del;
    del.push_back(EffectOf({"DEL", set_key}, true));
    const std::size_t before = Frees();
    locks.ApplyEffects(shard, del, seq++, Wall());
    EXPECT_LT(Frees() - before, 1000U) << "the members were freed under the lock";
    hot::Graveyard replaced = locks.Unlock();
    EXPECT_LT(Frees() - before, 1000U) << "freed before its holder let it go";
    replaced = {};
    EXPECT_GE(Frees() - before, 100'000U);
  }
}

// A cache fill evicts to make room under the exclusive lock; what it
// evicts is freed after the hold.
// The sequencer over a real WAL, so Reserve's own allocations count.
class HoldAllocTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kShards = 4;

  void SetUp() override {
    auto queue = queue::WalQueue::Open(queue::WalConfig{
        .wal_path = dir_.Sub("wal").string(),
        .segment_size_bytes = std::size_t{8} << 20,
        .shard_count = kShards,
        .min_retention = std::chrono::seconds{0},
        .retention_consumers = {core::kColdConsumer},
        .offset_fsync_interval = std::chrono::hours{1},
    });
    ASSERT_TRUE(queue.has_value()) << queue.error().message();
    queue_ = std::move(*queue);
    hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = std::size_t{64} << 20, .shard_count = kShards});
    loader_ = std::make_unique<Loader>(*hot_, router_, cold_);
    sequencer_ = std::make_unique<Sequencer>(*hot_, *queue_, *loader_, router_, SequencerConfig{});
  }

  // What this thread allocated while `args` held its hot shards.
  std::size_t AllocsUnderHold(std::vector<std::string> args) {
    std::size_t at_lock = 0;
    std::size_t under = 0;
    hot_->SetWriteHoldObserverForTesting([&at_lock, &under](bool held) {
      if (held) {
        at_lock = ThreadAllocs();
      } else {
        under += ThreadAllocs() - at_lock;
      }
    });
    auto result = RunToCompletion(*sequencer_, core::RespCommand{.args = std::move(args)},
                                  core::PredicateFlags::kNone);
    hot_->SetWriteHoldObserverForTesting(nullptr);
    EXPECT_TRUE(result.has_value()) << result.error().message();
    return under;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"hold_alloc"};
  std::unique_ptr<queue::WalQueue> queue_;
  testing::DrainRouter router_;
  ::testing::NiceMock<abyss::testing::MockColdStore> cold_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<Loader> loader_;
  std::unique_ptr<Sequencer> sequencer_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(HoldAllocTest, ASmallSetOverAResidentKeyAllocatesLittleUnderItsHold) {
  const std::string key(32, 'k');
  ASSERT_EQ(AllocsUnderHold({"SET", key, std::string(64, 'a')}) > 0, true);
  const std::size_t under = AllocsUnderHold({"SET", key, std::string(64, 'b')});
  RecordProperty("allocs_under_hold", static_cast<int>(under));
  std::cout << "allocations under the hold: " << under << '\n';
  // 16 with libc++: deciding 5, the log entry 4, Reserve 5 and the
  // apply 2. The rest is slack for other standard libraries.
  EXPECT_LE(under, 20U);
}

// The whole of a resident SET's write under process_crash, the fast
// path: nothing past what the blocking write path took (its shards,
// entries and reservation among them), per standard library.
#ifdef _MSC_VER
constexpr std::size_t kAllocsPerWrite = 25;
#else
constexpr std::size_t kAllocsPerWrite = 23;
#endif
TEST_F(HoldAllocTest, ASmallSetOverAResidentKeyAllocatesNoMoreThanTheBlockingPath) {
  const std::string key(32, 'k');
  std::size_t fewest = SIZE_MAX;
  std::size_t most = 0;
  for (int i = 0; i < 64; ++i) {
    const std::size_t before = ThreadAllocs();
    auto result = RunToCompletion(
        *sequencer_,
        core::RespCommand{.args = {"SET", key, std::string(64, static_cast<char>('a' + (i % 26)))}},
        core::PredicateFlags::kNone);
    const std::size_t used = ThreadAllocs() - before;
    ASSERT_TRUE(result.has_value()) << result.error().message();
    if (i < 8) continue;
    fewest = std::min(fewest, used);
    most = std::max(most, used);
  }
  RecordProperty("allocs_per_write", static_cast<int>(most));
  std::cout << "allocations per write: " << fewest << ".." << most << '\n';
  EXPECT_LE(most, kAllocsPerWrite);
}

TEST(FillAllocTest, WhatAFillEvictsIsFreedAfterTheHold) {
  hot::SingleShardStore store{hot::SingleShardConfig{.max_memory_bytes = size_t{3} << 20}};
  ASSERT_TRUE(store
                  .Apply(core::ops::WriteOp{core::ops::StringSet{
                             .key = "old", .value = std::string(size_t{1} << 20, 'o')}},
                         core::EvictionTTL{3600}, core::kFirstSeq, hot::kAllDrained)
                  .has_value());
  hot::LoadResult fill = hot::MakeLoadedFull(std::string(size_t{5} << 19, 'n'), 0);
  const auto token = store.BeginLoad("new");
  ASSERT_TRUE(token.has_value());

  const std::size_t before = BigFrees();
  hot::Graveyard evicted;
  store.SetGraveyard(&evicted);
  ASSERT_TRUE(store.CompleteLoad("new", token.value_or(hot::LoadToken{}), std::move(fill),
                                 core::EvictionTTL{3600}, hot::kAllDrained));
  store.SetGraveyard(nullptr);
  EXPECT_FALSE(store.HasEntry("old")) << "the fill made no room";
  EXPECT_EQ(BigFrees(), before) << "freed under the lock";
  evicted = {};
  EXPECT_EQ(BigFrees(), before + 1);
}

}  // namespace
}  // namespace abyss::engine
