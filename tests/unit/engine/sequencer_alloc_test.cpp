// Runs with the counting allocator of alloc_counter.cpp, so it is its
// own binary.

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/hot/sharded_hot_store.h"
#include "alloc_counter.h"
#include "sequencer_fixture.h"

namespace abyss::engine {
namespace {

using abyss::testing::BigAllocs;
using abyss::testing::BigFrees;
using abyss::testing::Frees;

class SequencerAllocTest : public testing::SequencerFixture {};

// A large SET's value is copied once, for the log, before the lock; the
// request's own bytes move into hot.
TEST_F(SequencerAllocTest, ALargeSetCopiesItsValueOnceAndMovesIt) {
  core::RespCommand cmd{.args = {"SET", "k", std::string(1 << 20, 'v')}};
  const char* request_bytes = cmd.args[2].data();
  const std::size_t before = BigAllocs();
  auto result = sequencer_->Execute(std::move(cmd), core::PredicateFlags::kNone);
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
  auto result = sequencer_->Execute(std::move(cmd), core::PredicateFlags::kNone);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(refused->load());
  EXPECT_EQ(BigAllocs() - before, 1U);
  ASSERT_EQ(Logged(ShardOf("k")).size(), 1U);
  EXPECT_EQ(Logged(ShardOf("k"))[0][2].size(), std::size_t{1} << 20);
}

// The sequencer frees what a write replaced only once it is published:
// a large free must not hold up this publish, and so the next writer.
TEST_F(SequencerAllocTest, AReplacedValueIsFreedAfterComplete) {
  core::RespCommand big{.args = {"SET", "k", std::string(1 << 20, 'v')}};
  ASSERT_TRUE(sequencer_->Execute(std::move(big), core::PredicateFlags::kNone).has_value());
  const std::size_t before = BigFrees();
  std::optional<std::size_t> at_complete;
  queue_.OnComplete([&at_complete] { at_complete = BigFrees(); });
  auto result = sequencer_->Execute(core::RespCommand{.args = {"SET", "k", "small"}},
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

}  // namespace
}  // namespace abyss::engine
