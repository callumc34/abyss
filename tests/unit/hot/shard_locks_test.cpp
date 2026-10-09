#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::hot {
namespace {

namespace ops = core::ops;
using Presence = KeyView::Presence;

constexpr uint32_t kShards = 4;

std::string KeyOn(core::ShardId shard, int n = 0) {
  for (int i = 0;; ++i) {
    std::string key = "key" + std::to_string(i);
    if (core::ComputeShard(key, kShards) == shard && n-- == 0) return key;
  }
}

core::Effect EffectOf(std::vector<std::string> args, bool replaces_state = true) {
  core::Effect effect{.key = args.at(1), .replaces_state = replaces_state};
  effect.cmd.args = std::move(args);
  return effect;
}

core::WallTime At(int64_t ms) { return core::WallTime{std::chrono::milliseconds{ms}}; }

class ShardLocksTest : public ::testing::Test {
 protected:
  void Build(size_t max_memory_bytes = size_t{64} << 20, double stub_fraction = 0.02) {
    store_ = std::make_unique<ShardedHotStore>(ShardedHotStoreConfig{
        .max_memory_bytes = max_memory_bytes,
        .shard_count = kShards,
        .stub_memory_fraction = stub_fraction,
        .drained = [this](core::ShardId) { return drained_.load(); },
    });
  }
  void SetUp() override { Build(); }

  void Apply(core::ShardId shard, std::vector<std::string> args, core::SequenceId seq) {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{shard});
    std::vector<core::Effect> effects;
    effects.push_back(EffectOf(std::move(args)));
    locks.ApplyEffects(shard, effects, seq, At(1000));
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<core::SequenceId> drained_{0};
  std::unique_ptr<ShardedHotStore> store_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(ShardLocksTest, AViewNamesItsShard) {
  const std::string key = KeyOn(2);
  Apply(2, {"SET", key, "v"}, 5);
  auto locks = store_->LockExclusive(std::vector<core::ShardId>{1, 2});
  const KeyView view = locks.View(key, 0);
  EXPECT_EQ(view.presence, Presence::kLive);
  EXPECT_EQ(view.shard, 2U);
  EXPECT_EQ(view.latest_seq, 5U);
  EXPECT_EQ(locks.View(KeyOn(1), 0).shard, 1U);
}

// The drained seq is read once the locks are held, so it is no older
// than an eviction behind a miss: the flush floor is judged by it.
TEST_F(ShardLocksTest, AWipeSetsTheFloorAViewReads) {
  const std::string key = KeyOn(0);
  Apply(0, {"SET", key, "v"}, 1);
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
    locks.Wipe(0, 4);
  }
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
    const KeyView view = locks.View(key, 0);
    EXPECT_EQ(view.presence, Presence::kTombstoned);
    EXPECT_TRUE(view.flush_floor);
    EXPECT_EQ(view.latest_seq, 4U);
    EXPECT_EQ(locks.BeginLoad(key).status, LoadStart::Status::kFlushed);
  }
  drained_ = 4;
  auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
  EXPECT_EQ(locks.View(key, 0).presence, Presence::kNonResident);
}

// A batch keeps every install, past the stub cap and the budget; the
// next eviction trims them.
TEST_F(ShardLocksTest, ABatchOfLoadsKeepsEveryInstall) {
  // Two stubs a shard.
  Build(size_t{64} << 20,
        2.0 * kShards * static_cast<double>(kStubBytes) / static_cast<double>(size_t{64} << 20));
  drained_ = 100;
  std::vector<LoadCompletion> loads;
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
    for (int i = 0; i < 5; ++i) {
      const std::string key = KeyOn(0, i);
      const LoadStart start = locks.BeginLoad(key);
      ASSERT_TRUE(start.started());
      loads.push_back({.key = key, .token = start.token, .result = LoadedExists{}});
    }
  }
  auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
  EXPECT_EQ(locks.CompleteLoads(0, loads), 5U);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(locks.View(KeyOn(0, i), 0).presence, Presence::kStub) << i;
  }
}

TEST_F(ShardLocksTest, AReadFencesOnWhatAnswers) {
  const std::string live = KeyOn(1, 0);
  const std::string gone = KeyOn(1, 1);
  Apply(1, {"SET", live, "v"}, 3);
  Apply(1, {"SET", gone, "v"}, 4);
  Apply(1, {"DEL", gone}, 7);

  const auto hit = store_->Read(ops::ReadOp{ops::StringGet{.key = live}});
  ASSERT_TRUE(hit.result.has_value());
  EXPECT_EQ(hit.result->AsString(), "v");
  EXPECT_EQ(hit.fence, 3U);

  const auto deleted = store_->Read(ops::ReadOp{ops::StringGet{.key = gone}});
  ASSERT_TRUE(deleted.result.has_value());
  EXPECT_TRUE(deleted.result->IsNull());
  EXPECT_EQ(deleted.fence, 7U) << "a tombstone's seq counts";

  const auto exists = store_->Read(ops::ReadOp{ops::Exists{.keys = {gone}}});
  ASSERT_TRUE(exists.result.has_value());
  EXPECT_EQ(exists.result->AsInteger(), 0);
  EXPECT_EQ(exists.fence, 7U);

  const auto miss = store_->Read(ops::ReadOp{ops::StringGet{.key = KeyOn(1, 2)}});
  ASSERT_FALSE(miss.result.has_value());
  EXPECT_EQ(miss.result.error().code(), core::ErrorCode::kNotFound);
  EXPECT_FALSE(miss.fence.has_value());

  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{1});
    locks.Wipe(1, 9);
  }
  const auto floor = store_->Read(ops::ReadOp{ops::SetCard{.key = KeyOn(1, 3)}});
  ASSERT_TRUE(floor.result.has_value()) << "under the floor a miss is absent";
  EXPECT_EQ(floor.result->AsInteger(), 0);
  EXPECT_EQ(floor.fence, 9U);
}

// Loaded state carries 0, below every durable end, and a shard's first
// write carries kFirstSeq: the two never share a fence.
TEST_F(ShardLocksTest, LoadedStateFencesOnZeroAndAFirstWriteOnItsSeq) {
  const std::string loaded = KeyOn(3, 0);
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{3});
    const LoadStart start = locks.BeginLoad(loaded);
    ASSERT_TRUE(start.started());
    std::vector<LoadCompletion> loads;
    loads.push_back({.key = loaded, .token = start.token, .result = LoadedAbsent{}});
    locks.CompleteLoads(3, loads);
  }
  EXPECT_EQ(store_->Read(ops::ReadOp{ops::StringGet{.key = loaded}}).fence, 0U);

  const std::string written = KeyOn(3, 1);
  Apply(3, {"SET", written, "v"}, core::kFirstSeq);
  EXPECT_EQ(store_->Read(ops::ReadOp{ops::StringGet{.key = written}}).fence, core::kFirstSeq);
}

TEST_F(ShardLocksTest, TheShardClockOnlyRises) {
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{0, 1});
    locks.RaiseAppendedAt(0, At(500));
    locks.RaiseAppendedAt(0, At(200));
    EXPECT_EQ(locks.LastAppendedAt(0), At(500));
    EXPECT_EQ(locks.LastAppendedAt(1), core::WallTime{});
  }
  // A replayed entry raises its key's shard, Write or Flush alike.
  core::QueueEntry write{.seq = 1,
                         .appended_at = At(900),
                         .payload = core::entry::Write{.cmd = {.args = {"SET", KeyOn(1), "v"}}},
                         .replaces_state = true};
  store_->ApplyLogged(1, write);
  core::QueueEntry flush{.seq = 2, .appended_at = At(950), .payload = core::entry::Flush{}};
  store_->ApplyLogged(2, flush);
  auto locks = store_->LockExclusive(std::vector<core::ShardId>{0, 1, 2});
  EXPECT_EQ(locks.LastAppendedAt(1), At(900));
  EXPECT_EQ(locks.LastAppendedAt(2), At(950));
  EXPECT_EQ(locks.View(KeyOn(1), 0).presence, Presence::kLive);
}

TEST_F(ShardLocksTest, BackpressureIsJudgedLive) {
  // 4 KiB a shard: over its limit past 5 KiB.
  Build(size_t{16} << 10);
  Apply(0, {"SET", KeyOn(0, 0), std::string(3000, 'a')}, 1);
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
    EXPECT_FALSE(locks.OverBackpressure(0));
  }
  Apply(0, {"SET", KeyOn(0, 1), std::string(3000, 'b')}, 2);
  {
    auto locks = store_->LockExclusive(std::vector<core::ShardId>{0});
    EXPECT_TRUE(locks.OverBackpressure(0)) << "nothing drained, so nothing could be evicted";
  }
  drained_ = 2;
  EXPECT_TRUE(store_->EvictShardToTarget(0));
}

}  // namespace
}  // namespace abyss::hot
