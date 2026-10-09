#include "abyss/engine/hot_replayer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

constexpr core::SequenceId kFirst = core::kFirstSeq;
// The log's wall time, in ms.
constexpr int64_t kT0 = 1'700'000'000'000;
constexpr int64_t kTwoHours = int64_t{2} * 3600 * 1000;

core::WallTime At(int64_t ms) { return core::WallTime{std::chrono::milliseconds{ms}}; }

core::QueueEntry Frame(core::SequenceId seq, std::vector<std::string> args, bool replaces_state,
                       int64_t at_ms = kT0) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = At(at_ms),
      .payload = core::entry::Write{.cmd = core::RespCommand{.args = std::move(args)}},
      .replaces_state = replaces_state,
  };
}

core::QueueEntry FlushFrame(core::SequenceId seq, int64_t at_ms = kT0) {
  return core::QueueEntry{.seq = seq, .appended_at = At(at_ms), .payload = core::entry::Flush{}};
}

// The bytes each key of Sets takes in hot.
size_t EntryBytes() {
  hot::SingleShardStore probe(hot::SingleShardConfig{});
  const auto applied =
      probe.Apply(core::ops::StringSet{.key = "k10000", .value = std::string(200, 'x')},
                  core::EvictionTTL{3600});
  EXPECT_TRUE(applied.has_value());
  return static_cast<size_t>(probe.Stats().used_bytes);
}

// Frames [first, first + n), each setting a key of its own, all as
// long as the probe's.
std::vector<core::QueueEntry> Sets(core::SequenceId first, size_t n) {
  std::vector<core::QueueEntry> frames;
  frames.reserve(n);
  for (core::SequenceId seq = first; seq < first + n; ++seq) {
    frames.push_back(
        Frame(seq, {"SET", "k" + std::to_string(10000 + seq), std::string(200, 'x')}, true));
  }
  return frames;
}

// A clock that counts its reads.
struct CountingClocks {
  std::atomic<int> steady_reads{0};
  std::atomic<int> wall_reads{0};
  core::SteadyTime steady_now{std::chrono::hours{1000}};
  core::WallTime wall_now = At(kT0);

  core::SteadyClockFn Steady() {
    return [this] {
      steady_reads.fetch_add(1);
      return steady_now;
    };
  }
  core::WallClockFn Wall() {
    return [this] {
      wall_reads.fetch_add(1);
      return wall_now;
    };
  }
};

class HotReplayerTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }

  void Build(size_t max_memory_bytes = size_t{64} << 20) {
    hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = max_memory_bytes,
        .shard_count = 1,
        .drained = [this](core::ShardId) { return horizon_.load(); },
        .eviction_policy = &policy_,
        .steady_clock = hot_clocks_.Steady(),
        .wall_clock = hot_clocks_.Wall(),
    });
    replayer_ = std::make_unique<HotReplayer>(
        *hot_,
        [this](core::ShardId shard, core::SequenceId through) -> core::Result<void> {
          drains_.emplace_back(shard, through);
          if (!drain_result_.has_value()) return drain_result_;
          if (drain_advances_) horizon_.store(through);
          return {};
        },
        HotReplayer::Config{.steady_clock = clocks_.Steady(), .wall_clock = clocks_.Wall()});
  }

  // One batch of every frame, from kFirst; the replayer expects `end`.
  core::Result<void> ReplayAll(std::vector<core::QueueEntry> frames, core::SequenceId end) {
    const std::vector<core::SequenceId> from{kFirst};
    const std::vector<core::SequenceId> ends{end};
    replayer_->Begin(from, ends);
    return replayer_->Apply(0, frames);
  }

  core::Result<void> ReplayAll(std::vector<core::QueueEntry> frames) {
    const core::SequenceId end = kFirst + frames.size();
    return ReplayAll(std::move(frames), end);
  }

  hot::KeyView::Presence PresenceOf(std::string_view key) {
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{0});
    return locks.View(key, 0).presence;
  }
  bool Resident(std::string_view key) {
    const auto presence = PresenceOf(key);
    return presence != hot::KeyView::Presence::kNonResident &&
           presence != hot::KeyView::Presence::kStub;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  CountingClocks hot_clocks_;
  CountingClocks clocks_;
  std::atomic<core::SequenceId> horizon_{hot::kAllDrained};
  std::vector<std::pair<core::ShardId, core::SequenceId>> drains_;
  core::Result<void> drain_result_;
  bool drain_advances_ = true;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<HotReplayer> replayer_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// A flagged write or a Flush makes its key resident; any other write
// applies only to a key hot holds, a deleted one included, and is
// otherwise skipped and counted.
TEST_F(HotReplayerTest, OnlyAWholeStateMakesAKeyResident) {
  Build();
  ASSERT_TRUE(ReplayAll({
                            Frame(kFirst, {"SADD", "tail", "c"}, false),
                            Frame(kFirst + 1, {"SADD", "whole", "a"}, true),
                            Frame(kFirst + 2, {"SADD", "whole", "b"}, false),
                            Frame(kFirst + 3, {"DEL", "gone"}, true),
                            Frame(kFirst + 4, {"SADD", "gone", "x"}, false),
                        })
                  .has_value());

  EXPECT_FALSE(Resident("tail")) << "built a key from part of its history";
  EXPECT_TRUE(Resident("whole"));
  EXPECT_EQ(hot_->Exec(core::ops::SetCard{.key = "whole"})->AsInteger(), 2);
  EXPECT_EQ(hot_->Exec(core::ops::SetCard{.key = "gone"})->AsInteger(), 1)
      << "a tombstone is the key's whole state";
  EXPECT_EQ(replayer_->Skipped(), 1U);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kRecoveryHotSkippedFramesTotal), 1.0);
  EXPECT_EQ(replayer_->Replayed(), 5U);
}

// A skipped frame still raises its shard's stamp, so the sequencer
// never stamps a later write below a logged one.
TEST_F(HotReplayerTest, ASkippedFrameStillRaisesItsShardsStamp) {
  Build();
  ASSERT_TRUE(ReplayAll({Frame(kFirst, {"SADD", "tail", "c"}, false, kT0 + 5000)}).has_value());
  ASSERT_EQ(replayer_->Skipped(), 1U);
  auto locks = hot_->LockExclusive(std::vector<core::ShardId>{0});
  EXPECT_EQ(locks.LastAppendedAt(0), At(kT0 + 5000));
}

// A skipped write made the key's stub stale; the stub goes with it.
TEST_F(HotReplayerTest, ASkippedWriteDropsItsKeysStub) {
  Build();
  ASSERT_TRUE(
      ReplayAll({Frame(kFirst, {"SET", "k", "v", "PXAT", std::to_string(kT0 + 10'000)}, true)},
                kFirst + 2)
          .has_value());
  hot_->EvictExpired(core::SteadyTime{std::chrono::hours{1'000'000}});
  ASSERT_TRUE(hot_->FindStub("k").has_value()) << "evicting k left no stub";

  std::vector<core::QueueEntry> batch{Frame(kFirst + 1, {"PERSIST", "k"}, false)};
  ASSERT_TRUE(replayer_->Apply(0, batch).has_value());
  EXPECT_FALSE(hot_->FindStub("k").has_value()) << "a stub kept the TTL PERSIST removed";
  EXPECT_FALSE(Resident("k"));
}

// A Flush wipes the shard at its seq: no earlier key survives, stubs
// included, and the flush floor is set.
TEST_F(HotReplayerTest, AFlushLeavesNothingFromBeforeIt) {
  Build();
  ASSERT_TRUE(ReplayAll({Frame(kFirst, {"SET", "a", "v"}, true),
                         Frame(kFirst + 1, {"SET", "b", "v"}, true)},
                        kFirst + 4)
                  .has_value());
  hot_->EvictExpired(core::SteadyTime{std::chrono::hours{1'000'000}});
  ASSERT_TRUE(hot_->FindStub("a").has_value());

  std::vector<core::QueueEntry> batch{FlushFrame(kFirst + 2),
                                      Frame(kFirst + 3, {"SET", "c", "v"}, true)};
  ASSERT_TRUE(replayer_->Apply(0, batch).has_value());
  EXPECT_FALSE(hot_->FindStub("a").has_value());
  EXPECT_FALSE(Resident("a"));
  EXPECT_FALSE(Resident("b"));
  EXPECT_TRUE(Resident("c"));
  horizon_.store(kFirst);
  EXPECT_TRUE(hot_->KnownAbsentAfterFlush("a")) << "no flush floor";
}

// Frames must arrive in order and without gaps.
TEST_F(HotReplayerTest, AFrameOutOfPlaceFailsReplay) {
  Build();
  auto applied = ReplayAll(
      {Frame(kFirst, {"SET", "a", "v"}, true), Frame(kFirst + 2, {"SET", "b", "v"}, true)},
      kFirst + 3);
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kInternal);
  EXPECT_NE(applied.error().message().find("at its cursor"), std::string::npos);
}

// Replay must apply or skip exactly every frame below each shard's end.
TEST_F(HotReplayerTest, FinishShortOfAShardsEndFailsReplay) {
  Build();
  ASSERT_TRUE(ReplayAll({Frame(kFirst, {"SET", "a", "v"}, true)}, kFirst + 3).has_value());
  auto finished = replayer_->Finish();
  ASSERT_FALSE(finished.has_value());
  EXPECT_EQ(finished.error().code(), core::ErrorCode::kInternal);
  EXPECT_NE(finished.error().message().find("got 1 of its 3 frames"), std::string::npos)
      << finished.error().message();
}

// Replay reads no clock: written keys link at their appended_at. Only
// Finish reads the clocks, once each.
TEST_F(HotReplayerTest, ReplayReadsNoClock) {
  Build();
  ASSERT_TRUE(ReplayAll({
                            Frame(kFirst, {"SET", "s", "v", "PXAT", std::to_string(kT0 - 1)}, true),
                            Frame(kFirst + 1, {"SADD", "set", "a"}, true),
                            Frame(kFirst + 2, {"HSET", "h", "f", "v"}, true),
                            Frame(kFirst + 3, {"ZADD", "z", "1", "m"}, true),
                            Frame(kFirst + 4, {"PEXPIREAT", "set", std::to_string(kT0 - 1)}, false),
                            Frame(kFirst + 5, {"DEL", "h"}, true),
                        })
                  .has_value());
  EXPECT_EQ(hot_clocks_.steady_reads.load(), 0);
  EXPECT_EQ(hot_clocks_.wall_reads.load(), 0);
  EXPECT_EQ(clocks_.steady_reads.load(), 0);
  EXPECT_EQ(clocks_.wall_reads.load(), 0);
  EXPECT_TRUE(Resident("s")) << "replay judged a TTL";

  ASSERT_TRUE(replayer_->Finish().has_value());
  EXPECT_EQ(clocks_.steady_reads.load(), 1);
  EXPECT_EQ(clocks_.wall_reads.load(), 1);
}

// The sweep judges each key by when it was written: a key written more
// than its eviction ago is evicted, a recent one stays.
TEST_F(HotReplayerTest, TheSweepEvictsByWriteTimeNotReplayTime) {
  Build();
  clocks_.wall_now = At(kT0 + kTwoHours);
  ASSERT_TRUE(
      ReplayAll({
                    Frame(kFirst, {"SET", "old", "v"}, true, kT0),
                    Frame(kFirst + 1, {"SET", "recent", "v"}, true, kT0 + kTwoHours - 60'000),
                })
          .has_value());
  ASSERT_TRUE(Resident("old"));
  ASSERT_TRUE(replayer_->Finish().has_value());
  EXPECT_FALSE(Resident("old")) << "a key past its eviction stayed hot for another window";
  EXPECT_TRUE(Resident("recent"));
  ASSERT_TRUE(hot_->FindStub("old").has_value()) << "evicted, not expired";
}

// Not even the sweep evicts what cold has not drained.
TEST_F(HotReplayerTest, TheSweepNeverEvictsAnUndrainedKey) {
  Build();
  clocks_.wall_now = At(kT0 + kTwoHours);
  ASSERT_TRUE(ReplayAll({Frame(kFirst, {"SET", "old", "v"}, true, kT0)}).has_value());
  horizon_.store(0);
  ASSERT_TRUE(replayer_->Finish().has_value());
  EXPECT_TRUE(Resident("old"));
}

// Replay may exceed the budget only within the backpressure ratio. At
// the ratio with nothing evictable, the replayer makes cold drain
// through the frame, rather than wait for a drain that only replay
// drives, then evicts.
TEST_F(HotReplayerTest, AtTheRatioWithNothingEvictableColdIsMadeToDrain) {
  const size_t entry = EntryBytes();
  Build(20 * entry);
  horizon_.store(0);
  std::vector<core::QueueEntry> frames = Sets(kFirst, 200);
  const std::vector<core::SequenceId> from{kFirst};
  const std::vector<core::SequenceId> end{kFirst + frames.size()};
  replayer_->Begin(from, end);
  uint64_t peak = 0;
  for (auto& frame : frames) {
    std::vector<core::QueueEntry> one{std::move(frame)};
    ASSERT_TRUE(replayer_->Apply(0, one).has_value());
    peak = std::max(peak, hot_->Stats()->used_bytes);
  }

  EXPECT_LE(static_cast<double>(peak), (20.0 * static_cast<double>(entry) * 1.25) + entry)
      << "replay passed the backpressure ratio";
  ASSERT_GT(replayer_->DrainRequests(), 0U);
  EXPECT_EQ(drains_.size(), replayer_->DrainRequests());
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kRecoveryColdDrainRequestsTotal),
            static_cast<double>(replayer_->DrainRequests()));
  for (size_t i = 1; i < drains_.size(); ++i) {
    EXPECT_GT(drains_[i].second, drains_[i - 1].second) << "a drain asked for nothing new";
  }
  // Cold's FinishReplay drains it all; the sweep then reaches the budget.
  horizon_.store(hot::kAllDrained);
  ASSERT_TRUE(replayer_->Finish().has_value());
  EXPECT_LE(hot_->Stats()->used_bytes, 20 * entry);
}

// A drain cold cannot make fails recovery, loudly.
TEST_F(HotReplayerTest, ADrainThatFailsFailsReplay) {
  Build(4096);
  horizon_.store(0);
  drain_result_ = std::unexpected(core::Error{core::ErrorCode::kTimeout, "cold stuck (test)"});
  std::vector<core::QueueEntry> frames;
  frames.reserve(100);
  for (core::SequenceId i = 0; i < 100; ++i) {
    frames.push_back(
        Frame(kFirst + i, {"SET", "k" + std::to_string(i), std::string(200, 'x')}, true));
  }
  auto applied = ReplayAll(std::move(frames));
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kTimeout);
  EXPECT_NE(applied.error().message().find("cold could not drain"), std::string::npos);
  EXPECT_EQ(replayer_->DrainRequests(), 1U);
}

// When even a drain leaves nothing to evict, replay carries on over
// the limit without asking again in that batch. Cold absorbs each
// batch before hot replays it, so the next batch asks again.
TEST_F(HotReplayerTest, StillOverAfterADrainAsksAgainOnlyInTheNextBatch) {
  const size_t entry = EntryBytes();
  Build(20 * entry);
  horizon_.store(0);
  drain_advances_ = false;
  const std::vector<core::SequenceId> from{kFirst};
  const std::vector<core::SequenceId> end{kFirst + 45};
  replayer_->Begin(from, end);
  auto first = Sets(kFirst, 40);
  ASSERT_TRUE(replayer_->Apply(0, first).has_value());
  EXPECT_EQ(replayer_->DrainRequests(), 1U) << "asked again within a batch";
  auto second = Sets(kFirst + 40, 5);
  ASSERT_TRUE(replayer_->Apply(0, second).has_value());
  EXPECT_EQ(replayer_->DrainRequests(), 2U) << "a new batch never asked";
  EXPECT_EQ(replayer_->Replayed(), 45U);
}

// A drained seq that never advances, as an entry cold cannot parse
// pins it, leaves replay nothing to evict. Replay fails, naming the
// cause, at the first frame past twice the backpressure limit.
TEST_F(HotReplayerTest, APinnedDrainFailsReplayPastTwiceTheLimit) {
  const size_t entry = EntryBytes();
  Build(20 * entry);
  horizon_.store(0);
  drain_advances_ = false;
  const uint64_t ceiling = uint64_t{2} * 25 * entry;
  constexpr size_t kFrames = 80;
  constexpr size_t kBatch = 4;
  const std::vector<core::SequenceId> from{kFirst};
  const std::vector<core::SequenceId> end{kFirst + kFrames};
  replayer_->Begin(from, end);
  core::Result<void> applied;
  for (size_t i = 0; i < kFrames && applied.has_value(); i += kBatch) {
    auto batch = Sets(kFirst + i, kBatch);
    applied = replayer_->Apply(0, batch);
    if (applied.has_value()) {
      EXPECT_LE(hot_->Stats()->used_bytes, ceiling) << "replay passed the ceiling";
    }
  }

  ASSERT_FALSE(applied.has_value()) << "replay grew without bound";
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kResourceExhausted);
  const std::string& message = applied.error().message();
  const core::SequenceId failed_at = kFirst + replayer_->Replayed() - 1;
  EXPECT_NE(message.find("shard 0 "), std::string::npos) << message;
  EXPECT_NE(message.find("at seq " + std::to_string(failed_at) + " "), std::string::npos)
      << message;
  EXPECT_NE(message.find("drained seq is pinned"), std::string::npos) << message;
  EXPECT_NE(message.find("larger than the shard's budget"), std::string::npos) << message;
  const uint64_t used = hot_->Stats()->used_bytes;
  EXPECT_GT(used, ceiling);
  EXPECT_LE(used, ceiling + entry) << "failed later than the first frame past it";
}

}  // namespace
}  // namespace abyss::engine
