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
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "hot_bytes.h"

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

// The bytes each key of Sets takes in hot, past its map's buckets.
size_t EntryBytes() {
  return static_cast<size_t>(abyss::testing::StringEntryBytes("k10000", std::string(200, 'x')));
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

  void Build(size_t max_memory_bytes = size_t{64} << 20, uint32_t shards = 1) {
    hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = max_memory_bytes,
        .shard_count = shards,
        // Sized for 64 keys, so its buckets never grow mid-replay.
        .reserve_bytes_per_key = std::max<size_t>(max_memory_bytes / 64, 1),
        .drained = [this](core::ShardId) { return horizon_.load(); },
        .eviction_policy = &policy_,
        .steady_clock = hot_clocks_.Steady(),
        .wall_clock = hot_clocks_.Wall(),
    });
    replayer_ = std::make_unique<HotReplayer>(
        *hot_, HotReplayer::Config{.steady_clock = clocks_.Steady(), .wall_clock = clocks_.Wall()});
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
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{hot_->ShardOf(key)});
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

// A Write logged on another key's stream is corrupt: replay refuses
// it rather than give the key that stream's seq.
TEST_F(HotReplayerTest, AWriteOnAnotherShardsStreamFailsReplay) {
  Build(size_t{64} << 20, 2);
  std::string key = "k";
  while (hot_->ShardOf(key) != 1) key += "k";
  const std::vector<core::SequenceId> from{kFirst, kFirst};
  const std::vector<core::SequenceId> end{kFirst + 1, kFirst};
  replayer_->Begin(from, end);
  std::vector<core::QueueEntry> batch{Frame(kFirst, {"SET", key, "v"}, true)};

  const auto applied = replayer_->Apply(0, batch);
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kCorruption);
  const std::string& message = applied.error().message();
  EXPECT_NE(message.find("seq " + std::to_string(kFirst) + " on shard 0 "), std::string::npos)
      << message;
  EXPECT_NE(message.find("key_hash " + std::to_string(log::KeyHash(key))), std::string::npos)
      << message;
  EXPECT_FALSE(Resident(key));
  EXPECT_EQ(replayer_->Replayed(), 0U);
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

// Replay may exceed the budget only within the backpressure ratio: at
// the ratio it evicts what cold has drained, which cold, fed each
// batch first, has drained through the batch.
TEST_F(HotReplayerTest, AtTheRatioReplayEvictsWhatColdHasDrained) {
  const size_t entry = EntryBytes();
  Build(20 * entry);
  std::vector<core::QueueEntry> frames = Sets(kFirst, 200);
  const std::vector<core::SequenceId> from{kFirst};
  const std::vector<core::SequenceId> end{kFirst + frames.size()};
  replayer_->Begin(from, end);
  uint64_t peak = 0;
  for (auto& frame : frames) {
    horizon_.store(frame.seq);
    std::vector<core::QueueEntry> one{std::move(frame)};
    ASSERT_TRUE(replayer_->Apply(0, one).has_value());
    peak = std::max(peak, hot_->Stats()->used_bytes);
  }

  EXPECT_LE(static_cast<double>(peak), (20.0 * static_cast<double>(entry) * 1.25) + entry)
      << "replay passed the backpressure ratio";
  ASSERT_TRUE(replayer_->Finish().has_value());
  EXPECT_LE(hot_->Stats()->used_bytes, 20 * entry);
}

// Over the limit with nothing evictable, replay carries on and tries
// again in the next batch, by when cold may have drained further.
TEST_F(HotReplayerTest, StillOverWithNothingEvictableEvictsInTheNextBatch) {
  const size_t entry = EntryBytes();
  Build(20 * entry);
  horizon_.store(0);
  const std::vector<core::SequenceId> from{kFirst};
  const std::vector<core::SequenceId> end{kFirst + 45};
  replayer_->Begin(from, end);
  auto first = Sets(kFirst, 40);
  ASSERT_TRUE(replayer_->Apply(0, first).has_value());
  EXPECT_GT(hot_->Stats()->used_bytes, 25 * entry) << "evicted what cold had not drained";
  horizon_.store(kFirst + 44);
  auto second = Sets(kFirst + 40, 5);
  ASSERT_TRUE(replayer_->Apply(0, second).has_value());
  EXPECT_LE(static_cast<double>(hot_->Stats()->used_bytes),
            (20.0 * static_cast<double>(entry) * 1.25) + entry)
      << "the next batch never evicted";
  EXPECT_EQ(replayer_->Replayed(), 45U);
}

// A drained seq that never advances, as an entry cold cannot parse
// pins it, leaves replay nothing to evict. Replay fails, naming the
// cause, at the first frame past twice the backpressure limit.
TEST_F(HotReplayerTest, APinnedDrainedSeqFailsReplayPastTwiceTheLimit) {
  const size_t entry = EntryBytes();
  Build(20 * entry);
  horizon_.store(0);
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
