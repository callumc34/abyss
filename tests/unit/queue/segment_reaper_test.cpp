#include "abyss/queue/segment_reaper.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/memory_offset_store.h"
#include "abyss/queue/segment_registry.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

// Logs of sealed segments, oldest first. Like the WAL, it reclaims only
// a log's oldest segment.
class FakeRegistry : public SegmentRegistry {
 public:
  explicit FakeRegistry(uint32_t logs = 1) : logs_(logs) {}

  void Add(SealedSegmentInfo info) {
    const std::scoped_lock lock(mu_);
    logs_.at(info.log).push_back(std::move(info));
  }

  // Makes RemoveSegment fail for one segment, as an I/O error would.
  void FailRemoval(uint32_t log, uint64_t ordinal) {
    const std::scoped_lock lock(mu_);
    unremovable_.insert({log, ordinal});
  }

  uint32_t LogCount() const override { return static_cast<uint32_t>(logs_.size()); }

  std::vector<SealedSegmentInfo> ListSealedSegments(uint32_t log,
                                                    std::size_t max_count) const override {
    const std::scoped_lock lock(mu_);
    const auto& segments = logs_.at(log);
    const auto count = static_cast<std::ptrdiff_t>(std::min(max_count, segments.size()));
    return {segments.begin(), segments.begin() + count};
  }

  core::Result<void> RemoveSegment(uint32_t log, uint64_t ordinal) override {
    const std::scoped_lock lock(mu_);
    auto& segments = logs_.at(log);
    if (segments.empty() || segments.front().ordinal != ordinal) {
      return std::unexpected(
          core::Error{core::ErrorCode::kFailedPrecondition, "not the oldest segment"});
    }
    if (unremovable_.contains({log, ordinal})) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInternal, "remove segment: permission denied"});
    }
    segments.erase(segments.begin());
    removed_.insert({log, ordinal});
    return {};
  }

  size_t segment_count(uint32_t log = 0) const {
    const std::scoped_lock lock(mu_);
    return logs_.at(log).size();
  }

  bool was_removed(uint32_t log, uint64_t ordinal) const {
    const std::scoped_lock lock(mu_);
    return removed_.contains({log, ordinal});
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::vector<SealedSegmentInfo>> logs_;
  std::set<std::pair<uint32_t, uint64_t>> removed_;
  std::set<std::pair<uint32_t, uint64_t>> unremovable_;
};

SegmentRegistry::SealedSegmentInfo MakeInfo(uint32_t log, uint64_t ordinal,
                                            std::vector<SegmentShardRange> shards,
                                            core::WallTime sealed_at) {
  return {.log = log, .ordinal = ordinal, .shards = std::move(shards), .sealed_at = sealed_at};
}

SegmentShardRange Range(core::ShardId shard, core::SequenceId min_seq, core::SequenceId max_seq) {
  return {.shard = shard, .min_seq = min_seq, .max_seq = max_seq};
}

core::QueueEntry MakeWrite(std::vector<std::string> args) {
  core::QueueEntry entry;
  entry.appended_at = core::WallClock::now();
  entry.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return entry;
}

SegmentReaperConfig Config(std::vector<core::ConsumerId> consumers, std::chrono::seconds retention,
                           core::WallTime now) {
  return {.consumers = std::move(consumers), .min_retention = retention, .wall_clock = [now] {
            return now;
          }};
}

TEST(SegmentReaperTest, DeletesSegmentReleasedByAllConsumers) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 99)}, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 99).has_value());
  ASSERT_TRUE(offsets.Set(1, 0, 99).has_value());

  SegmentReaper reaper(registry, offsets, Config({0, 1}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 1U);
  EXPECT_EQ(result->failed, 0U);
  EXPECT_FALSE(result->oldest_eligible_unreaped.has_value());
  EXPECT_EQ(registry.segment_count(), 0U);
  EXPECT_TRUE(registry.was_removed(0, 0));
}

TEST(SegmentReaperTest, SkipsSegmentNotYetReleased) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 100)}, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 50).has_value());
  ASSERT_TRUE(offsets.Set(1, 0, 100).has_value());

  SegmentReaper reaper(registry, offsets, Config({0, 1}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

TEST(SegmentReaperTest, EveryShardTheSegmentHoldsMustBeReleased) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 50), Range(1, 0, 50)}, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 50).has_value());
  ASSERT_TRUE(offsets.Set(0, 1, 10).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 1s, now));
  auto held = reaper.RunOnce();
  ASSERT_TRUE(held.has_value());
  EXPECT_EQ(held->deleted, 0U);

  ASSERT_TRUE(offsets.Set(0, 1, 50).has_value());
  auto released = reaper.RunOnce();
  ASSERT_TRUE(released.has_value());
  EXPECT_EQ(released->deleted, 1U);
}

TEST(SegmentReaperTest, SkipsSegmentNotYetMinRetention) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 99)}, now - 30s));
  ASSERT_TRUE(offsets.Set(0, 0, 100).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 60s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_FALSE(result->oldest_eligible_unreaped.has_value());
  EXPECT_EQ(registry.segment_count(), 1U);
}

// min_retention runs from the seal: a spare that sat idle before it was
// written to gains no age from that.
TEST(SegmentReaperTest, RetentionAgesASegmentFromItsSeal) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto sealed_at = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 9)}, sealed_at));
  ASSERT_TRUE(offsets.Set(0, 0, 9).has_value());

  SegmentReaper early(registry, offsets, Config({0}, 60s, sealed_at + 60s - 1ms));
  auto kept = early.RunOnce();
  ASSERT_TRUE(kept.has_value());
  EXPECT_EQ(kept->deleted, 0U);
  EXPECT_FALSE(kept->oldest_eligible_unreaped.has_value()) << "not yet eligible";

  SegmentReaper due(registry, offsets, Config({0}, 60s, sealed_at + 60s));
  auto reclaimed = due.RunOnce();
  ASSERT_TRUE(reclaimed.has_value());
  EXPECT_EQ(reclaimed->deleted, 1U);
}

TEST(SegmentReaperTest, SkipsConsumerWithNoCommitAtAll) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 99)}, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 99).has_value());
  // Consumer 1 has no commit for shard 0, so the segment is retained.

  SegmentReaper reaper(registry, offsets, Config({0, 1}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

// Reclaiming segment 1 past a pinned segment 0 would leave a hole a
// rebuild from FirstSeq replays across, so nothing goes.
TEST(SegmentReaperTest, APinnedSegmentHoldsBackEveryLaterOne) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  const auto held_back_at = now - 48h;
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 9), Range(1, 0, 4)}, now - 72h));
  registry.Add(MakeInfo(0, 1, {Range(0, 10, 19)}, held_back_at));
  registry.Add(MakeInfo(0, 2, {Range(0, 20, 29)}, now - 24h));
  ASSERT_TRUE(offsets.Set(0, 0, 29).has_value());
  ASSERT_TRUE(offsets.Set(0, 1, 3).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(result->failed, 0U);
  EXPECT_EQ(registry.segment_count(), 3U);
  EXPECT_EQ(result->oldest_eligible_unreaped, std::optional{held_back_at});

  ASSERT_TRUE(offsets.Set(0, 1, 4).has_value());
  auto released = reaper.RunOnce();
  ASSERT_TRUE(released.has_value());
  EXPECT_EQ(released->deleted, 3U);
  EXPECT_FALSE(released->oldest_eligible_unreaped.has_value());
}

TEST(SegmentReaperTest, EmptyConsumerListRetainsAll) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 99)}, now - 48h));

  SegmentReaper reaper(registry, offsets, Config({}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
}

TEST(SegmentReaperTest, RunOnceMulticall) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 99)}, now - 48h));
  registry.Add(MakeInfo(0, 1, {Range(0, 100, 199)}, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 150).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 1s, now));
  auto first = reaper.RunOnce();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->deleted, 1U);
  EXPECT_EQ(registry.segment_count(), 1U);

  // A second pass is a no-op until offsets advance.
  auto second = reaper.RunOnce();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

TEST(SegmentReaperTest, AFailedRemovalStopsTheSweepOfItsLog) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  const auto stuck_sealed_at = now - 72h;
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 99)}, stuck_sealed_at));
  registry.Add(MakeInfo(0, 1, {Range(0, 100, 199)}, now - 48h));
  registry.FailRemoval(0, 0);
  ASSERT_TRUE(offsets.Set(0, 0, 199).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(result->failed, 1U);
  ASSERT_TRUE(result->first_error.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(result->first_error->code(), core::ErrorCode::kInternal);
  EXPECT_EQ(result->oldest_eligible_unreaped, std::optional{stuck_sealed_at});
  EXPECT_EQ(registry.segment_count(), 2U);
}

TEST(SegmentReaperTest, LogsAreSweptIndependently) {
  FakeRegistry registry(2);   // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, {Range(0, 0, 9)}, now - 48h));
  registry.Add(MakeInfo(1, 0, {Range(1, 0, 9)}, now - 48h));
  registry.Add(MakeInfo(1, 1, {Range(1, 10, 19)}, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 5).has_value());
  ASSERT_TRUE(offsets.Set(0, 1, 19).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 2U);
  EXPECT_EQ(registry.segment_count(0), 1U);
  EXPECT_EQ(registry.segment_count(1), 0U);
}

TEST(SegmentReaperTest, OneSweepReclaimsMoreThanABatch) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)
  const auto now = std::chrono::system_clock::now();
  constexpr uint64_t kSegments = 200;
  for (uint64_t ordinal = 0; ordinal < kSegments; ++ordinal) {
    registry.Add(MakeInfo(0, ordinal, {Range(0, ordinal, ordinal)}, now - 48h));
  }
  ASSERT_TRUE(offsets.Set(0, 0, kSegments).has_value());

  SegmentReaper reaper(registry, offsets, Config({0}, 1s, now));
  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, kSegments);
  EXPECT_EQ(registry.segment_count(), 0U);
}

// The reaper's failure path must reach an operator: a scraped counter
// plus the age of the oldest segment retention could not reclaim. The
// log keeps retrying the file, and later sweeps go on once it is gone.
TEST(WalQueueReaperTest, ReaperFailureSurfacedAsMetricAndRetried) {
  metrics::testing::Reset();
  const abyss::testing::TempDir dir("wal_reaper");
  auto opened = WalQueue::Open(WalConfig{
      .wal_path = dir.String(),
      .segment_size_bytes = 8192,
      .shard_count = 1,
      .min_retention = 0s,
      .retention_consumers = {0},
  });
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  auto queue = std::move(*opened);

  const std::string value(256, 'v');
  for (int i = 0; i < 100; ++i) {
    auto appended = queue->Append(0, MakeWrite({"SET", "k" + std::to_string(i), value}));
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
  }
  const auto tail = queue->TailSeq(0);
  ASSERT_TRUE(tail.has_value()) << tail.error().message();
  auto durable = queue->AwaitDurable(0, *tail, core::Durability::kPowerLoss, 5s);
  ASSERT_TRUE(durable.has_value() && *durable);
  const std::size_t sealed = queue->ListSealedSegments().size();
  ASSERT_GE(sealed, 2U);
  auto committed = queue->CommitOffset(0, 0, *tail);
  ASSERT_TRUE(committed.has_value()) << committed.error().message();

  // Retention honours the commit once both checkpoint slots hold it;
  // the second persist's sweep is the one that reclaims.
  ASSERT_TRUE(queue->FlushOffsets().has_value());
  ASSERT_EQ(queue->ListSealedSegments().size(), sealed);
  queue->InjectSegmentRemoveErrorForTesting(
      0, core::Error{core::ErrorCode::kInternal, "injected remove failure"});
  auto flushed = queue->FlushOffsets();
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();

  EXPECT_GT(queue->ReaperFailures(), 0U);
  const auto failures =
      metrics::testing::GetCounterValue(metrics::names::kQueueReaperFailuresTotal);
  EXPECT_GT(failures.value_or(0.0), 0.0);
  EXPECT_TRUE(queue->OldestEligibleUnreapedAge().has_value());
  // The failed segment has left the readable log; nothing after it
  // went.
  EXPECT_EQ(queue->ListSealedSegments().size(), sealed - 1);
  const auto first = queue->FirstSeq(0);
  ASSERT_TRUE(first.has_value());
  EXPECT_GT(*first, 0U);

  for (int i = 0; i < 250 && !queue->ListSealedSegments().empty(); ++i) {
    ASSERT_TRUE(queue->FlushOffsets().has_value());
    std::this_thread::sleep_for(20ms);
  }
  EXPECT_TRUE(queue->ListSealedSegments().empty()) << "the stuck reclaim was never retried";
  EXPECT_FALSE(queue->OldestEligibleUnreapedAge().has_value());
  const auto all_gone = queue->FirstSeq(0);
  ASSERT_TRUE(all_gone.has_value());
  auto read = queue->Read(0, *all_gone, 1000, 0ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  ASSERT_FALSE(read->empty());
  EXPECT_EQ(read->back().seq, *tail);
}

}  // namespace
}  // namespace abyss::queue
