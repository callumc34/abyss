#include "abyss/queue/segment_reaper.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/memory_offset_store.h"
#include "abyss/queue/segment_registry.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

class FakeRegistry : public SegmentRegistry {
 public:
  void Add(SealedSegmentInfo info) {
    const std::scoped_lock lock(mu_);
    segments_.push_back(std::move(info));
  }

  // Makes RemoveSegment fail for one segment, as an unlink EACCES/EIO would.
  void FailRemoval(core::ShardId shard, core::SequenceId base_seq) {
    const std::scoped_lock lock(mu_);
    unremovable_.insert({shard, base_seq});
  }

  std::vector<SealedSegmentInfo> ListSealedSegments() const override {
    const std::scoped_lock lock(mu_);
    return segments_;
  }

  core::Result<void> RemoveSegment(core::ShardId shard, core::SequenceId base_seq) override {
    const std::scoped_lock lock(mu_);
    if (unremovable_.contains({shard, base_seq})) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInternal, "unlink segment: permission denied"});
    }
    std::erase_if(segments_, [&](const SealedSegmentInfo& s) {
      return s.shard == shard && s.base_seq == base_seq;
    });
    removed_.insert({shard, base_seq});
    return {};
  }

  size_t segment_count() const {
    const std::scoped_lock lock(mu_);
    return segments_.size();
  }

  bool was_removed(core::ShardId shard, core::SequenceId base_seq) const {
    const std::scoped_lock lock(mu_);
    return removed_.contains({shard, base_seq});
  }

 private:
  mutable std::mutex mu_;
  std::vector<SealedSegmentInfo> segments_;
  std::set<std::pair<core::ShardId, core::SequenceId>> removed_;
  std::set<std::pair<core::ShardId, core::SequenceId>> unremovable_;
};

SegmentRegistry::SealedSegmentInfo MakeInfo(core::ShardId shard, core::SequenceId base_seq,
                                            core::SequenceId last_seq, core::WallTime created_at) {
  return {
      .path = "/dev/null",
      .shard = shard,
      .base_seq = base_seq,
      .last_seq = last_seq,
      .created_at = created_at,
  };
}

core::QueueEntry MakeWrite(std::vector<std::string> args) {
  core::QueueEntry entry;
  entry.appended_at = core::WallClock::now();
  entry.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return entry;
}

TEST(SegmentReaperTest, DeletesSegmentAckedByAllConsumers) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 99, now - 48h));

  ASSERT_TRUE(offsets.Set(0, 0, 99).has_value());
  ASSERT_TRUE(offsets.Set(1, 0, 99).has_value());

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0, 1}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 1U);
  EXPECT_EQ(result->failed, 0U);
  EXPECT_FALSE(result->oldest_eligible_unreaped.has_value());
  EXPECT_EQ(registry.segment_count(), 0U);
  EXPECT_TRUE(registry.was_removed(0, 0));
}

TEST(SegmentReaperTest, SkipsSegmentNotYetAcked) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 100, now - 48h));

  ASSERT_TRUE(offsets.Set(0, 0, 50).has_value());
  ASSERT_TRUE(offsets.Set(1, 0, 100).has_value());

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0, 1}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

TEST(SegmentReaperTest, SkipsSegmentNotYetMinRetention) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 99, now - 30s));

  ASSERT_TRUE(offsets.Set(0, 0, 100).has_value());

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0}, .min_retention = 60s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

TEST(SegmentReaperTest, SkipsConsumerWithNoAckAtAll) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 99, now - 48h));

  ASSERT_TRUE(offsets.Set(0, 0, 99).has_value());
  // consumer 1 has no ack for shard 0 — should retain.

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0, 1}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

TEST(SegmentReaperTest, MultiShardIndependent) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 50, now - 48h));
  registry.Add(MakeInfo(1, 0, 50, now - 48h));

  ASSERT_TRUE(offsets.Set(0, 0, 50).has_value());
  ASSERT_TRUE(offsets.Set(0, 1, 10).has_value());  // shard 1 not caught up

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 1U);
  EXPECT_TRUE(registry.was_removed(0, 0));
  EXPECT_FALSE(registry.was_removed(1, 0));
}

TEST(SegmentReaperTest, EmptyConsumerListRetainsAll) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 99, now - 48h));

  SegmentReaper reaper(registry, offsets,
                       {.consumers = {}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->deleted, 0U);
}

TEST(SegmentReaperTest, RunOnceMulticall) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  registry.Add(MakeInfo(0, 0, 99, now - 48h));
  registry.Add(MakeInfo(0, 100, 199, now - 48h));
  ASSERT_TRUE(offsets.Set(0, 0, 150).has_value());  // only first segment eligible.

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto first = reaper.RunOnce();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->deleted, 1U);
  EXPECT_EQ(registry.segment_count(), 1U);

  // Second pass is a no-op until offsets advance.
  auto second = reaper.RunOnce();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->deleted, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

TEST(SegmentReaperTest, ReaperContinuesPastUnremovableSegment) {
  FakeRegistry registry;      // NOLINT(misc-const-correctness)
  MemoryOffsetStore offsets;  // NOLINT(misc-const-correctness)

  const auto now = std::chrono::system_clock::now();
  const auto stuck_created_at = now - 72h;
  registry.Add(MakeInfo(0, 0, 99, stuck_created_at));  // swept first, cannot be unlinked
  registry.Add(MakeInfo(0, 100, 199, now - 48h));
  registry.Add(MakeInfo(0, 200, 299, now - 24h));
  registry.FailRemoval(0, 0);

  ASSERT_TRUE(offsets.Set(0, 0, 299).has_value());

  SegmentReaper reaper(
      registry, offsets,
      {.consumers = {0}, .min_retention = 1s, .wall_clock = [&]() { return now; }});

  auto result = reaper.RunOnce();
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->deleted, 2U);
  EXPECT_EQ(result->failed, 1U);
  ASSERT_TRUE(result->first_error.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(result->first_error->code(), core::ErrorCode::kInternal);
  EXPECT_EQ(result->oldest_eligible_unreaped, std::optional{stuck_created_at});

  EXPECT_TRUE(registry.was_removed(0, 100));
  EXPECT_TRUE(registry.was_removed(0, 200));
  EXPECT_FALSE(registry.was_removed(0, 0));
  EXPECT_EQ(registry.segment_count(), 1U);
}

// The reaper's failure path must reach an operator: a scraped counter plus the
// age of the oldest segment retention could not reclaim.
TEST(WalQueueReaperTest, ReaperFailureSurfacedAsMetric) {
#ifdef _WIN32
  GTEST_SKIP() << "delete-pending semantics prevent replacing an open segment file";
#endif
  metrics::testing::Reset();
  const abyss::testing::TempDir dir("wal_reaper");

  auto opened = WalQueue::Open(WalConfig{
      .wal_path = dir.String(),
      .segment_size_bytes = 4096,
      .shard_count = 1,
      .commit = {.policy = FsyncPolicy::kNone},
      .min_retention = 0s,
      .retention_consumers = {0},
  });
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  auto queue = std::move(*opened);

  const std::string value(256, 'v');
  for (int i = 0; i < 500 && queue->ListSealedSegments().size() < 2; ++i) {
    auto appended = queue->Append(0, MakeWrite({"SET", "k" + std::to_string(i), value}));
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
  }
  const auto sealed = queue->ListSealedSegments();
  ASSERT_GE(sealed.size(), 2U);

  // Replace the oldest sealed segment with a non-empty directory: unlink then
  // fails for that path alone, for any process privilege level.
  const std::filesystem::path stuck_path{sealed.front().path};
  ASSERT_TRUE(std::filesystem::remove(stuck_path));
  ASSERT_TRUE(std::filesystem::create_directory(stuck_path));
  ASSERT_TRUE(std::filesystem::create_directory(stuck_path / "blocker"));

  auto durable = queue->DurableSeq(0);
  ASSERT_TRUE(durable.has_value()) << durable.error().message();
  auto acked = queue->Ack(0, 0, *durable);
  ASSERT_TRUE(acked.has_value()) << acked.error().message();

  EXPECT_GT(queue->ReaperFailures(), 0U);
  const auto failures =
      metrics::testing::GetCounterValue(metrics::names::kQueueReaperFailuresTotal);
  EXPECT_GT(failures.value_or(0.0), 0.0);
  EXPECT_TRUE(queue->OldestEligibleUnreapedAge().has_value());

  // The sweep did not stop at the stuck segment.
  EXPECT_TRUE(std::filesystem::exists(stuck_path));
  EXPECT_FALSE(std::filesystem::exists(sealed[1].path));

  // QUEUE-8: a segment we failed to unlink must stay registered. Deregistering
  // it would leave the bytes on disk with nothing tracking them — never retried,
  // absent from retention stats, and the stuck-age signal would read healthy.
  const auto still_sealed = queue->ListSealedSegments();
  EXPECT_TRUE(std::ranges::any_of(still_sealed, [&](const auto& s) {
    return std::filesystem::path{s.path} == stuck_path;
  })) << "the WAL forgot a segment it failed to unlink";

  // A later sweep retries it, so the failure count keeps rising rather than
  // going quiet while the disk stays full.
  const uint64_t failures_before = queue->ReaperFailures();
  auto reacked = queue->Ack(0, 0, *durable);
  ASSERT_TRUE(reacked.has_value()) << reacked.error().message();
  EXPECT_GT(queue->ReaperFailures(), failures_before) << "stuck segment was never retried";
  EXPECT_TRUE(queue->OldestEligibleUnreapedAge().has_value());
}

}  // namespace
}  // namespace abyss::queue
