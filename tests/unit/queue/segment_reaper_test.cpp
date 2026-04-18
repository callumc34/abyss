#include "abyss/queue/segment_reaper.h"

#include <gtest/gtest.h>

#include <chrono>
#include <mutex>
#include <set>
#include <vector>

#include "abyss/queue/memory_offset_store.h"
#include "abyss/queue/segment_registry.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

class FakeRegistry : public SegmentRegistry {
 public:
  void Add(SealedSegmentInfo info) {
    std::lock_guard lock(mu_);
    segments_.push_back(std::move(info));
  }

  std::vector<SealedSegmentInfo> ListSealedSegments() const override {
    std::lock_guard lock(mu_);
    return segments_;
  }

  core::Result<void> RemoveSegment(core::ShardId shard, core::SequenceId base_seq) override {
    std::lock_guard lock(mu_);
    std::erase_if(segments_, [&](const SealedSegmentInfo& s) {
      return s.shard == shard && s.base_seq == base_seq;
    });
    removed_.insert({shard, base_seq});
    return {};
  }

  size_t segment_count() const {
    std::lock_guard lock(mu_);
    return segments_.size();
  }

  bool was_removed(core::ShardId shard, core::SequenceId base_seq) const {
    std::lock_guard lock(mu_);
    return removed_.contains({shard, base_seq});
  }

 private:
  mutable std::mutex mu_;
  std::vector<SealedSegmentInfo> segments_;
  std::set<std::pair<core::ShardId, core::SequenceId>> removed_;
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
  EXPECT_EQ(*result, 1U);
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
  EXPECT_EQ(*result, 0U);
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
  EXPECT_EQ(*result, 0U);
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
  EXPECT_EQ(*result, 0U);
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
  EXPECT_EQ(*result, 1U);
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
  EXPECT_EQ(*result, 0U);
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
  EXPECT_EQ(*first, 1U);
  EXPECT_EQ(registry.segment_count(), 1U);

  // Second pass is a no-op until offsets advance.
  auto second = reaper.RunOnce();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*second, 0U);
  EXPECT_EQ(registry.segment_count(), 1U);
}

}  // namespace
}  // namespace abyss::queue
