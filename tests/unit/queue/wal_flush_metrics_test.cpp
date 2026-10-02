#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

class WalFlushMetricsTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }
};

core::QueueEntry SetEntry(std::string key) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", std::move(key), "v"}}},
  };
}

// An Append covers one WAL entry; an AppendBatch covers all of its own.
TEST_F(WalFlushMetricsTest, FlushCountsWalEntriesNotSubmits) {
  const abyss::testing::TempDir dir("wal_flush_metrics");
  auto opened = WalQueue::Open(WalConfig{
      .wal_path = dir.String(),
      .shard_count = 1,
      .commit = {.policy = FsyncPolicy::kPerWrite},
      .min_retention = 0s,
  });
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  auto& queue = **opened;

  const std::vector<core::QueueEntry> batch{SetEntry("a"), SetEntry("b"), SetEntry("c")};
  auto batched = queue.AppendBatch(0, batch);
  ASSERT_TRUE(batched.has_value());
  ASSERT_TRUE(batched->durable.get().has_value());
  auto single = queue.Append(0, SetEntry("d"));
  ASSERT_TRUE(single.has_value());
  ASSERT_TRUE(single->durable.get().has_value());

  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kWalFlushBatchEntries), 2U);
  EXPECT_EQ(metrics::testing::GetHistogramSum(metrics::names::kWalFlushBatchEntries), 4.0);
}

}  // namespace
}  // namespace abyss::queue
