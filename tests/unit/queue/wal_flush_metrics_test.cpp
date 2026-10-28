#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"
#include "queue_append.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;
using abyss::testing::Append;
using abyss::testing::AppendBatch;
using abyss::testing::WaitAcked;

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

// An append covers one WAL entry; a batch covers all of its own.
// Each append is awaited, so each flush covers exactly that append.
TEST_F(WalFlushMetricsTest, FlushCountsWalEntriesNotSubmits) {
  const abyss::testing::TempDir dir("wal_flush_metrics");
  auto opened = WalQueue::Open(WalConfig{
      .wal_path = dir.String(),
      .shard_count = 1,
      .durability = core::Durability::kPowerLoss,
      .min_retention = 0s,
  });
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  auto& queue = **opened;

  const std::vector<core::QueueEntry> batch{SetEntry("a"), SetEntry("b"), SetEntry("c")};
  auto batched = AppendBatch(queue, 0, batch);
  ASSERT_TRUE(batched.has_value());
  ASSERT_TRUE(WaitAcked(queue, 0, batched->last).has_value());
  auto single = Append(queue, 0, SetEntry("d"));
  ASSERT_TRUE(single.has_value());
  ASSERT_TRUE(WaitAcked(queue, 0, *single).has_value());

  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kWalFlushBatchEntries), 2U);
  EXPECT_EQ(metrics::testing::GetHistogramSum(metrics::names::kWalFlushBatchEntries), 4.0);
}

// Flushes per write divides by this counter, so it counts every entry.
TEST_F(WalFlushMetricsTest, AppendedCountsEveryEntry) {
  const abyss::testing::TempDir dir("wal_appended_metric");
  auto opened = WalQueue::Open(WalConfig{
      .wal_path = dir.String(),
      .shard_count = 2,
      .min_retention = 0s,
  });
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  auto& queue = **opened;

  const std::vector<core::QueueEntry> batch{SetEntry("a"), SetEntry("b"), SetEntry("c")};
  ASSERT_TRUE(AppendBatch(queue, 0, batch).has_value());
  ASSERT_TRUE(Append(queue, 1, SetEntry("d")).has_value());

  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kQueueAppendedTotal),
            std::optional<double>{4.0});
}

}  // namespace
}  // namespace abyss::queue
