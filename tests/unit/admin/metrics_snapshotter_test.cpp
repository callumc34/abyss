#include "abyss/admin/metrics_snapshotter.h"

#include <gtest/gtest.h>

#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::admin {
namespace {

class FakeStatusProvider : public StatusProvider {
 public:
  StatusSnapshot Snapshot() const override { return snapshot; }

  StatusSnapshot snapshot;
};

class MetricsSnapshotterTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  FakeStatusProvider provider_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(MetricsSnapshotterTest, PublishesQueueDepthAndDiskBytes) {
  provider_.snapshot.queue.total_entries = 4321;
  provider_.snapshot.queue.total_bytes = 987654;

  MetricsSnapshotter snapshotter(provider_);
  snapshotter.Observe();

  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kQueueDepth), 4321.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kQueueDiskBytes), 987654.0);
}

TEST_F(MetricsSnapshotterTest, PublishesTierKeyCountsAndFootprints) {
  provider_.snapshot.hot.key_count = 11;
  provider_.snapshot.hot.memory_bytes = 22;
  provider_.snapshot.cold.key_count = 33;
  provider_.snapshot.cold.disk_bytes = 44;
  provider_.snapshot.cold.buffer.entries = 55;
  provider_.snapshot.cold.buffer.bytes = 66;

  MetricsSnapshotter snapshotter(provider_);
  snapshotter.Observe();

  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotKeys), 11.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotMemoryBytes), 22.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kColdKeys), 33.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kColdDiskBytes), 44.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kColdBufferEntries), 55.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kColdBufferBytes), 66.0);
}

// The gauge is in seconds while the status schema carries milliseconds; a unit
// mismatch here would silently misreport the cold-gap alarm by 1000x.
TEST_F(MetricsSnapshotterTest, ConvertsAgeMillisToSeconds) {
  provider_.snapshot.cold.buffer.oldest_entry_age_ms = 5500;
  provider_.snapshot.queue.oldest_eligible_unreaped_age_ms = 2000;

  MetricsSnapshotter snapshotter(provider_);
  snapshotter.Observe();

  EXPECT_DOUBLE_EQ(
      *metrics::testing::GetGaugeValue(metrics::names::kColdBufferOldestEntryAgeSeconds), 5.5);
  EXPECT_DOUBLE_EQ(
      *metrics::testing::GetGaugeValue(metrics::names::kQueueOldestEligibleUnreapedAgeSeconds),
      2.0);
}

TEST_F(MetricsSnapshotterTest, PublishesFleetReadBufferHighWater) {
  provider_.snapshot.connections.read_buffer_high_water_bytes = 5000;

  MetricsSnapshotter snapshotter(provider_);
  snapshotter.Observe();

  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kNetReadBufferHighWaterBytes), 5000.0);
}

// Successive ticks must track the live value rather than latching a maximum;
// a stale-high gauge would hide recovery just as badly as a stale-low one.
TEST_F(MetricsSnapshotterTest, ObserveTracksLatestValue) {
  MetricsSnapshotter snapshotter(provider_);

  provider_.snapshot.queue.total_entries = 100;
  snapshotter.Observe();
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kQueueDepth), 100.0);

  provider_.snapshot.queue.total_entries = 7;
  snapshotter.Observe();
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kQueueDepth), 7.0);
}

// ADP-012 disabled-state contract: Scrape is empty, but observation keeps
// updating state so enabling later needs no hot-path flag check.
TEST_F(MetricsSnapshotterTest, DisabledRegistryScrapesEmptyButKeepsObserving) {
  auto& registry = metrics::Registry::Instance();
  registry.SetEnabled(false);

  provider_.snapshot.queue.total_entries = 314;
  MetricsSnapshotter snapshotter(provider_);
  snapshotter.Observe();

  EXPECT_TRUE(registry.Scrape().empty());

  registry.SetEnabled(true);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kQueueDepth), 314.0);
}

}  // namespace
}  // namespace abyss::admin
