#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

class ScrapeTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(ScrapeTest, EmptyWhenNothingRegistered) {
  EXPECT_TRUE(Registry::Instance().Scrape().empty());
}

TEST_F(ScrapeTest, IncludesCounterWithHelpAndType) {
  auto& reg = Registry::Instance();
  auto h = reg.Counter(names::kHitsTotal, Tier::kHot);
  h.Increment(42.0);

  const std::string out = reg.Scrape();
  EXPECT_TRUE(out.contains("# HELP abyss_hits_total"));
  EXPECT_TRUE(out.contains("# TYPE abyss_hits_total counter"));
  EXPECT_TRUE(out.contains(R"(abyss_hits_total{tier="hot"})"));
  EXPECT_TRUE(out.contains("42"));
}

TEST_F(ScrapeTest, IncludesGaugeAndHistogram) {
  auto& reg = Registry::Instance();
  reg.Gauge(names::kHotKeys).Set(7.0);
  reg.Histogram(names::kWalFlushDurationSeconds).Observe(0.001);

  const std::string out = reg.Scrape();
  EXPECT_TRUE(out.contains("# TYPE abyss_hot_keys gauge"));
  EXPECT_TRUE(out.contains("# TYPE abyss_wal_flush_duration_seconds histogram"));
  EXPECT_TRUE(out.contains("abyss_wal_flush_duration_seconds_count"));
  EXPECT_TRUE(out.contains("abyss_wal_flush_duration_seconds_sum"));
  EXPECT_TRUE(out.contains("abyss_wal_flush_duration_seconds_bucket{"));
}

}  // namespace
}  // namespace abyss::metrics
