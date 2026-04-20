#include <gtest/gtest.h>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

class RegisterTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(RegisterTest, CounterIncrementAndRead) {
  auto& reg = Registry::Instance();
  auto handle = reg.Counter(names::kHitsTotal, Tier::kHot);
  handle.Increment();
  handle.Increment();
  handle.Increment(3.0);

  const auto value = testing::GetCounterValue(names::kHitsTotal, Tier::kHot);
  ASSERT_TRUE(value.has_value());
  EXPECT_DOUBLE_EQ(*value, 5.0);
}

TEST_F(RegisterTest, GaugeSetAndRead) {
  auto& reg = Registry::Instance();
  auto handle = reg.Gauge(names::kHotMemoryBytes);
  handle.Set(1024.0);
  handle.Increment(512.0);
  handle.Decrement(256.0);

  const auto value = testing::GetGaugeValue(names::kHotMemoryBytes);
  ASSERT_TRUE(value.has_value());
  EXPECT_DOUBLE_EQ(*value, 1280.0);
}

TEST_F(RegisterTest, HistogramObserveAndRead) {
  auto& reg = Registry::Instance();
  auto handle = reg.Histogram(names::kQueueAppendDurationSeconds);
  handle.Observe(0.002);
  handle.Observe(0.050);
  handle.Observe(0.500);

  const auto count = testing::GetHistogramCount(names::kQueueAppendDurationSeconds);
  const auto sum = testing::GetHistogramSum(names::kQueueAppendDurationSeconds);
  ASSERT_TRUE(count.has_value());
  ASSERT_TRUE(sum.has_value());
  EXPECT_EQ(*count, 3U);
  EXPECT_NEAR(*sum, 0.552, 1e-9);
}

TEST_F(RegisterTest, LabelledCounterSeriesAreDistinct) {
  auto& reg = Registry::Instance();
  reg.Counter(names::kHitsTotal, Tier::kHot).Increment();
  reg.Counter(names::kHitsTotal, Tier::kHot).Increment();
  reg.Counter(names::kHitsTotal, Tier::kCold).Increment();

  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kHitsTotal, Tier::kHot), 2.0);
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kHitsTotal, Tier::kCold), 1.0);
  EXPECT_FALSE(testing::GetCounterValue(names::kHitsTotal, Tier::kBuffer).has_value());
}

TEST_F(RegisterTest, UnregisteredSeriesReturnsNullopt) {
  EXPECT_FALSE(testing::GetCounterValue(names::kMissesTotal).has_value());
  EXPECT_FALSE(testing::GetGaugeValue(names::kHotKeys).has_value());
}

}  // namespace
}  // namespace abyss::metrics
