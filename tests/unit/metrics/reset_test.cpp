#include <gtest/gtest.h>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

TEST(MetricsReset, EmptiesRegistry) {
  auto& reg = Registry::Instance();
  reg.Counter(names::kMissesTotal).Increment();
  ASSERT_TRUE(testing::GetCounterValue(names::kMissesTotal).has_value());

  testing::Reset();
  EXPECT_FALSE(testing::GetCounterValue(names::kMissesTotal).has_value());
  EXPECT_TRUE(reg.Scrape().empty());
}

TEST(MetricsReset, RestoresEnabledFlag) {
  auto& reg = Registry::Instance();
  reg.SetEnabled(false);
  testing::Reset();
  EXPECT_TRUE(reg.Enabled());
}

TEST(MetricsReset, ReRegistrationAfterResetStartsFresh) {
  auto& reg = Registry::Instance();
  reg.Counter(names::kMissesTotal).Increment(5.0);
  testing::Reset();

  auto handle = reg.Counter(names::kMissesTotal);
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kMissesTotal), 0.0);
  handle.Increment();
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kMissesTotal), 1.0);
}

}  // namespace
}  // namespace abyss::metrics
