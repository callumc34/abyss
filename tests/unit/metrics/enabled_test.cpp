#include <gtest/gtest.h>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

class EnabledTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(EnabledTest, DefaultsToEnabled) { EXPECT_TRUE(Registry::Instance().Enabled()); }

TEST_F(EnabledTest, DisabledScrapeIsEmpty) {
  auto& reg = Registry::Instance();
  reg.Counter(names::kMissesTotal).Increment();
  reg.SetEnabled(false);
  EXPECT_TRUE(reg.Scrape().empty());
}

TEST_F(EnabledTest, DisabledObservationStillUpdatesInternalState) {
  auto& reg = Registry::Instance();
  auto h = reg.Counter(names::kMissesTotal);
  reg.SetEnabled(false);
  h.Increment();
  h.Increment();
  reg.SetEnabled(true);
  ASSERT_TRUE(testing::GetCounterValue(names::kMissesTotal).has_value());
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kMissesTotal), 2.0);
}

}  // namespace
}  // namespace abyss::metrics
