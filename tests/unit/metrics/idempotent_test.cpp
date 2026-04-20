#include <gtest/gtest.h>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

class IdempotentTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(IdempotentTest, SameDescriptorTwiceSharesState) {
  auto& reg = Registry::Instance();
  auto a = reg.Counter(names::kMissesTotal);
  auto b = reg.Counter(names::kMissesTotal);
  a.Increment();
  b.Increment();
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kMissesTotal), 2.0);
}

TEST_F(IdempotentTest, SameLabelledSeriesSharesState) {
  auto& reg = Registry::Instance();
  auto a = reg.Counter(names::kHitsTotal, Tier::kHot);
  auto b = reg.Counter(names::kHitsTotal, Tier::kHot);
  a.Increment(4.0);
  b.Increment(6.0);
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kHitsTotal, Tier::kHot), 10.0);
}

TEST_F(IdempotentTest, DifferentLabelValuesGetDistinctSeries) {
  auto& reg = Registry::Instance();
  auto hot = reg.Counter(names::kHitsTotal, Tier::kHot);
  auto cold = reg.Counter(names::kHitsTotal, Tier::kCold);
  hot.Increment();
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kHitsTotal, Tier::kHot), 1.0);
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kHitsTotal, Tier::kCold), 0.0);
  cold.Increment();
  EXPECT_DOUBLE_EQ(*testing::GetCounterValue(names::kHitsTotal, Tier::kCold), 1.0);
}

}  // namespace
}  // namespace abyss::metrics
