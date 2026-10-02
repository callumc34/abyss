#include "sweep.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace abyss::perf {
namespace {

constexpr int64_t kBoundNs = 1'000'000;

// Drives the sweep against a server that holds the bound up to
// `capacity` ops/s and answers above it with a slow tail.
std::vector<uint64_t> RunAgainstCapacity(RateSweep& sweep, uint64_t capacity) {
  std::vector<uint64_t> offered;
  while (auto rate = sweep.Next()) {
    offered.push_back(*rate);
    const bool holds = *rate <= capacity;
    sweep.Record(static_cast<double>(*rate), holds ? kBoundNs / 2 : kBoundNs * 4, 0);
  }
  return offered;
}

TEST(RateSweepTest, DoublesThenBisectsToTheHighestPassingRate) {
  RateSweep sweep{1000, kBoundNs};
  const auto offered = RunAgainstCapacity(sweep, 5500);

  const std::vector<uint64_t> expected{1000, 2000, 4000, 8000, 6000, 5000, 5500, 5750};
  EXPECT_EQ(offered, expected);
  EXPECT_EQ(sweep.Result(), 5500U);
  ASSERT_EQ(sweep.Steps().size(), expected.size());
  EXPECT_FALSE(sweep.Steps()[3].pass);
  EXPECT_TRUE(sweep.Steps()[6].pass);
}

TEST(RateSweepTest, FailingStartRateHalvesThenBisects) {
  RateSweep sweep{1000, kBoundNs};
  const auto offered = RunAgainstCapacity(sweep, 300);

  const std::vector<uint64_t> expected{1000, 500, 250, 375, 312, 281, 296, 304};
  EXPECT_EQ(offered, expected);
  EXPECT_EQ(sweep.Result(), 296U);
}

TEST(RateSweepTest, NoResultOnlyWhenTheFloorFails) {
  RateSweep sweep{1000, kBoundNs};
  const auto offered = RunAgainstCapacity(sweep, 0);

  // Halving stops at the floor of 1/1024 of the start rate.
  const std::vector<uint64_t> expected{1000, 500, 250, 125, 62, 31, 15, 7, 3, 1};
  EXPECT_EQ(offered, expected);
  EXPECT_FALSE(sweep.Result().has_value());
}

TEST(RateSweepTest, MinimumRateRaisesTheFloor) {
  RateSweep sweep{1000, kBoundNs, 200};
  const auto offered = RunAgainstCapacity(sweep, 0);
  EXPECT_EQ(offered, (std::vector<uint64_t>{1000, 500, 250}));
}

TEST(RateSweepTest, ShortfallAndErrorsFailAStep) {
  RateSweep sweep{1000, kBoundNs};
  EXPECT_TRUE(sweep.Record(950.0, kBoundNs, 0).pass);
  EXPECT_FALSE(sweep.Record(1899.0, kBoundNs / 2, 0).pass);  // below 95% of 2000
  ASSERT_EQ(sweep.Next(), 1500U);
  EXPECT_FALSE(sweep.Record(1500.0, kBoundNs / 2, 1).pass);
  EXPECT_EQ(sweep.Result(), 1000U);
}

}  // namespace
}  // namespace abyss::perf
