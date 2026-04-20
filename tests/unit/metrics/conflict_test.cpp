#include <gtest/gtest.h>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

constexpr CounterDesc<> kTestCounterA{.name = "abyss_conflict_test", .help = "first help"};
constexpr CounterDesc<> kTestCounterB{.name = "abyss_conflict_test", .help = "second help"};
constexpr CounterDesc<Tier> kTestCounterLabelled{.name = "abyss_conflict_test",
                                                 .help = "first help"};
constexpr GaugeDesc<> kTestGaugeSameName{.name = "abyss_conflict_test", .help = "first help"};

constexpr std::array<double, 2> kBucketsA{1.0, 10.0};
constexpr std::array<double, 2> kBucketsB{2.0, 20.0};
constexpr HistogramDesc<> kTestHistoA{
    .name = "abyss_conflict_histo",
    .help = "first help",
    .buckets = kBucketsA,
};
constexpr HistogramDesc<> kTestHistoB{
    .name = "abyss_conflict_histo",
    .help = "first help",
    .buckets = kBucketsB,
};

class ConflictTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(ConflictTest, ConflictingHelpAborts) {
  auto& reg = Registry::Instance();
  reg.Counter(kTestCounterA);
  EXPECT_DEATH({ reg.Counter(kTestCounterB); }, "conflicting registration");
}

TEST_F(ConflictTest, ConflictingLabelArityAborts) {
  auto& reg = Registry::Instance();
  reg.Counter(kTestCounterA);
  EXPECT_DEATH({ reg.Counter(kTestCounterLabelled, Tier::kHot); }, "conflicting registration");
}

TEST_F(ConflictTest, ConflictingKindAborts) {
  auto& reg = Registry::Instance();
  reg.Counter(kTestCounterA);
  EXPECT_DEATH({ reg.Gauge(kTestGaugeSameName); }, "conflicting registration");
}

TEST_F(ConflictTest, ConflictingBucketsAborts) {
  auto& reg = Registry::Instance();
  reg.Histogram(kTestHistoA);
  EXPECT_DEATH({ reg.Histogram(kTestHistoB); }, "conflicting registration");
}

}  // namespace
}  // namespace abyss::metrics
