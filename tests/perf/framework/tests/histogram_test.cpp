#include "histogram.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>

namespace abyss::perf {
namespace {

TEST(HistogramTest, EmptyHistogramReportsZeros) {
  Histogram h;
  EXPECT_EQ(h.Count(), 0);
  EXPECT_EQ(h.MinNs(), 0);
  EXPECT_EQ(h.MaxNs(), 0);
  EXPECT_EQ(h.MeanNs(), 0.0);
}

TEST(HistogramTest, RecordTracksCountAndPercentiles) {
  Histogram h;
  for (int64_t i = 1; i <= 1000; ++i) {
    h.Record(i * 1000);  // 1us..1000us
  }
  EXPECT_EQ(h.Count(), 1000);
  EXPECT_LE(h.PercentileNs(50.0), 510'000);
  EXPECT_GE(h.PercentileNs(50.0), 490'000);
  EXPECT_LE(h.PercentileNs(99.0), 1'000'000);
  EXPECT_GE(h.PercentileNs(99.0), 980'000);
}

TEST(HistogramTest, RecordClampsAboveHighestAndSetsSaturated) {
  constexpr int64_t kHighestNs = 10'000'000;
  Histogram h{Histogram::kDefaultLowestNs, kHighestNs, Histogram::kDefaultSignificantDigits};
  EXPECT_FALSE(h.Saturated());
  h.Record(5'000'000);
  EXPECT_FALSE(h.Saturated());
  h.Record(50'000'000);  // above highest_ns; the framework clamps and flags.
  EXPECT_TRUE(h.Saturated());
  // HdrHistogram returns each value's bucket upper bound (~0.1% precision at
  // 3 sig figs); clamped values sit in the topmost in-range bucket.
  EXPECT_LE(h.MaxNs(), kHighestNs + (kHighestNs / 100));
}

TEST(HistogramTest, MergeCombinesCountsAndSaturation) {
  Histogram a;
  Histogram b;
  for (int64_t i = 0; i < 100; ++i) a.Record(i * 1000 + 1000);
  for (int64_t i = 0; i < 200; ++i) b.Record(i * 1000 + 1000);
  a.Merge(b);
  EXPECT_EQ(a.Count(), 300);

  Histogram saturated{Histogram::kDefaultLowestNs, 1'000'000, Histogram::kDefaultSignificantDigits};
  saturated.Record(10'000'000);  // saturates
  EXPECT_TRUE(saturated.Saturated());
  Histogram clean;
  clean.Record(5'000);
  clean.Merge(saturated);
  EXPECT_TRUE(clean.Saturated());
}

TEST(HistogramTest, EncodeBase64ProducesNonEmptyOutput) {
  Histogram h;
  for (int64_t i = 0; i < 100; ++i) h.Record(i * 1000 + 1000);
  const auto encoded = h.EncodeBase64();
  EXPECT_FALSE(encoded.empty());
  // HdrHistogram encoded form begins with "HIST" header prefix in some
  // versions; we don't pin the format, only that it's non-empty.
}

TEST(HistogramTest, ResetClearsCountAndSaturated) {
  Histogram h{Histogram::kDefaultLowestNs, 10'000'000, Histogram::kDefaultSignificantDigits};
  h.Record(5'000'000);
  h.Record(50'000'000);
  EXPECT_GT(h.Count(), 0);
  EXPECT_TRUE(h.Saturated());
  h.Reset();
  EXPECT_EQ(h.Count(), 0);
  EXPECT_FALSE(h.Saturated());
}

TEST(HistogramTest, WriteHgrmEmitsPercentileLines) {
  Histogram h;
  for (int64_t i = 1; i <= 1000; ++i) h.Record(i * 1000);
  std::ostringstream out;
  h.WriteHgrmTo(out);
  const auto s = out.str();
  EXPECT_NE(s.find("Percentile"), std::string::npos);
  EXPECT_NE(s.find("Mean"), std::string::npos);
  EXPECT_NE(s.find("Total count"), std::string::npos);
}

TEST(HistogramTest, MoveTransfersOwnership) {
  Histogram a;
  a.Record(1000);
  EXPECT_EQ(a.Count(), 1);
  Histogram b{std::move(a)};
  EXPECT_EQ(b.Count(), 1);
}

}  // namespace
}  // namespace abyss::perf
