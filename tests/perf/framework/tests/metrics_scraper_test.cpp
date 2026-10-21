#include "metrics_scraper.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstddef>
#include <limits>
#include <string>
#include <string_view>

namespace abyss::perf {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();

TEST(MetricsScraperTest, ParsesPlainMetrics) {
  const std::string body = R"(# HELP a A counter
# TYPE a counter
a 42
b 3.14
)";
  const auto m = MetricsScraper::ParseMetrics(body);
  ASSERT_TRUE(m.contains("a"));
  ASSERT_TRUE(m.contains("b"));
  EXPECT_DOUBLE_EQ(m.at("a"), 42.0);
  EXPECT_DOUBLE_EQ(m.at("b"), 3.14);
}

TEST(MetricsScraperTest, IgnoresCommentsAndBlankLines) {
  const std::string body = R"(# this is a comment
# HELP abyss_x x
# TYPE abyss_x gauge

abyss_x 99
)";
  const auto m = MetricsScraper::ParseMetrics(body);
  EXPECT_EQ(m.size(), 1U);
  EXPECT_DOUBLE_EQ(m.at("abyss_x"), 99.0);
}

TEST(MetricsScraperTest, ExtractsLabelledMetrics) {
  const std::string body = R"(abyss_op_total{op="GET",status="ok"} 1234
abyss_op_total{op="SET",status="ok"} 5678
)";
  const auto m = MetricsScraper::ParseMetrics(body);
  ASSERT_TRUE(m.contains("abyss_op_total"));
  // Last-line-wins for the same metric name with different labels — the
  // scraper documents this; consumers needing per-label values should ask
  // for them explicitly.
  EXPECT_DOUBLE_EQ(m.at("abyss_op_total"), 5678.0);
}

TEST(MetricsScraperTest, IgnoresMalformedLines) {
  const std::string body = R"(this is not a metric
=== nope ===
valid_metric 17
)";
  const auto m = MetricsScraper::ParseMetrics(body);
  EXPECT_EQ(m.size(), 1U);
  EXPECT_DOUBLE_EQ(m.at("valid_metric"), 17.0);
}

TEST(MetricsScraperTest, HandlesEmptyBody) {
  const auto m = MetricsScraper::ParseMetrics("");
  EXPECT_TRUE(m.empty());
}

constexpr std::string_view kHistogramStart = R"(# HELP h_seconds A histogram
# TYPE h_seconds histogram
h_seconds_count{phase="a"} 4
h_seconds_sum{phase="a"} 4
h_seconds_bucket{phase="a",le="1"} 4
h_seconds_bucket{phase="a",le="2"} 4
h_seconds_bucket{phase="a",le="+Inf"} 4
# TYPE c counter
c_count 7
)";

constexpr std::string_view kHistogramEnd = R"(# HELP h_seconds A histogram
# TYPE h_seconds histogram
h_seconds_count{phase="a"} 14
h_seconds_sum{phase="a"} 19
h_seconds_bucket{phase="a",le="1"} 4
h_seconds_bucket{phase="a",le="2"} 14
h_seconds_bucket{phase="a",le="+Inf"} 14
h_seconds_count{phase="b"} 0
h_seconds_sum{phase="b"} 0
h_seconds_bucket{phase="b",le="1"} 0
h_seconds_bucket{phase="b",le="2"} 0
h_seconds_bucket{phase="b",le="+Inf"} 0
# TYPE g_seconds histogram
g_seconds_count 2
g_seconds_sum 20
g_seconds_bucket{le="1"} 0
g_seconds_bucket{le="+Inf"} 2
# TYPE c counter
c_count 9
)";

TEST(MetricsScraperTest, ParsesHistogramsPerSeriesWithoutLe) {
  const auto h = MetricsScraper::ParseHistograms(std::string{kHistogramEnd});
  ASSERT_EQ(h.size(), 3U) << "c_count belongs to a counter";
  const auto& a = h.at(R"(h_seconds{phase="a"})");
  EXPECT_DOUBLE_EQ(a.count, 14.0);
  EXPECT_DOUBLE_EQ(a.sum, 19.0);
  ASSERT_EQ(a.buckets.size(), 3U);
  EXPECT_DOUBLE_EQ(a.buckets[1].first, 2.0);
  EXPECT_DOUBLE_EQ(a.buckets[1].second, 14.0);
  EXPECT_TRUE(std::isinf(a.buckets[2].first));
  EXPECT_TRUE(h.contains("g_seconds"));
}

// A quantile is its bucket's upper bound, never interpolated, with the
// samples it rests on.
TEST(MetricsScraperTest, QuantilesAreTheirBucketBoundWithTheirSamples) {
  const HistogramSeries series{
      .buckets = {{1.0, 990.0}, {2.0, 997.0}, {5.0, 1000.0}, {kInf, 1000.0}},
      .count = 1000.0,
      .sum = 0.0};
  const BucketQuantile p50 = QuantileBucket(series, 0.5);
  EXPECT_DOUBLE_EQ(p50.le, 1.0);
  EXPECT_EQ(p50.samples, 1000U);
  const BucketQuantile p99 = QuantileBucket(series, 0.99);
  EXPECT_DOUBLE_EQ(p99.le, 1.0) << "990 of 1000 are at most 1";
  const BucketQuantile p999 = QuantileBucket(series, 0.999);
  EXPECT_DOUBLE_EQ(p999.le, 5.0);
  EXPECT_EQ(p999.samples, 3U) << "the p99.9 rests on the three samples above 2";
  EXPECT_FALSE(p999.beyond);

  const HistogramSeries tail{.buckets = {{1.0, 5.0}, {kInf, 10.0}}, .count = 10.0, .sum = 0.0};
  const BucketQuantile over = QuantileBucket(tail, 0.99);
  EXPECT_DOUBLE_EQ(over.le, 1.0) << "past the last finite bound";
  EXPECT_TRUE(over.beyond);
  EXPECT_EQ(over.samples, 5U);
  EXPECT_EQ(QuantileBucket(HistogramSeries{}, 0.5).samples, 0U);
}

TEST(MetricsScraperTest, SummariesCoverOnlyWhatTheRunRecorded) {
  const auto summaries =
      SummariseHistograms(std::string{kHistogramStart}, std::string{kHistogramEnd});
  ASSERT_EQ(summaries.size(), 2U) << "phase b recorded nothing";
  const auto& g = summaries[0];
  EXPECT_EQ(g.series, "g_seconds") << "absent at the start: counted from zero";
  EXPECT_EQ(g.count, 2U);
  const auto& a = summaries[1];
  EXPECT_EQ(a.series, R"(h_seconds{phase="a"})");
  EXPECT_EQ(a.count, 10U);
  EXPECT_DOUBLE_EQ(a.mean_us, 1.5e6);
  EXPECT_DOUBLE_EQ(a.p50_us.le, 2e6);
  EXPECT_EQ(a.p50_us.samples, 10U) << "all ten new samples are in (1, 2]";
}

}  // namespace
}  // namespace abyss::perf
