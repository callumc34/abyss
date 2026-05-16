#include "metrics_scraper.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>

namespace abyss::perf {
namespace {

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
  EXPECT_EQ(m.size(), 1u);
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
  EXPECT_EQ(m.size(), 1u);
  EXPECT_DOUBLE_EQ(m.at("valid_metric"), 17.0);
}

TEST(MetricsScraperTest, HandlesEmptyBody) {
  const auto m = MetricsScraper::ParseMetrics("");
  EXPECT_TRUE(m.empty());
}

}  // namespace
}  // namespace abyss::perf
