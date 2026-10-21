#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/result.h"

namespace abyss::perf {

// One histogram series of a scrape.
struct HistogramSeries {
  // (upper bound, cumulative count), ascending; +Inf is the last.
  std::vector<std::pair<double, double>> buckets;
  double count = 0.0;
  double sum = 0.0;
};

// A quantile read off the bucket bounds, which are as fine as it gets.
struct BucketQuantile {
  // The upper bound of the bucket it falls in; past the last finite
  // bound, that bound with `beyond` set.
  double le = 0.0;
  bool beyond = false;
  // Samples at or above that bucket's lower bound: what it rests on.
  uint64_t samples = 0;
};

// What one histogram series recorded between two scrapes, in µs.
struct HistogramSummary {
  // The name and labels, `le` left out.
  std::string series;
  uint64_t count = 0;
  double mean_us = 0.0;
  BucketQuantile p50_us;
  BucketQuantile p99_us;
  BucketQuantile p999_us;
};

// Scrapes a Prometheus /metrics endpoint and parses the text exposition
// format. Only the simple form `name value` and `name{labels} value` is
// supported; HELP and TYPE annotations are ignored. For metrics with
// labels, parsed values are keyed by `name` only; the latest line wins.
class MetricsScraper {
 public:
  MetricsScraper(std::string base_url, std::string path = "/metrics");

  core::Result<std::map<std::string, double>> Snapshot();

  static std::map<std::string, double> ParseMetrics(const std::string& body);
  // Histograms declared by `# TYPE`, keyed by name and labels but le.
  static std::map<std::string, HistogramSeries> ParseHistograms(const std::string& body);

 private:
  std::string base_url_;
  std::string path_;
};

// The bucket the `q` quantile of a series falls in.
BucketQuantile QuantileBucket(const HistogramSeries& series, double q);

// Every histogram series that recorded something from `start` to `end`.
std::vector<HistogramSummary> SummariseHistograms(const std::string& start, const std::string& end);

}  // namespace abyss::perf
