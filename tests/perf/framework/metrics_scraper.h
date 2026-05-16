#pragma once

#include <map>
#include <string>

#include "abyss/core/result.h"

namespace abyss::perf {

// Scrapes a Prometheus /metrics endpoint and parses the text exposition
// format. Only the simple form `name value` and `name{labels} value` is
// supported; HELP and TYPE annotations are ignored. For metrics with
// labels, parsed values are keyed by `name` only; the latest line wins.
class MetricsScraper {
 public:
  MetricsScraper(std::string base_url, std::string path = "/metrics");

  core::Result<std::map<std::string, double>> Snapshot();

  static std::map<std::string, double> ParseMetrics(const std::string& body);

 private:
  std::string base_url_;
  std::string path_;
};

}  // namespace abyss::perf
