#include "metrics_scraper.h"

#include <httplib.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/result.h"

namespace abyss::perf {

namespace {

using core::Error;
using core::ErrorCode;
using core::Result;

std::string_view Trim(std::string_view s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r')) {
    s.remove_suffix(1);
  }
  return s;
}

// Split a metric line `name[{labels}] value [timestamp]` into name and value.
// Returns false if the line is malformed.
bool ParseLine(std::string_view line, std::string& name_out, double& value_out) {
  if (line.empty() || line.front() == '#') return false;
  const auto brace = line.find('{');
  const auto first_space = line.find_first_of(" \t");
  if (first_space == std::string_view::npos) return false;

  // NOLINTNEXTLINE(cppcoreguidelines-init-variables): false positive on conditional init.
  const size_t name_end =
      brace != std::string_view::npos && brace < first_space ? brace : first_space;
  const auto name = Trim(line.substr(0, name_end));
  if (name.empty()) return false;

  const auto value_start =
      brace != std::string_view::npos && brace < first_space ? line.find('}', brace) : first_space;
  if (value_start == std::string_view::npos) return false;
  const auto value_token = Trim(line.substr(value_start + 1));
  if (value_token.empty()) return false;

  const auto value_end = value_token.find_first_of(" \t");
  const auto value_str =
      value_end == std::string_view::npos ? value_token : value_token.substr(0, value_end);

  double parsed = 0.0;
  const char* begin = value_str.data();
  const char* end = value_str.data() + value_str.size();
  const auto [ptr, ec] = std::from_chars(begin, end, parsed);
  if (ec != std::errc{} || ptr != end) {
    if (value_str == "NaN" || value_str == "+Inf" || value_str == "-Inf") {
      return false;
    }
    return false;
  }
  name_out = std::string{name};
  value_out = parsed;
  return true;
}

std::optional<double> ParseNumber(std::string_view token) {
  if (token == "+Inf") return std::numeric_limits<double>::infinity();
  double parsed = 0.0;
  const auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), parsed);
  if (ec != std::errc{} || ptr != token.data() + token.size()) return std::nullopt;
  return parsed;
}

// A sample line split into its name, its labels but `le`, the `le`
// bound if any, and its value.
struct Sample {
  std::string_view name;
  std::string labels;
  std::optional<double> le;
  double value = 0.0;
};

std::optional<Sample> ParseSample(std::string_view line) {
  if (line.empty() || line.front() == '#') return std::nullopt;
  Sample sample;
  size_t pos = line.find_first_of("{ \t");
  if (pos == std::string_view::npos) return std::nullopt;
  sample.name = line.substr(0, pos);
  if (line[pos] == '{') {
    ++pos;
    while (pos < line.size() && line[pos] != '}') {
      const size_t eq = line.find('=', pos);
      if (eq == std::string_view::npos || eq + 1 >= line.size() || line[eq + 1] != '"') {
        return std::nullopt;
      }
      size_t end = eq + 2;
      while (end < line.size() && line[end] != '"') end += line[end] == '\\' ? 2 : 1;
      if (end >= line.size()) return std::nullopt;
      const std::string_view key = line.substr(pos, eq - pos);
      const std::string_view value = line.substr(eq + 2, end - eq - 2);
      if (key == "le") {
        sample.le = ParseNumber(value);
        if (!sample.le.has_value()) return std::nullopt;
      } else {
        if (!sample.labels.empty()) sample.labels += ',';
        sample.labels.append(line.substr(pos, end + 1 - pos));
      }
      pos = end + 1;
      if (pos < line.size() && line[pos] == ',') ++pos;
    }
    if (pos >= line.size()) return std::nullopt;
    ++pos;
  }
  const auto value = Trim(line.substr(pos));
  const auto parsed = ParseNumber(value.substr(0, value.find_first_of(" \t")));
  if (!parsed.has_value()) return std::nullopt;
  sample.value = *parsed;
  return sample;
}

std::string SeriesKey(std::string_view name, const std::string& labels) {
  if (labels.empty()) return std::string{name};
  return std::string{name} + "{" + labels + "}";
}

// `end` less `start`, bucket by bucket; a series absent at the start
// counts from zero.
HistogramSeries Delta(const HistogramSeries& end, const HistogramSeries* start) {
  if (start == nullptr) return end;
  HistogramSeries out = end;
  out.count -= start->count;
  out.sum -= start->sum;
  for (auto& [bound, cumulative] : out.buckets) {
    const auto it = std::ranges::find(start->buckets, bound, &std::pair<double, double>::first);
    if (it != start->buckets.end()) cumulative -= it->second;
  }
  return out;
}

}  // namespace

MetricsScraper::MetricsScraper(std::string base_url, std::string path)
    : base_url_(std::move(base_url)), path_(std::move(path)) {}

Result<std::map<std::string, double>> MetricsScraper::Snapshot() {
  httplib::Client client(base_url_);
  client.set_connection_timeout(2, 0);
  client.set_read_timeout(2, 0);
  auto res = client.Get(path_);
  if (!res) {
    return std::unexpected(Error(ErrorCode::kUnavailable, "metrics endpoint unreachable"));
  }
  if (res->status != 200) {
    return std::unexpected(
        Error(ErrorCode::kUnavailable, "metrics endpoint status " + std::to_string(res->status)));
  }
  return ParseMetrics(res->body);
}

std::map<std::string, double> MetricsScraper::ParseMetrics(const std::string& body) {
  std::map<std::string, double> out;
  std::istringstream stream{body};
  std::string line;
  while (std::getline(stream, line)) {
    std::string name;
    double value = 0.0;
    if (ParseLine(line, name, value)) {
      out[name] = value;
    }
  }
  return out;
}

std::map<std::string, HistogramSeries> MetricsScraper::ParseHistograms(const std::string& body) {
  std::set<std::string, std::less<>> histograms;
  std::map<std::string, HistogramSeries> out;
  std::istringstream stream{body};
  std::string line;
  constexpr std::string_view kType = "# TYPE ";
  while (std::getline(stream, line)) {
    const std::string_view view{line};
    if (view.starts_with(kType)) {
      const auto rest = view.substr(kType.size());
      const auto space = rest.find(' ');
      if (space != std::string_view::npos && Trim(rest.substr(space + 1)) == "histogram") {
        histograms.emplace(rest.substr(0, space));
      }
      continue;
    }
    const auto sample = ParseSample(view);
    if (!sample.has_value()) continue;
    for (const std::string_view suffix : {"_bucket", "_sum", "_count"}) {
      if (!sample->name.ends_with(suffix)) continue;
      const auto family = sample->name.substr(0, sample->name.size() - suffix.size());
      if (!histograms.contains(family)) continue;
      HistogramSeries& series = out[SeriesKey(family, sample->labels)];
      if (suffix == "_bucket" && sample->le.has_value()) {
        series.buckets.emplace_back(*sample->le, sample->value);
      } else if (suffix == "_sum") {
        series.sum = sample->value;
      } else if (suffix == "_count") {
        series.count = sample->value;
      }
      break;
    }
  }
  for (auto& [_, series] : out) std::ranges::sort(series.buckets);
  return out;
}

BucketQuantile QuantileBucket(const HistogramSeries& series, double q) {
  if (series.buckets.empty() || series.count <= 0.0) return {};
  const double total = series.buckets.back().second;
  const double rank = q * total;
  double lower = 0.0;
  double below = 0.0;
  for (const auto& [bound, cumulative] : series.buckets) {
    if (cumulative >= rank && cumulative > below) {
      return {.le = std::isinf(bound) ? lower : bound,
              .beyond = std::isinf(bound),
              .samples = static_cast<uint64_t>(std::llround(total - below))};
    }
    lower = bound;
    below = cumulative;
  }
  return {.le = lower, .beyond = true, .samples = 0};
}

std::vector<HistogramSummary> SummariseHistograms(const std::string& start,
                                                  const std::string& end) {
  constexpr double kUs = 1e6;
  const auto in_us = [](BucketQuantile q) {
    q.le *= kUs;
    return q;
  };
  const auto before = MetricsScraper::ParseHistograms(start);
  std::vector<HistogramSummary> out;
  for (const auto& [key, at_end] : MetricsScraper::ParseHistograms(end)) {
    const auto it = before.find(key);
    const HistogramSeries delta = Delta(at_end, it == before.end() ? nullptr : &it->second);
    if (delta.count <= 0.0) continue;
    out.push_back({
        .series = key,
        .count = static_cast<uint64_t>(std::llround(delta.count)),
        .mean_us = delta.sum / delta.count * kUs,
        .p50_us = in_us(QuantileBucket(delta, 0.5)),
        .p99_us = in_us(QuantileBucket(delta, 0.99)),
        .p999_us = in_us(QuantileBucket(delta, 0.999)),
    });
  }
  return out;
}

}  // namespace abyss::perf
