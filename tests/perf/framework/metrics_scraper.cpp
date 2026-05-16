#include "metrics_scraper.h"

#include <httplib.h>

#include <charconv>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

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

}  // namespace abyss::perf
