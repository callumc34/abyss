#pragma once

#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "http_client.h"

namespace abyss::system_test {

using LabelMatch = std::pair<std::string_view, std::string_view>;

// Parses a single counter sample from a Prometheus text-format payload.
// Returns nullopt when no sample matches the metric name and label set.
// The match is exact on label key+value but tolerant of label ordering and
// whitespace. Lines beginning with '#' are skipped (HELP/TYPE).
inline std::optional<double> ParseCounter(std::string_view body, std::string_view name,
                                          std::initializer_list<LabelMatch> labels) {
  size_t pos = 0;
  while (pos < body.size()) {
    const auto eol = body.find('\n', pos);
    const std::string_view line =
        eol == std::string_view::npos ? body.substr(pos) : body.substr(pos, eol - pos);
    pos = eol == std::string_view::npos ? body.size() : eol + 1;
    if (line.empty() || line.front() == '#') continue;
    if (!line.starts_with(name)) continue;

    std::string_view rest = line.substr(name.size());
    std::string_view label_part;
    if (!rest.empty() && rest.front() == '{') {
      const auto close = rest.find('}');
      if (close == std::string_view::npos) continue;
      label_part = rest.substr(1, close - 1);
      rest = rest.substr(close + 1);
    } else if (!rest.empty() && rest.front() != ' ') {
      // Different metric whose name is a prefix of `name`.
      continue;
    }

    bool all_matched = true;
    for (const auto& [k, v] : labels) {
      bool found = false;
      size_t lp = 0;
      while (lp < label_part.size()) {
        const auto comma = label_part.find(',', lp);
        const std::string_view kv = comma == std::string_view::npos
                                        ? label_part.substr(lp)
                                        : label_part.substr(lp, comma - lp);
        lp = comma == std::string_view::npos ? label_part.size() : comma + 1;
        const auto eq = kv.find('=');
        if (eq == std::string_view::npos) continue;
        std::string_view key = kv.substr(0, eq);
        std::string_view val = kv.substr(eq + 1);
        if (!val.empty() && val.front() == '"') val.remove_prefix(1);
        if (!val.empty() && val.back() == '"') val.remove_suffix(1);
        if (key == k && val == v) {
          found = true;
          break;
        }
      }
      if (!found) {
        all_matched = false;
        break;
      }
    }
    if (!all_matched) continue;

    while (!rest.empty() && rest.front() == ' ') rest.remove_prefix(1);
    if (rest.empty()) continue;
    double value = 0.0;
    bool negative = false;
    size_t i = 0;
    if (rest[i] == '-') {
      negative = true;
      ++i;
    }
    bool any_digit = false;
    while (i < rest.size() && rest[i] >= '0' && rest[i] <= '9') {
      value = (value * 10.0) + static_cast<double>(rest[i] - '0');
      any_digit = true;
      ++i;
    }
    if (i < rest.size() && rest[i] == '.') {
      ++i;
      double frac = 0.1;
      while (i < rest.size() && rest[i] >= '0' && rest[i] <= '9') {
        value += static_cast<double>(rest[i] - '0') * frac;
        frac *= 0.1;
        any_digit = true;
        ++i;
      }
    }
    if (!any_digit) continue;
    return negative ? -value : value;
  }
  return std::nullopt;
}

inline std::optional<double> ParseCounter(std::string_view body, std::string_view name) {
  return ParseCounter(body, name, {});
}

// Fetches /metrics and returns the body, or empty on error.
inline std::string Scrape(uint16_t metrics_port) {
  const auto resp =
      abyss::testing::HttpTestClient::Send("127.0.0.1", metrics_port, "GET", "/metrics");
  if (!resp.ok || resp.status != 200) return {};
  return resp.body;
}

// Polls /metrics every `interval` until the named counter reaches `threshold`
// or the deadline elapses. Returns the observed value on success, nullopt on
// timeout. On timeout, `last_body` (if non-null) holds the final scrape for
// the failing assertion to log.
inline std::optional<double> PollCounterAtLeast(
    uint16_t metrics_port, std::string_view name, std::initializer_list<LabelMatch> labels,
    double threshold, std::chrono::milliseconds timeout, std::string* last_body = nullptr,
    std::chrono::milliseconds interval = std::chrono::milliseconds{20}) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string body;
  while (std::chrono::steady_clock::now() < deadline) {
    body = Scrape(metrics_port);
    if (auto v = ParseCounter(body, name, labels); v.has_value() && *v >= threshold) {
      if (last_body != nullptr) *last_body = std::move(body);
      return v;
    }
    std::this_thread::sleep_for(interval);
  }
  if (last_body != nullptr) *last_body = std::move(body);
  return std::nullopt;
}

inline std::optional<double> PollCounterAtLeast(uint16_t metrics_port, std::string_view name,
                                                double threshold, std::chrono::milliseconds timeout,
                                                std::string* last_body = nullptr) {
  return PollCounterAtLeast(metrics_port, name, {}, threshold, timeout, last_body);
}

// Waits until nothing is left in the compaction buffer, then returns the scrape
// that observed it. Call this before baselining any cold-tier flush counter.
//
// The data-path fixtures probe readiness with a real `SET`/`DEL` pair, and those
// land in the compaction buffer like any other write. Flush readiness is per
// entry (`CompactionBuffer::FlushReady`), so the probe's own entry becomes
// flushable roughly one quiet window after SetUp returns -- inside the test
// body -- and both `abyss_cold_flush_total{status="success"}` and
// `abyss_cold_flush_reason_total{reason="quiet"}` count per entry, so it is
// worth exactly +1 on each. A baseline taken before that lands attributes the
// probe's flush to whatever the test did next: an exact-delta assertion flakes,
// and an at-least assertion silently passes without the test's own write ever
// having been flushed, which is the more dangerous outcome.
inline std::string AwaitColdQuiescence(
    uint16_t metrics_port, std::chrono::milliseconds timeout = std::chrono::seconds{15},
    std::chrono::milliseconds interval = std::chrono::milliseconds{50}) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string body;
  while (std::chrono::steady_clock::now() < deadline) {
    body = Scrape(metrics_port);
    const auto flushed = ParseCounter(body, "abyss_cold_flush_total", {{"status", "success"}});
    const auto buffered = ParseCounter(body, "abyss_cold_buffer_entries");
    // Both conditions matter: an empty buffer alone is also true before the
    // probe's writes have been drained out of the queue into it.
    if (flushed.value_or(0.0) >= 1.0 && buffered.value_or(-1.0) == 0.0) return body;
    std::this_thread::sleep_for(interval);
  }
  return {};
}

}  // namespace abyss::system_test
