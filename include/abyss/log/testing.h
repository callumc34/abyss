#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "abyss/log/log.h"

namespace abyss::log::testing {

// Structured record captured by CapturingSink. Fields are stringified at
// capture time; tests assert on the record shape, not the JSON text.
struct CapturedRecord {
  std::chrono::system_clock::time_point ts;
  Level level;
  std::string component;
  std::string msg;
  std::vector<std::pair<std::string, std::string>> fields;
};

// Installs a capturing sink as the sole sink and clears any prior records.
// Returns a handle whose destructor detaches the sink, restores the prior
// sink configuration, and leaves the facade in its pre-test state.
class CapturingSink {
 public:
  CapturingSink();
  ~CapturingSink();

  CapturingSink(const CapturingSink&) = delete;
  CapturingSink& operator=(const CapturingSink&) = delete;
  CapturingSink(CapturingSink&&) = delete;
  CapturingSink& operator=(CapturingSink&&) = delete;

  std::vector<CapturedRecord> Records() const;
  size_t Size() const noexcept;
  void Clear();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Detaches all sinks, clears the logger cache, resets to pre-Init state
// (stderr fallback at INFO). Not safe concurrently with emission.
void Reset();

// Runs the JSON or text formatter against a synthesised record. Used by
// tests that need to assert on byte-level formatter output without reaching
// into spdlog directly.
std::string FormatJson(Level level, std::string_view component, std::string_view msg,
                       std::span<const LogField> fields);
std::string FormatText(Level level, std::string_view component, std::string_view msg,
                       std::span<const LogField> fields);

}  // namespace abyss::log::testing
