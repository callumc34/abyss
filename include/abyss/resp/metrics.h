#pragma once

#include <array>
#include <cstdint>
#include <string_view>
#include <unordered_map>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::resp {

class CommandRegistry;

inline constexpr std::string_view kUnknownCmdLabel = "unknown";

// Handles are pre-registered so the per-request path is lock-free.
class RespMetrics {
 public:
  explicit RespMetrics(const CommandRegistry& registry);

  // `cmd` must be either a registry canonical name or kUnknownCmdLabel.
  void RecordDuration(std::string_view cmd, double seconds) noexcept;
  void RecordRequest(std::string_view cmd, metrics::RequestStatus status) noexcept;
  void RecordParseError() noexcept;
  void RecordProtocol(uint8_t version) noexcept;

 private:
  static constexpr size_t kStatusCount = 6;

  struct PerCmd {
    metrics::HistogramHandle duration;
    std::array<metrics::CounterHandle, kStatusCount> request_total;
  };

  PerCmd Register(std::string_view cmd);
  PerCmd* Lookup(std::string_view cmd) noexcept;

  std::unordered_map<std::string_view, PerCmd> per_cmd_;
  metrics::CounterHandle parse_errors_;
  std::array<metrics::CounterHandle, 2> protocol_totals_;  // index 0 → v2, 1 → v3
};

}  // namespace abyss::resp
