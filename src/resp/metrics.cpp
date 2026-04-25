#include "abyss/resp/metrics.h"

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/resp/command_registry.h"

namespace abyss::resp {

RespMetrics::PerCmd RespMetrics::Register(std::string_view cmd) {
  auto& reg = metrics::Registry::Instance();
  const auto label = metrics::CmdLabel{.value = cmd};
  auto counter = [&reg, label](metrics::RequestStatus s) {
    return reg.Counter(metrics::names::kRespRequestsTotal, label, s);
  };
  return PerCmd{
      .duration = reg.Histogram(metrics::names::kRespRequestDurationSeconds, label),
      .request_total =
          {
              counter(metrics::RequestStatus::kOk),
              counter(metrics::RequestStatus::kError),
              counter(metrics::RequestStatus::kLoading),
              counter(metrics::RequestStatus::kUnknown),
              counter(metrics::RequestStatus::kArity),
              counter(metrics::RequestStatus::kNoProto),
          },
  };
}

RespMetrics::RespMetrics(const CommandRegistry& registry) {
  per_cmd_.reserve(registry.All().size() + 1);
  for (const auto& spec : registry.All()) {
    per_cmd_.emplace(spec.name, Register(spec.name));
  }
  per_cmd_.emplace(kUnknownCmdLabel, Register(kUnknownCmdLabel));

  auto& reg = metrics::Registry::Instance();
  parse_errors_ = reg.Counter(metrics::names::kRespParseErrorsTotal);
  protocol_totals_ = {
      reg.Counter(metrics::names::kRespProtocolVersionTotal, metrics::ProtoLabel{.version = 2}),
      reg.Counter(metrics::names::kRespProtocolVersionTotal, metrics::ProtoLabel{.version = 3}),
  };
}

RespMetrics::PerCmd* RespMetrics::Lookup(std::string_view cmd) noexcept {
  auto it = per_cmd_.find(cmd);
  if (it == per_cmd_.end()) {
    it = per_cmd_.find(kUnknownCmdLabel);
    if (it == per_cmd_.end()) return nullptr;
  }
  return &it->second;
}

void RespMetrics::RecordDuration(std::string_view cmd, double seconds) noexcept {
  if (auto* p = Lookup(cmd); p != nullptr) {
    p->duration.Observe(seconds);
  }
}

void RespMetrics::RecordRequest(std::string_view cmd, metrics::RequestStatus status) noexcept {
  if (auto* p = Lookup(cmd); p != nullptr) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    p->request_total[static_cast<size_t>(status)].Increment();
  }
}

void RespMetrics::RecordParseError() noexcept { parse_errors_.Increment(); }

void RespMetrics::RecordProtocol(uint8_t version) noexcept {
  if (version == 2) {
    protocol_totals_[0].Increment();
  } else if (version == 3) {
    protocol_totals_[1].Increment();
  }
}

}  // namespace abyss::resp
