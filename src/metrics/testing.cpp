#include "abyss/metrics/testing.h"

#include "abyss/metrics/metrics.h"

namespace abyss::metrics::testing {

void Reset() { internal::ResetForTesting(); }

std::optional<double> GetCounterValue(std::string_view name, std::span<const LabelPair> labels) {
  return RegistryFindCounter(name, labels);
}

std::optional<double> GetGaugeValue(std::string_view name, std::span<const LabelPair> labels) {
  return RegistryFindGauge(name, labels);
}

std::optional<uint64_t> GetHistogramCount(std::string_view name,
                                          std::span<const LabelPair> labels) {
  return RegistryFindHistogramCount(name, labels);
}

std::optional<double> GetHistogramSum(std::string_view name, std::span<const LabelPair> labels) {
  return RegistryFindHistogramSum(name, labels);
}

}  // namespace abyss::metrics::testing
