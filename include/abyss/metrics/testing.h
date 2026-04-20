#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::metrics::testing {

// Replaces the registry's internal backend with a fresh instance. Handles
// issued before Reset orphan onto the prior backend; tests re-register.
// Not safe to call concurrently with registration or observation.
void Reset();

// Returns nullopt when the series was never registered with these labels.
// Each accessor locks briefly against registration; never call from the hot path.
std::optional<double> GetCounterValue(std::string_view name, std::span<const LabelPair> labels);
std::optional<double> GetGaugeValue(std::string_view name, std::span<const LabelPair> labels);
std::optional<uint64_t> GetHistogramCount(std::string_view name, std::span<const LabelPair> labels);
std::optional<double> GetHistogramSum(std::string_view name, std::span<const LabelPair> labels);

template <class... Ts>
// NOLINTNEXTLINE(misc-unused-parameters)
std::optional<double> GetCounterValue(const CounterDesc<Ts...>& desc, Ts... values) {
  const std::array<LabelPair, sizeof...(Ts)> labels{
      LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
  return GetCounterValue(desc.name, labels);
}

template <class... Ts>
// NOLINTNEXTLINE(misc-unused-parameters)
std::optional<double> GetGaugeValue(const GaugeDesc<Ts...>& desc, Ts... values) {
  const std::array<LabelPair, sizeof...(Ts)> labels{
      LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
  return GetGaugeValue(desc.name, labels);
}

template <class... Ts>
// NOLINTNEXTLINE(misc-unused-parameters)
std::optional<uint64_t> GetHistogramCount(const HistogramDesc<Ts...>& desc, Ts... values) {
  const std::array<LabelPair, sizeof...(Ts)> labels{
      LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
  return GetHistogramCount(desc.name, labels);
}

template <class... Ts>
// NOLINTNEXTLINE(misc-unused-parameters)
std::optional<double> GetHistogramSum(const HistogramDesc<Ts...>& desc, Ts... values) {
  const std::array<LabelPair, sizeof...(Ts)> labels{
      LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
  return GetHistogramSum(desc.name, labels);
}

}  // namespace abyss::metrics::testing
