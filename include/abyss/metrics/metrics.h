#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "abyss/metrics/names.h"

namespace abyss::metrics {

using LabelPair = std::pair<std::string_view, std::string>;

class Registry;

namespace internal {
void ResetForTesting();
}  // namespace internal

// Opaque non-owning handle. Observation is thread-safe, lock-free, and
// allocation-free. The underlying metric outlives every handle.
class CounterHandle {
 public:
  CounterHandle() noexcept = default;

  void Increment() noexcept;
  void Increment(double amount) noexcept;

 private:
  friend class Registry;
  explicit CounterHandle(void* counter) noexcept : counter_(counter) {}

  void* counter_ = nullptr;
};

class GaugeHandle {
 public:
  GaugeHandle() noexcept = default;

  void Set(double value) noexcept;
  void Increment() noexcept;
  void Increment(double amount) noexcept;
  void Decrement() noexcept;
  void Decrement(double amount) noexcept;

 private:
  friend class Registry;
  explicit GaugeHandle(void* gauge) noexcept : gauge_(gauge) {}

  void* gauge_ = nullptr;
};

class HistogramHandle {
 public:
  HistogramHandle() noexcept = default;

  void Observe(double value) noexcept;

 private:
  friend class Registry;
  explicit HistogramHandle(void* histogram) noexcept : histogram_(histogram) {}

  void* histogram_ = nullptr;
};

// Process-wide metrics registry. Lazy function-local singleton; no explicit
// Init. Registration is serialised; observation through handles is lock-free.
// Conflicting registration of an existing name aborts with a stderr
// diagnostic -- a programmer error that cannot be resolved at runtime.
class Registry {
 public:
  static Registry& Instance() noexcept;

  Registry(const Registry&) = delete;
  Registry& operator=(const Registry&) = delete;
  Registry(Registry&&) = delete;
  Registry& operator=(Registry&&) = delete;

  template <class... Ts>
  // NOLINTNEXTLINE(misc-unused-parameters)
  CounterHandle Counter(const CounterDesc<Ts...>& desc, Ts... values) {
    const std::array<LabelPair, sizeof...(Ts)> labels{
        LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
    return RegisterCounter(desc.name, desc.help, labels);
  }

  template <class... Ts>
  // NOLINTNEXTLINE(misc-unused-parameters)
  GaugeHandle Gauge(const GaugeDesc<Ts...>& desc, Ts... values) {
    const std::array<LabelPair, sizeof...(Ts)> labels{
        LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
    return RegisterGauge(desc.name, desc.help, labels);
  }

  template <class... Ts>
  // NOLINTNEXTLINE(misc-unused-parameters)
  HistogramHandle Histogram(const HistogramDesc<Ts...>& desc, Ts... values) {
    const std::array<LabelPair, sizeof...(Ts)> labels{
        LabelPair{ToStringView(LabelKeyOf<Ts>::value), ToLabelString(values)}...};
    return RegisterHistogram(desc.name, desc.help, labels, desc.buckets);
  }

  // When disabled, Scrape() returns empty; observation still updates state
  // so re-enabling does not lose data.
  void SetEnabled(bool enabled) noexcept;
  bool Enabled() const noexcept;

  // Prometheus text-format payload. Allocates; not for hot paths.
  std::string Scrape() const;

 private:
  Registry();
  ~Registry();  // NOLINT(performance-trivially-destructible)

  CounterHandle RegisterCounter(std::string_view name, std::string_view help,
                                std::span<const LabelPair> labels);
  GaugeHandle RegisterGauge(std::string_view name, std::string_view help,
                            std::span<const LabelPair> labels);
  HistogramHandle RegisterHistogram(std::string_view name, std::string_view help,
                                    std::span<const LabelPair> labels,
                                    std::span<const double> buckets);

  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend void internal::ResetForTesting();
  friend std::optional<double> RegistryFindCounter(std::string_view, std::span<const LabelPair>);
  friend std::optional<double> RegistryFindGauge(std::string_view, std::span<const LabelPair>);
  friend std::optional<uint64_t> RegistryFindHistogramCount(std::string_view,
                                                            std::span<const LabelPair>);
  friend std::optional<double> RegistryFindHistogramSum(std::string_view,
                                                        std::span<const LabelPair>);
};

std::optional<double> RegistryFindCounter(std::string_view name, std::span<const LabelPair> labels);
std::optional<double> RegistryFindGauge(std::string_view name, std::span<const LabelPair> labels);
std::optional<uint64_t> RegistryFindHistogramCount(std::string_view name,
                                                   std::span<const LabelPair> labels);
std::optional<double> RegistryFindHistogramSum(std::string_view name,
                                               std::span<const LabelPair> labels);

}  // namespace abyss::metrics
