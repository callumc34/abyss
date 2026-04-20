#include "abyss/metrics/metrics.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abyss/core/thread_annotations.h"

#if ABYSS_WITH_METRICS
#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>
#endif

namespace abyss::metrics {

#if ABYSS_WITH_METRICS

namespace {

enum class FamilyKind : uint8_t { kCounter, kGauge, kHistogram };

struct FamilyEntry {
  FamilyKind kind;
  std::string help;
  std::vector<std::string> label_keys;
  std::vector<double> buckets;
  void* family = nullptr;
};

[[noreturn]] void Die(std::string_view msg) {
  std::fputs("abyss::metrics: ", stderr);
  std::fwrite(msg.data(), 1, msg.size(), stderr);
  std::fputc('\n', stderr);
  std::abort();
}

std::vector<std::string> LabelKeysOf(std::span<const LabelPair> labels) {
  std::vector<std::string> keys;
  keys.reserve(labels.size());
  for (const auto& p : labels) keys.emplace_back(p.first);
  return keys;
}

std::map<std::string, std::string> LabelMapOf(std::span<const LabelPair> labels) {
  std::map<std::string, std::string> m;
  for (const auto& p : labels) m.emplace(std::string(p.first), p.second);
  return m;
}

bool SameKeys(const std::vector<std::string>& a, std::span<const LabelPair> b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (a[i] != b[i].first) return false;
  }
  return true;
}

bool SameBuckets(const std::vector<double>& a, std::span<const double> b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

}  // namespace

struct Registry::Impl {
  mutable std::mutex mu;
  std::shared_ptr<prometheus::Registry> backend ABYSS_GUARDED_BY(mu) =
      std::make_shared<prometheus::Registry>();
  std::unordered_map<std::string, FamilyEntry> families ABYSS_GUARDED_BY(mu);
  std::atomic<bool> enabled{true};
};

// NOLINTNEXTLINE(modernize-use-equals-default)
Registry::Registry() : impl_(std::make_unique<Impl>()) {}
Registry::~Registry() = default;

Registry& Registry::Instance() noexcept {
  static Registry instance;
  return instance;
}

void Registry::SetEnabled(bool enabled) noexcept {
  impl_->enabled.store(enabled, std::memory_order_relaxed);
}

bool Registry::Enabled() const noexcept { return impl_->enabled.load(std::memory_order_relaxed); }

std::string Registry::Scrape() const {
  if (!Enabled()) return {};
  const std::lock_guard<std::mutex> lk(impl_->mu);
  const prometheus::TextSerializer serializer;
  return serializer.Serialize(impl_->backend->Collect());
}

CounterHandle Registry::RegisterCounter(std::string_view name, std::string_view help,
                                        std::span<const LabelPair> labels) {
  const std::lock_guard<std::mutex> lk(impl_->mu);
  const std::string name_key(name);
  auto it = impl_->families.find(name_key);
  prometheus::Family<prometheus::Counter>* family = nullptr;
  if (it != impl_->families.end()) {
    if (it->second.kind != FamilyKind::kCounter || it->second.help != help ||
        !SameKeys(it->second.label_keys, labels)) {
      Die("conflicting registration for counter '" + name_key + "'");
    }
    family = static_cast<prometheus::Family<prometheus::Counter>*>(it->second.family);
  } else {
    family = &prometheus::BuildCounter()
                  .Name(name_key)
                  .Help(std::string(help))
                  .Register(*impl_->backend);
    FamilyEntry entry;
    entry.kind = FamilyKind::kCounter;
    entry.help = std::string(help);
    entry.label_keys = LabelKeysOf(labels);
    entry.family = family;
    impl_->families.emplace(name_key, std::move(entry));
  }
  auto& counter = family->Add(LabelMapOf(labels));
  return CounterHandle(&counter);
}

GaugeHandle Registry::RegisterGauge(std::string_view name, std::string_view help,
                                    std::span<const LabelPair> labels) {
  const std::lock_guard<std::mutex> lk(impl_->mu);
  const std::string name_key(name);
  auto it = impl_->families.find(name_key);
  prometheus::Family<prometheus::Gauge>* family = nullptr;
  if (it != impl_->families.end()) {
    if (it->second.kind != FamilyKind::kGauge || it->second.help != help ||
        !SameKeys(it->second.label_keys, labels)) {
      Die("conflicting registration for gauge '" + name_key + "'");
    }
    family = static_cast<prometheus::Family<prometheus::Gauge>*>(it->second.family);
  } else {
    family =
        &prometheus::BuildGauge().Name(name_key).Help(std::string(help)).Register(*impl_->backend);
    FamilyEntry entry;
    entry.kind = FamilyKind::kGauge;
    entry.help = std::string(help);
    entry.label_keys = LabelKeysOf(labels);
    entry.family = family;
    impl_->families.emplace(name_key, std::move(entry));
  }
  auto& gauge = family->Add(LabelMapOf(labels));
  return GaugeHandle(&gauge);
}

HistogramHandle Registry::RegisterHistogram(std::string_view name, std::string_view help,
                                            std::span<const LabelPair> labels,
                                            std::span<const double> buckets) {
  const std::lock_guard<std::mutex> lk(impl_->mu);
  const std::string name_key(name);
  auto it = impl_->families.find(name_key);
  prometheus::Family<prometheus::Histogram>* family = nullptr;
  if (it != impl_->families.end()) {
    if (it->second.kind != FamilyKind::kHistogram || it->second.help != help ||
        !SameKeys(it->second.label_keys, labels) || !SameBuckets(it->second.buckets, buckets)) {
      Die("conflicting registration for histogram '" + name_key + "'");
    }
    family = static_cast<prometheus::Family<prometheus::Histogram>*>(it->second.family);
  } else {
    family = &prometheus::BuildHistogram()
                  .Name(name_key)
                  .Help(std::string(help))
                  .Register(*impl_->backend);
    FamilyEntry entry;
    entry.kind = FamilyKind::kHistogram;
    entry.help = std::string(help);
    entry.label_keys = LabelKeysOf(labels);
    entry.buckets.assign(buckets.begin(), buckets.end());
    entry.family = family;
    impl_->families.emplace(name_key, std::move(entry));
  }
  auto& hist = family->Add(LabelMapOf(labels),
                           prometheus::Histogram::BucketBoundaries{buckets.begin(), buckets.end()});
  return HistogramHandle(&hist);
}

void CounterHandle::Increment() noexcept {
  if (counter_ != nullptr) static_cast<prometheus::Counter*>(counter_)->Increment();
}

void CounterHandle::Increment(double amount) noexcept {
  if (counter_ != nullptr) static_cast<prometheus::Counter*>(counter_)->Increment(amount);
}

void GaugeHandle::Set(double value) noexcept {
  if (gauge_ != nullptr) static_cast<prometheus::Gauge*>(gauge_)->Set(value);
}

void GaugeHandle::Increment() noexcept {
  if (gauge_ != nullptr) static_cast<prometheus::Gauge*>(gauge_)->Increment();
}

void GaugeHandle::Increment(double amount) noexcept {
  if (gauge_ != nullptr) static_cast<prometheus::Gauge*>(gauge_)->Increment(amount);
}

void GaugeHandle::Decrement() noexcept {
  if (gauge_ != nullptr) static_cast<prometheus::Gauge*>(gauge_)->Decrement();
}

void GaugeHandle::Decrement(double amount) noexcept {
  if (gauge_ != nullptr) static_cast<prometheus::Gauge*>(gauge_)->Decrement(amount);
}

void HistogramHandle::Observe(double value) noexcept {
  if (histogram_ != nullptr) static_cast<prometheus::Histogram*>(histogram_)->Observe(value);
}

namespace internal {

void ResetForTesting() {
  Registry& r = Registry::Instance();
  const std::lock_guard<std::mutex> lk(r.impl_->mu);
  r.impl_->families.clear();
  r.impl_->backend = std::make_shared<prometheus::Registry>();
  r.impl_->enabled.store(true, std::memory_order_relaxed);
}

}  // namespace internal

std::optional<double> RegistryFindCounter(std::string_view name,
                                          std::span<const LabelPair> labels) {
  Registry& r = Registry::Instance();
  const std::lock_guard<std::mutex> lk(r.impl_->mu);
  const auto it = r.impl_->families.find(std::string(name));
  if (it == r.impl_->families.end() || it->second.kind != FamilyKind::kCounter) return std::nullopt;
  const auto map = LabelMapOf(labels);
  auto* family = static_cast<prometheus::Family<prometheus::Counter>*>(it->second.family);
  if (!family->Has(map)) return std::nullopt;
  return family->Add(map).Value();
}

std::optional<double> RegistryFindGauge(std::string_view name, std::span<const LabelPair> labels) {
  Registry& r = Registry::Instance();
  const std::lock_guard<std::mutex> lk(r.impl_->mu);
  const auto it = r.impl_->families.find(std::string(name));
  if (it == r.impl_->families.end() || it->second.kind != FamilyKind::kGauge) return std::nullopt;
  const auto map = LabelMapOf(labels);
  auto* family = static_cast<prometheus::Family<prometheus::Gauge>*>(it->second.family);
  if (!family->Has(map)) return std::nullopt;
  return family->Add(map).Value();
}

std::optional<uint64_t> RegistryFindHistogramCount(std::string_view name,
                                                   std::span<const LabelPair> labels) {
  Registry& r = Registry::Instance();
  const std::lock_guard<std::mutex> lk(r.impl_->mu);
  const auto it = r.impl_->families.find(std::string(name));
  if (it == r.impl_->families.end() || it->second.kind != FamilyKind::kHistogram) {
    return std::nullopt;
  }
  const auto map = LabelMapOf(labels);
  auto* family = static_cast<prometheus::Family<prometheus::Histogram>*>(it->second.family);
  if (!family->Has(map)) return std::nullopt;
  const auto metric =
      family
          ->Add(map, prometheus::Histogram::BucketBoundaries{it->second.buckets.begin(),
                                                             it->second.buckets.end()})
          .Collect();
  return metric.histogram.sample_count;
}

std::optional<double> RegistryFindHistogramSum(std::string_view name,
                                               std::span<const LabelPair> labels) {
  Registry& r = Registry::Instance();
  const std::lock_guard<std::mutex> lk(r.impl_->mu);
  const auto it = r.impl_->families.find(std::string(name));
  if (it == r.impl_->families.end() || it->second.kind != FamilyKind::kHistogram) {
    return std::nullopt;
  }
  const auto map = LabelMapOf(labels);
  auto* family = static_cast<prometheus::Family<prometheus::Histogram>*>(it->second.family);
  if (!family->Has(map)) return std::nullopt;
  const auto metric =
      family
          ->Add(map, prometheus::Histogram::BucketBoundaries{it->second.buckets.begin(),
                                                             it->second.buckets.end()})
          .Collect();
  return metric.histogram.sample_sum;
}

#else  // !ABYSS_WITH_METRICS

struct Registry::Impl {
  std::atomic<bool> enabled{false};
};

Registry::Registry() : impl_(std::make_unique<Impl>()) {}
Registry::~Registry() = default;

Registry& Registry::Instance() noexcept {
  static Registry instance;
  return instance;
}

void Registry::SetEnabled(bool) noexcept {}
bool Registry::Enabled() const noexcept { return false; }
std::string Registry::Scrape() const { return {}; }

CounterHandle Registry::RegisterCounter(std::string_view, std::string_view,
                                        std::span<const LabelPair>) {
  return {};
}
GaugeHandle Registry::RegisterGauge(std::string_view, std::string_view,
                                    std::span<const LabelPair>) {
  return {};
}
HistogramHandle Registry::RegisterHistogram(std::string_view, std::string_view,
                                            std::span<const LabelPair>, std::span<const double>) {
  return {};
}

void CounterHandle::Increment() noexcept {}
void CounterHandle::Increment(double) noexcept {}
void GaugeHandle::Set(double) noexcept {}
void GaugeHandle::Increment() noexcept {}
void GaugeHandle::Increment(double) noexcept {}
void GaugeHandle::Decrement() noexcept {}
void GaugeHandle::Decrement(double) noexcept {}
void HistogramHandle::Observe(double) noexcept {}

namespace internal {
void ResetForTesting() {}
}  // namespace internal

std::optional<double> RegistryFindCounter(std::string_view, std::span<const LabelPair>) {
  return std::nullopt;
}
std::optional<double> RegistryFindGauge(std::string_view, std::span<const LabelPair>) {
  return std::nullopt;
}
std::optional<uint64_t> RegistryFindHistogramCount(std::string_view, std::span<const LabelPair>) {
  return std::nullopt;
}
std::optional<double> RegistryFindHistogramSum(std::string_view, std::span<const LabelPair>) {
  return std::nullopt;
}

#endif  // ABYSS_WITH_METRICS

}  // namespace abyss::metrics
