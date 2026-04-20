#include "abyss/consumer/flush_strategy.h"

namespace abyss::consumer {

FlushStrategy::FlushStrategy(std::chrono::seconds quiet_threshold,
                             std::chrono::seconds safety_margin, double jitter_fraction)
    : quiet_threshold_(quiet_threshold),
      safety_margin_(safety_margin),
      jitter_fraction_(jitter_fraction) {}

NextFlush FlushStrategy::NextFlushTime(const BufferEntry& entry, core::EvictionTTL eviction) const {
  const auto quiet_deadline = entry.last_modified + quiet_threshold_;
  const auto eviction_deadline = entry.first_seen + eviction - safety_margin_;
  if (quiet_deadline <= eviction_deadline) {
    return {quiet_deadline, FlushTrigger::kQuiet};
  }
  return {eviction_deadline, FlushTrigger::kDeadline};
}

std::chrono::milliseconds FlushStrategy::MaxJitter() const {
  auto quiet_ms = std::chrono::duration_cast<std::chrono::milliseconds>(quiet_threshold_);
  return std::chrono::milliseconds{
      static_cast<int64_t>(static_cast<double>(quiet_ms.count()) * jitter_fraction_)};
}

}  // namespace abyss::consumer
