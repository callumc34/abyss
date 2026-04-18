#pragma once

#include <chrono>

#include "abyss/consumer/buffer_entry.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class FlushStrategy {
 public:
  FlushStrategy() = default;
  FlushStrategy(std::chrono::seconds quiet_threshold, std::chrono::seconds safety_margin,
                double jitter_fraction = 0.1);

  core::SteadyTime NextFlushTime(const BufferEntry& entry, core::EvictionTTL eviction) const;
  std::chrono::milliseconds MaxJitter() const;

 private:
  std::chrono::seconds quiet_threshold_{30};
  std::chrono::seconds safety_margin_{300};
  double jitter_fraction_{0.1};
};

}  // namespace abyss::consumer
