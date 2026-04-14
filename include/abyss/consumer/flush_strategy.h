#pragma once

#include <chrono>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class FlushStrategy {
 public:
  core::SteadyTime NextFlushTime(const BufferEntry& entry, core::EvictionTTL eviction) const;

 private:
  std::chrono::seconds quiet_threshold_{30};
  std::chrono::seconds safety_margin_{300};
};

}  // namespace abyss::consumer
