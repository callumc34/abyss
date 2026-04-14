#include "abyss/consumer/flush_strategy.h"

#include <algorithm>

namespace abyss::consumer {

core::SteadyTime FlushStrategy::NextFlushTime(const BufferEntry& entry,
                                              core::EvictionTTL eviction) const {
  auto quiet_deadline = entry.last_modified + quiet_threshold_;
  auto eviction_deadline = entry.first_seen + eviction - safety_margin_;
  return std::min(quiet_deadline, eviction_deadline);
}

}  // namespace abyss::consumer
