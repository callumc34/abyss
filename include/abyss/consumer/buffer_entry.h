#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "abyss/consumer/compacted_state.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

enum class FlushTrigger : uint8_t { kQuiet = 0, kDeadline = 1 };

struct BufferEntry {
  std::string key;
  CompactedState state;
  core::SteadyTime first_seen;
  core::SteadyTime last_modified;
  uint64_t write_count = 0;
  core::EvictionTTL eviction{0};
  std::chrono::milliseconds jitter_offset{0};
  core::SequenceId first_seen_seq = 0;
  FlushTrigger last_trigger = FlushTrigger::kQuiet;
  // Scheduled time of this key's currently-live flush-heap entry. Absorb skips
  // pushing a duplicate HeapEntry when the recomputed schedule is unchanged,
  // bounding heap growth per key in the hot-key case (COLDC-4).
  core::SteadyTime scheduled_in_heap_{};
};

}  // namespace abyss::consumer
