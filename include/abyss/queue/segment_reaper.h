#pragma once

#include <chrono>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/offset_store.h"
#include "abyss/queue/segment_registry.h"

namespace abyss::queue {

struct SegmentReaperConfig {
  std::vector<core::ConsumerId> consumers;
  std::chrono::seconds min_retention{86400};
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

// Synchronous helper that deletes sealed segments.
class SegmentReaper {
 public:
  SegmentReaper(SegmentRegistry& registry, const OffsetStore& offsets, SegmentReaperConfig config);
  ~SegmentReaper() = default;

  SegmentReaper(const SegmentReaper&) = delete;
  SegmentReaper& operator=(const SegmentReaper&) = delete;
  SegmentReaper(SegmentReaper&&) = delete;
  SegmentReaper& operator=(SegmentReaper&&) = delete;

  // Scan sealed segments once and delete every one that is eligible.
  core::Result<size_t> RunOnce();

 private:
  bool ShouldDelete(const SegmentRegistry::SealedSegmentInfo& info, core::WallTime now) const;

  SegmentRegistry& registry_;
  const OffsetStore& offsets_;
  SegmentReaperConfig config_;
};

}  // namespace abyss::queue
