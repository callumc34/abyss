#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
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
  // Summary of one sweep. A removal failure is recorded and skipped past, never
  // aborted on, so a single stuck segment cannot halt reclamation of the rest.
  struct ReapOutcome {
    size_t deleted = 0;
    size_t failed = 0;
    std::optional<core::Error> first_error;
    // Creation time of the oldest segment that was eligible but is still on
    // disk after the sweep; the leading indicator that retention is stalled.
    std::optional<core::WallTime> oldest_eligible_unreaped;
  };

  SegmentReaper(SegmentRegistry& registry, const OffsetStore& offsets, SegmentReaperConfig config);
  ~SegmentReaper() = default;

  SegmentReaper(const SegmentReaper&) = delete;
  SegmentReaper& operator=(const SegmentReaper&) = delete;
  SegmentReaper(SegmentReaper&&) = delete;
  SegmentReaper& operator=(SegmentReaper&&) = delete;

  // Scan sealed segments once and delete every one that is eligible.
  [[nodiscard]] core::Result<ReapOutcome> RunOnce();

 private:
  bool ShouldDelete(const SegmentRegistry::SealedSegmentInfo& info, core::WallTime now) const;

  SegmentRegistry& registry_;
  const OffsetStore& offsets_;
  SegmentReaperConfig config_;
};

}  // namespace abyss::queue
