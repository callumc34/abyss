#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
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

// Synchronous helper that reclaims sealed segments. Per log it goes
// oldest first and stops at the first segment it cannot reclaim, so the
// retained segments stay one contiguous run.
class SegmentReaper {
 public:
  struct ReapOutcome {
    size_t deleted = 0;
    size_t failed = 0;
    std::optional<core::Error> first_error;
    // Seal time of the oldest segment that was eligible but is still
    // on disk after the sweep, whether its removal failed or an earlier
    // segment held it back; the leading indicator that retention
    // stalls.
    std::optional<core::WallTime> oldest_eligible_unreaped;
  };

  SegmentReaper(SegmentRegistry& registry, const OffsetStore& offsets, SegmentReaperConfig config);
  ~SegmentReaper() = default;

  SegmentReaper(const SegmentReaper&) = delete;
  SegmentReaper& operator=(const SegmentReaper&) = delete;
  SegmentReaper(SegmentReaper&&) = delete;
  SegmentReaper& operator=(SegmentReaper&&) = delete;

  // Sweeps every log once.
  [[nodiscard]] core::Result<ReapOutcome> RunOnce();

 private:
  void SweepLog(uint32_t log, core::WallTime now, ReapOutcome& outcome);
  // Every retention consumer's reclaim floor is past every shard it
  // holds.
  bool Released(const SegmentRegistry::SealedSegmentInfo& info) const;

  SegmentRegistry& registry_;
  const OffsetStore& offsets_;
  SegmentReaperConfig config_;
};

}  // namespace abyss::queue
