#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/log.h"

namespace abyss::queue {

// Read-only, mutation-aware view of a WAL's sealed segments, per log.
class SegmentRegistry {
 public:
  SegmentRegistry() = default;
  virtual ~SegmentRegistry() = default;

  SegmentRegistry(const SegmentRegistry&) = delete;
  SegmentRegistry& operator=(const SegmentRegistry&) = delete;
  SegmentRegistry(SegmentRegistry&&) = delete;
  SegmentRegistry& operator=(SegmentRegistry&&) = delete;

  struct SealedSegmentInfo {
    uint32_t log = 0;
    uint64_t ordinal = 0;
    // Every shard it holds frames for, with its seq range there.
    std::vector<SegmentShardRange> shards;
    // Retention ages it from here; non-decreasing within a log.
    core::WallTime sealed_at;
  };

  virtual uint32_t LogCount() const = 0;
  // Up to `max_count` of `log`'s sealed segments, oldest first.
  virtual std::vector<SealedSegmentInfo> ListSealedSegments(uint32_t log,
                                                            std::size_t max_count) const = 0;
  // Reclaims `log`'s oldest sealed segment, `ordinal`. Refused for any
  // other, and while an earlier reclaim's file is still being removed.
  virtual core::Result<void> RemoveSegment(uint32_t log, uint64_t ordinal) = 0;
};

}  // namespace abyss::queue
