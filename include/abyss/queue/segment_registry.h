#pragma once

#include <string>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::queue {

// Read-only, mutation-aware view of a WAL's sealed segments.
class SegmentRegistry {
 public:
  SegmentRegistry() = default;
  virtual ~SegmentRegistry() = default;

  SegmentRegistry(const SegmentRegistry&) = delete;
  SegmentRegistry& operator=(const SegmentRegistry&) = delete;
  SegmentRegistry(SegmentRegistry&&) = delete;
  SegmentRegistry& operator=(SegmentRegistry&&) = delete;

  struct SealedSegmentInfo {
    std::string path;
    core::ShardId shard = 0;
    core::SequenceId base_seq = 0;
    core::SequenceId last_seq = 0;
    core::WallTime created_at;
  };

  // All sealed segments across every shard, point-in-time snapshot.
  virtual std::vector<SealedSegmentInfo> ListSealedSegments() const = 0;

  // Drops the segment from the registry and unlinks its file.
  virtual core::Result<void> RemoveSegment(core::ShardId shard, core::SequenceId base_seq) = 0;
};

}  // namespace abyss::queue
