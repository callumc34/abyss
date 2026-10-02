#pragma once

#include <optional>

#include "abyss/core/types.h"

namespace abyss::queue {

// Read-only view of each consumer's PERSISTED committed offset: the floor
// the segment reaper may reclaim up to. Never an in-memory value.
class OffsetStore {
 public:
  OffsetStore() = default;
  virtual ~OffsetStore() = default;

  OffsetStore(const OffsetStore&) = delete;
  OffsetStore& operator=(const OffsetStore&) = delete;
  OffsetStore(OffsetStore&&) = delete;
  OffsetStore& operator=(OffsetStore&&) = delete;

  virtual std::optional<core::SequenceId> Get(core::ConsumerId consumer,
                                              core::ShardId shard) const = 0;
};

}  // namespace abyss::queue
