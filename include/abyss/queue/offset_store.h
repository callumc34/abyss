#pragma once

#include <optional>

#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::queue {

// Persistent record of the highest sequence ID each consumer has acknowledged.
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

  virtual core::Result<void> Set(core::ConsumerId consumer, core::ShardId shard,
                                 core::SequenceId seq) = 0;
};

}  // namespace abyss::queue
