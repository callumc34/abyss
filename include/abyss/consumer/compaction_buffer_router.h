#pragma once

#include <optional>
#include <string_view>

#include "abyss/consumer/compacted_state.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"

namespace abyss::consumer {

// Routes reads to the per-shard compaction buffers; concrete pools shard by key.
class CompactionBufferRouter {
 public:
  CompactionBufferRouter() = default;
  virtual ~CompactionBufferRouter() = default;
  CompactionBufferRouter(const CompactionBufferRouter&) = delete;
  CompactionBufferRouter& operator=(const CompactionBufferRouter&) = delete;
  CompactionBufferRouter(CompactionBufferRouter&&) = delete;
  CompactionBufferRouter& operator=(CompactionBufferRouter&&) = delete;

  // CompactionBuffer::Snapshot on `shard`'s buffer, which owns `key`.
  virtual std::optional<CompactedState> Snapshot(core::ShardId shard,
                                                 std::string_view key) const = 0;

  // Memory backpressure's wait for room (core::WaitList): links
  // `waiter` until `shard` has drained past `past`, waking one waiter
  // per advance, and asks the drain to run now. Outcomes as
  // WaitList::Register's; kUnavailable once the shard's consumer
  // stopped.
  virtual std::optional<core::Result<void>> WhenDrainedPast(core::ShardId shard,
                                                            core::SequenceId past,
                                                            core::Waiter& waiter) = 0;
  virtual bool CancelDrainWait(core::ShardId shard, core::Waiter& waiter) noexcept = 0;
  virtual void FinishDrainWait(core::ShardId shard, core::Waiter& waiter) noexcept = 0;
};

}  // namespace abyss::consumer
