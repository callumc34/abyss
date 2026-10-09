#pragma once

#include <chrono>
#include <optional>
#include <string_view>

#include "abyss/consumer/compacted_state.h"
#include "abyss/core/types.h"

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

  // Blocks until the shard's cold consumer has drained through `target_seq`,
  // or `timeout` elapses. Returns true on catch-up, false on timeout.
  virtual bool WaitForDrainedSeq(core::ShardId shard, core::SequenceId target_seq,
                                 std::chrono::milliseconds timeout) = 0;
};

}  // namespace abyss::consumer
