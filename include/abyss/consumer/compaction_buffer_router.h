#pragma once

#include <chrono>
#include <optional>
#include <string_view>

#include "abyss/consumer/compacted_state.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/ops.h"
#include "abyss/core/reader.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

// Routes reads to the per-shard compaction buffers; concrete pools shard by key.
class CompactionBufferRouter : public core::Reader {
 public:
  CompactionBufferRouter() = default;
  ~CompactionBufferRouter() override = default;
  CompactionBufferRouter(const CompactionBufferRouter&) = delete;
  CompactionBufferRouter& operator=(const CompactionBufferRouter&) = delete;
  CompactionBufferRouter(CompactionBufferRouter&&) = delete;
  CompactionBufferRouter& operator=(CompactionBufferRouter&&) = delete;

  core::Result<core::RespValue> Exec(
      const core::ops::ReadOp& op,
      std::optional<core::Duration> deadline = std::nullopt) override = 0;

  virtual core::Result<core::RespValue> Read(std::string_view key) const = 0;

  virtual BufferKeyPresence Probe(std::string_view key) const = 0;

  virtual HashOverlay HashOverlayFor(std::string_view key) const = 0;

  // CompactionBuffer::Snapshot on `shard`'s buffer, which owns `key`.
  virtual std::optional<CompactedState> Snapshot(core::ShardId shard,
                                                 std::string_view key) const = 0;

  // Blocks until the shard's cold consumer has drained through `target_seq`,
  // or `timeout` elapses. Returns true on catch-up, false on timeout.
  virtual bool WaitForDrainedSeq(core::ShardId shard, core::SequenceId target_seq,
                                 std::chrono::milliseconds timeout) = 0;
};

}  // namespace abyss::consumer
