#pragma once

#include <optional>
#include <string_view>

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
};

}  // namespace abyss::consumer
