#pragma once

#include <cstdint>
#include <span>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct MemoryStats {
  uint64_t used_bytes = 0;
  uint64_t key_count = 0;
  uint64_t eviction_count = 0;
};

class HotStore {
 public:
  virtual ~HotStore() = default;

  virtual Result<RespValue> Exec(const RespCommand& cmd) = 0;
  virtual Result<void> Apply(const RespCommand& cmd, EvictionTTL eviction) = 0;
  virtual Result<void> ApplyBatch(std::span<const RespCommand> cmds, EvictionTTL eviction) = 0;

  virtual Result<MemoryStats> Stats() = 0;
  virtual Result<void> Flush() = 0;
};

}  // namespace abyss::core
