#pragma once

#include <cstdint>
#include <span>

#include "abyss/core/ops.h"
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
  HotStore() = default;
  virtual ~HotStore() = default;
  HotStore(const HotStore&) = delete;
  HotStore& operator=(const HotStore&) = delete;
  HotStore(HotStore&&) = delete;
  HotStore& operator=(HotStore&&) = delete;

  virtual Result<RespValue> Exec(const ops::ReadOp& op) = 0;
  virtual Result<void> Apply(const ops::WriteOp& op, EvictionTTL eviction) = 0;
  virtual Result<void> ApplyBatch(std::span<const ops::WriteOp> ops, EvictionTTL eviction) = 0;

  virtual Result<MemoryStats> Stats() = 0;
  virtual Result<void> Flush() = 0;
};

}  // namespace abyss::core
