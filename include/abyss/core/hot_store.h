#pragma once

#include <cstdint>
#include <optional>
#include <span>

#include "abyss/core/ops.h"
#include "abyss/core/reader.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct MemoryStats {
  uint64_t used_bytes = 0;
  uint64_t key_count = 0;
  uint64_t eviction_count = 0;
};

class HotStore : public Reader {
 public:
  HotStore() = default;
  ~HotStore() override = default;
  HotStore(const HotStore&) = delete;
  HotStore& operator=(const HotStore&) = delete;
  HotStore(HotStore&&) = delete;
  HotStore& operator=(HotStore&&) = delete;

  // Deadline is accepted for interface uniformity; the in-memory hot tier
  // serves reads in microseconds and ignores the parameter.
  Result<RespValue> Exec(const ops::ReadOp& op,
                         std::optional<Duration> deadline = std::nullopt) override = 0;

  // Returns the typed reply the client observes (+OK for SET, count for SADD/etc.).
  // Per-key eviction (if any) is resolved internally by the implementation.
  virtual Result<RespValue> Apply(const ops::WriteOp& op) = 0;
  virtual Result<void> ApplyBatch(std::span<const ops::WriteOp> ops) = 0;

  virtual Result<MemoryStats> Stats() = 0;
  virtual Result<void> Wipe() = 0;
};

}  // namespace abyss::core
