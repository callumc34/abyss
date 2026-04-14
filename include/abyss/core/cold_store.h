#pragma once

#include <cstdint>
#include <span>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::core {

struct StorageStats {
  uint64_t disk_bytes = 0;
  uint64_t key_count = 0;
};

class ColdStore {
 public:
  virtual ~ColdStore() = default;

  virtual Result<RespValue> Exec(const RespCommand& cmd) = 0;
  virtual Result<void> ApplyBatch(std::span<const RespCommand> cmds) = 0;

  virtual Result<StorageStats> Stats() = 0;
  virtual Result<void> Compact() = 0;
};

}  // namespace abyss::core
