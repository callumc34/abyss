#pragma once

#include <cstdint>
#include <span>

#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::core {

struct StorageStats {
  uint64_t disk_bytes = 0;
  uint64_t key_count = 0;
};

class ColdStore {
 public:
  ColdStore() = default;
  virtual ~ColdStore() = default;
  ColdStore(const ColdStore&) = delete;
  ColdStore& operator=(const ColdStore&) = delete;
  ColdStore(ColdStore&&) = delete;
  ColdStore& operator=(ColdStore&&) = delete;

  virtual Result<RespValue> Exec(const ops::ReadOp& op) = 0;
  virtual Result<void> ApplyBatch(std::span<const ops::WriteOp> ops) = 0;

  virtual Result<StorageStats> Stats() = 0;
  virtual Result<void> Compact() = 0;
};

}  // namespace abyss::core
