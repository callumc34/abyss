#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include "abyss/core/ops.h"
#include "abyss/core/reader.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct StorageStats {
  uint64_t disk_bytes = 0;
  uint64_t key_count = 0;
};

class ColdStore : public Reader {
 public:
  ColdStore() = default;
  ~ColdStore() override = default;
  ColdStore(const ColdStore&) = delete;
  ColdStore& operator=(const ColdStore&) = delete;
  ColdStore(ColdStore&&) = delete;
  ColdStore& operator=(ColdStore&&) = delete;

  Result<RespValue> Exec(const ops::ReadOp& op,
                         std::optional<Duration> deadline = std::nullopt) override = 0;

  virtual Result<void> ApplyBatch(std::span<const ops::WriteOp> ops) = 0;

  // Drops every key in the cold store. Backends shared across shards must
  // make this idempotent — every per-shard Flush funnels through the same wipe.
  virtual Result<void> Wipe() = 0;

  virtual Result<StorageStats> Stats() = 0;
  virtual Result<void> Compact() = 0;

  // Returns the RESP command that reconstructs `key`'s hot-side view from
  // cold, or nullopt if the key does not exist or its type isn't promotable
  // in this implementation. Preserves absolute TTL.
  virtual Result<std::optional<RespCommand>> GetPromotionCommand(std::string_view key) = 0;

  // Server calls Start() once after recovery completes to enable any
  // backend-internal background work (TTL sweeping, prefetch, etc.). Stop()
  // is called during shutdown before the store is destroyed. Both are
  // idempotent. Backends with no background work leave the defaults.
  virtual Result<void> Start() { return {}; }
  virtual Result<void> Stop() { return {}; }
};

}  // namespace abyss::core
