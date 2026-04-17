#pragma once

#include <cstddef>
#include <cstdint>

#include "abyss/core/hot_store.h"

namespace abyss::hot {

struct HashmapConfig {
  size_t max_memory_bytes = 4294967296;
  uint32_t shard_count = 64;
};

class HashmapStore : public core::HotStore {
 public:
  explicit HashmapStore(HashmapConfig config);
  ~HashmapStore() override;

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) override;
  core::Result<void> Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction) override;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::EvictionTTL eviction) override;
  core::Result<core::MemoryStats> Stats() override;
  core::Result<void> Flush() override;

 private:
  HashmapConfig config_;
};

}  // namespace abyss::hot
