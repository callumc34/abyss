#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/hot_store.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::hot {

struct ShardedHotStoreConfig {
  size_t max_memory_bytes = 4294967296;
  uint32_t shard_count = 64;
  core::SteadyClockFn steady_clock = core::DefaultSteadyClock;
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

class ShardedHotStore : public core::HotStore {
 public:
  explicit ShardedHotStore(ShardedHotStoreConfig config);
  ~ShardedHotStore() override;
  ShardedHotStore(const ShardedHotStore&) = delete;
  ShardedHotStore& operator=(const ShardedHotStore&) = delete;
  ShardedHotStore(ShardedHotStore&&) = delete;
  ShardedHotStore& operator=(ShardedHotStore&&) = delete;

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) override;
  core::Result<void> Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction) override;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::EvictionTTL eviction) override;
  core::Result<core::MemoryStats> Stats() override;
  core::Result<void> Flush() override;

  void DrainAccessBuffers(core::SteadyTime now, core::EvictionTTL eviction);
  size_t EvictExpired(core::SteadyTime now);

  uint32_t shard_count() const { return config_.shard_count; }

 private:
  struct Shard {
    mutable std::shared_mutex mutex;
    SingleShardStore store;
    mutable std::mutex access_mutex;
    std::vector<std::string> access_buffer;

    explicit Shard(SingleShardConfig config) : store(config) {}
  };

  Shard& ShardFor(std::string_view key);

  core::Result<core::RespValue> ExecMultiStringGet(const core::ops::MultiStringGet& op);
  core::Result<core::RespValue> ExecExists(const core::ops::Exists& op);

  core::Result<void> ApplyDel(const core::ops::Del& op);
  core::Result<void> ApplyMultiStringSet(const core::ops::MultiStringSet& op,
                                         core::EvictionTTL eviction);

  ShardedHotStoreConfig config_;
  std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace abyss::hot
