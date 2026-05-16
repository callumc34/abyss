#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::hot {

struct ShardedHotStoreConfig {
  size_t max_memory_bytes = 4294967296;
  uint32_t shard_count = 64;
  // Borrowed from the server's single EvictionPolicy. Must outlive the store.
  // Nullable for tests that don't exercise eviction (DefaultPolicy is used).
  const core::EvictionPolicy* eviction_policy = nullptr;
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

  core::Result<core::RespValue> Exec(
      const core::ops::ReadOp& op, std::optional<core::Duration> deadline = std::nullopt) override;
  core::Result<core::RespValue> Apply(const core::ops::WriteOp& op) override;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops) override;
  core::Result<core::MemoryStats> Stats() override;
  core::Result<void> Flush() override;

  // Refreshes the deadline for every buffered access, using the per-key
  // eviction cached on each Entry at Apply time. See ADP-002 §Eviction.
  void DrainAccessBuffers(core::SteadyTime now);
  size_t EvictExpired(core::SteadyTime now);

  uint32_t shard_count() const { return config_.shard_count; }

 private:
  struct Shard {
    mutable std::shared_mutex mutex;
    SingleShardStore store;
    mutable std::mutex access_mutex;
    std::vector<std::string> access_buffer;

    explicit Shard(SingleShardConfig config) : store(std::move(config)) {}
  };

  Shard& ShardFor(std::string_view key);
  core::EvictionTTL ResolveEviction(std::string_view key) const;

  core::Result<core::RespValue> ExecExists(const core::ops::Exists& op);
  core::Result<core::RespValue> ApplyDel(const core::ops::Del& op);

  ShardedHotStoreConfig config_;
  // Fallback when config_.eviction_policy is null; keeps Resolve() infallible.
  core::EvictionPolicy default_policy_;
  std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace abyss::hot
