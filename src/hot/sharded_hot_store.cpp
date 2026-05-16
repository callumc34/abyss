#include "abyss/hot/sharded_hot_store.h"

#include <utility>

#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.hot.store")

namespace abyss::hot {

ShardedHotStore::ShardedHotStore(ShardedHotStoreConfig config) : config_(std::move(config)) {
  SingleShardConfig shard_config{
      .max_memory_bytes = config_.max_memory_bytes / config_.shard_count,
      .steady_clock = config_.steady_clock,
      .wall_clock = config_.wall_clock,
  };
  shards_.reserve(config_.shard_count);
  for (uint32_t i = 0; i < config_.shard_count; ++i) {
    shards_.push_back(std::make_unique<Shard>(shard_config));
  }
  ABYSS_LOG_INFO("hot store opened", {"shards", static_cast<int64_t>(config_.shard_count)},
                 {"max_memory_bytes", static_cast<uint64_t>(config_.max_memory_bytes)});
}

ShardedHotStore::~ShardedHotStore() = default;

ShardedHotStore::Shard& ShardedHotStore::ShardFor(std::string_view key) {
  return *shards_[core::ComputeShard(key, config_.shard_count)];
}

core::EvictionTTL ShardedHotStore::ResolveEviction(std::string_view key) const {
  const auto& policy =
      config_.eviction_policy != nullptr ? *config_.eviction_policy : default_policy_;
  return policy.Resolve(key);
}

core::Result<core::RespValue> ShardedHotStore::Exec(const core::ops::ReadOp& op,
                                                    std::optional<core::Duration> /*deadline*/)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return ExecExists(o);
        } else {
          auto key = o.key;
          auto& shard = ShardFor(key);
          std::shared_lock lock(shard.mutex);
          auto result = shard.store.Exec(op);
          if (result.has_value()) {
            std::scoped_lock access_lock(shard.access_mutex);
            shard.access_buffer.emplace_back(key);
          }
          return result;
        }
      },
      op);
}

// Engine fan-out always issues Exists{single key}. The vector shape and the
// cross-shard grouping survive only to keep Exists itself a stable internal
// probe — a single-element keys vector executes one iteration.
core::Result<core::RespValue> ShardedHotStore::ExecExists(const core::ops::Exists& op) {
  int64_t total = 0;
  for (auto key : op.keys) {
    auto& shard = ShardFor(key);
    std::shared_lock lock(shard.mutex);
    core::ops::Exists shard_op{.keys = {key}};
    auto result = shard.store.Exec(core::ops::ReadOp{shard_op});
    if (result.has_value()) total += result->AsInteger();
  }
  return core::RespValue::Integer(total);
}

core::Result<core::RespValue> ShardedHotStore::Apply(const core::ops::WriteOp& op)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o);
        } else {
          const auto key = core::ops::PrimaryKey(core::ops::WriteOp{o});
          const auto eviction = ResolveEviction(key);
          auto& shard = ShardFor(key);
          std::unique_lock lock(shard.mutex);
          return shard.store.Apply(op, eviction);
        }
      },
      op);
}

// Engine fan-out always issues Del{single key}. The vector iteration is
// preserved so resolver-materialised single-key Dels and any future single-key
// Del callers share the same path; multi-key Del WAL entries no longer occur.
core::Result<core::RespValue> ShardedHotStore::ApplyDel(const core::ops::Del& op) {
  int64_t total_removed = 0;
  for (auto key : op.keys) {
    auto& shard = ShardFor(key);
    std::unique_lock lock(shard.mutex);
    core::ops::Del shard_op{.keys = {key}};
    auto result = shard.store.Apply(core::ops::WriteOp{shard_op}, core::EvictionTTL{0});
    if (!result.has_value()) return std::unexpected(result.error());
    total_removed += result->AsInteger();
  }
  return core::RespValue::Integer(total_removed);
}

core::Result<void> ShardedHotStore::ApplyBatch(std::span<const core::ops::WriteOp> ops) {
  for (const auto& op : ops) {
    auto result = Apply(op);
    if (!result.has_value()) return std::unexpected(result.error());
  }
  return {};
}

core::Result<core::MemoryStats> ShardedHotStore::Stats() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  core::MemoryStats total{};
  for (const auto& shard : shards_) {
    std::shared_lock lock(shard->mutex);
    auto stats = shard->store.Stats();
    total.used_bytes += stats.used_bytes;
    total.key_count += stats.key_count;
    total.eviction_count += stats.eviction_count;
  }
  return total;
}

core::Result<void> ShardedHotStore::Wipe() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  for (auto& shard : shards_) {
    std::unique_lock lock(shard->mutex);
    shard->store.Wipe();
  }
  return {};
}

void ShardedHotStore::DrainAccessBuffers(core::SteadyTime now) {
  for (auto& shard : shards_) {
    std::vector<std::string> keys;
    {
      std::scoped_lock access_lock(shard->access_mutex);
      keys.swap(shard->access_buffer);
    }
    if (keys.empty()) continue;
    std::unique_lock lock(shard->mutex);
    for (const auto& key : keys) {
      shard->store.RefreshAccess(key, now);
    }
  }
}

size_t ShardedHotStore::EvictExpired(core::SteadyTime now) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  size_t total = 0;
  for (auto& shard : shards_) {
    std::unique_lock lock(shard->mutex);
    total += shard->store.EvictExpired(now);
  }
  return total;
}

}  // namespace abyss::hot
