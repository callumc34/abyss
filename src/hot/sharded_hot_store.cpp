#include "abyss/hot/sharded_hot_store.h"

#include <map>
#include <utility>

#include "abyss/core/ops.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/hot/shard_router.h"

namespace abyss::hot {

ShardedHotStore::ShardedHotStore(ShardedHotStoreConfig config) : config_(config) {
  SingleShardConfig shard_config{
      .max_memory_bytes = config_.max_memory_bytes / config_.shard_count,
  };
  shards_.reserve(config_.shard_count);
  for (uint32_t i = 0; i < config_.shard_count; ++i) {
    shards_.push_back(std::make_unique<Shard>(shard_config));
  }
}

ShardedHotStore::~ShardedHotStore() = default;

ShardedHotStore::Shard& ShardedHotStore::ShardFor(std::string_view key) {
  return *shards_[ComputeShard(key, config_.shard_count)];
}

core::Result<core::RespValue> ShardedHotStore::Exec(const core::ops::ReadOp& op)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::MultiStringGet>) {
          return ExecMultiStringGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return ExecExists(o);
        } else {
          auto key = o.key;
          auto& shard = ShardFor(key);
          std::shared_lock lock(shard.mutex);
          auto result = shard.store.Exec(op);
          if (result.has_value()) {
            std::lock_guard access_lock(shard.access_mutex);
            shard.access_buffer.emplace_back(key);
          }
          return result;
        }
      },
      op);
}

core::Result<core::RespValue> ShardedHotStore::ExecMultiStringGet(
    const core::ops::MultiStringGet& op) {
  std::vector<core::RespValue> results(op.keys.size(), core::RespValue::Null());

  std::map<uint32_t, std::vector<size_t>> shard_indices;
  for (size_t i = 0; i < op.keys.size(); ++i) {
    auto shard_id = ComputeShard(op.keys[i], config_.shard_count);
    shard_indices[shard_id].push_back(i);
  }

  for (const auto& [shard_id, indices] : shard_indices) {
    auto& shard = *shards_[shard_id];
    std::shared_lock lock(shard.mutex);
    for (auto idx : indices) {
      core::ops::StringGet get_op{.key = op.keys[idx]};
      auto result = shard.store.Exec(core::ops::ReadOp{get_op});
      if (result.has_value()) {
        results[idx] = std::move(*result);
        std::lock_guard access_lock(shard.access_mutex);
        shard.access_buffer.emplace_back(op.keys[idx]);
      }
    }
  }

  return core::RespValue::Array(std::move(results));
}

core::Result<core::RespValue> ShardedHotStore::ExecExists(const core::ops::Exists& op) {
  int64_t total = 0;

  std::map<uint32_t, std::vector<std::string_view>> shard_keys;
  for (auto key : op.keys) {
    auto shard_id = ComputeShard(key, config_.shard_count);
    shard_keys[shard_id].push_back(key);
  }

  for (const auto& [shard_id, keys] : shard_keys) {
    auto& shard = *shards_[shard_id];
    std::shared_lock lock(shard.mutex);
    core::ops::Exists shard_op{.keys = keys};
    auto result = shard.store.Exec(core::ops::ReadOp{shard_op});
    if (result.has_value()) {
      total += result->AsInteger();
    }
  }

  return core::RespValue::Integer(total);
}

core::Result<void> ShardedHotStore::Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op, eviction](const auto& o) -> core::Result<void> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o);
        } else if constexpr (std::is_same_v<T, core::ops::MultiStringSet>) {
          return ApplyMultiStringSet(o, eviction);
        } else {
          auto key = core::ops::PrimaryKey(core::ops::WriteOp{o});
          auto& shard = ShardFor(key);
          std::unique_lock lock(shard.mutex);
          return shard.store.Apply(op, eviction);
        }
      },
      op);
}

core::Result<void> ShardedHotStore::ApplyDel(const core::ops::Del& op) {
  std::map<uint32_t, std::vector<std::string_view>> shard_keys;
  for (auto key : op.keys) {
    auto shard_id = ComputeShard(key, config_.shard_count);
    shard_keys[shard_id].push_back(key);
  }

  for (const auto& [shard_id, keys] : shard_keys) {
    auto& shard = *shards_[shard_id];
    std::unique_lock lock(shard.mutex);
    core::ops::Del shard_op{.keys = keys};
    auto result = shard.store.Apply(core::ops::WriteOp{shard_op}, core::EvictionTTL{0});
    if (!result.has_value()) return result;
  }
  return {};
}

core::Result<void> ShardedHotStore::ApplyMultiStringSet(const core::ops::MultiStringSet& op,
                                                        core::EvictionTTL eviction) {
  std::map<uint32_t, std::vector<const core::ops::MultiStringSet::Entry*>> shard_entries;
  for (const auto& entry : op.entries) {
    auto shard_id = ComputeShard(entry.key, config_.shard_count);
    shard_entries[shard_id].push_back(&entry);
  }

  for (const auto& [shard_id, entries] : shard_entries) {
    auto& shard = *shards_[shard_id];
    std::unique_lock lock(shard.mutex);
    for (const auto* entry : entries) {
      core::ops::StringSet set_op{.key = entry->key, .value = entry->value};
      auto result = shard.store.Apply(core::ops::WriteOp{set_op}, eviction);
      if (!result.has_value()) return result;
    }
  }
  return {};
}

core::Result<void> ShardedHotStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                               core::EvictionTTL eviction) {
  for (const auto& op : ops) {
    auto result = Apply(op, eviction);
    if (!result.has_value()) return result;
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

core::Result<void> ShardedHotStore::Flush() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  for (auto& shard : shards_) {
    std::unique_lock lock(shard->mutex);
    shard->store.Flush();
  }
  return {};
}

void ShardedHotStore::DrainAccessBuffers(core::SteadyTime now, core::EvictionTTL eviction) {
  for (auto& shard : shards_) {
    std::vector<std::string> keys;
    {
      std::lock_guard access_lock(shard->access_mutex);
      keys.swap(shard->access_buffer);
    }
    if (keys.empty()) continue;
    std::unique_lock lock(shard->mutex);
    for (const auto& key : keys) {
      shard->store.RefreshAccess(key, now, eviction);
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
