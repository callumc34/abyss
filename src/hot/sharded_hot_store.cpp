#include "abyss/hot/sharded_hot_store.h"

#include <optional>
#include <string>
#include <utility>

#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.hot.store")

namespace abyss::hot {

ShardedHotStore::ShardedHotStore(ShardedHotStoreConfig config) : config_(std::move(config)) {
  const auto stub_budget = static_cast<double>(config_.max_memory_bytes) *
                           config_.stub_memory_fraction / static_cast<double>(kStubBytes);
  SingleShardConfig shard_config{
      .max_memory_bytes = config_.max_memory_bytes / config_.shard_count,
      .stub_max_entries = static_cast<size_t>(stub_budget) / config_.shard_count,
      .backpressure_ratio = config_.backpressure_ratio,
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

core::ShardId ShardedHotStore::ShardIndex(std::string_view key) const {
  return core::ComputeShard(key, config_.shard_count);
}

ShardedHotStore::Shard& ShardedHotStore::ShardFor(std::string_view key) {
  return *shards_[ShardIndex(key)];
}

core::SequenceId ShardedHotStore::Horizon(core::ShardId shard) const {
  return config_.drained ? config_.drained(shard) : kAllDrained;
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
            // De-dup within a drain interval and bound the queue: a hot key is
            // buffered once per tick, and once the cap is hit further refreshes
            // are dropped (a dropped refresh only shortens a key's deadline,
            // which is safe — the key is still durable in queue/cold). Drops
            // are counted so backlog/pressure is observable (XRES-2).
            const size_t high_water = config_.access_buffer_high_water;
            if (high_water != 0 && shard.access_buffer.size() >= high_water) {
              ++shard.access_dropped;
            } else if (shard.access_seen.insert(std::string(key)).second) {
              shard.access_buffer.emplace_back(key);
            }
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

core::Result<core::RespValue> ShardedHotStore::Apply(
    const core::ops::WriteOp& op, core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op, seq](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o, seq);
        } else {
          const auto key = core::ops::PrimaryKey(core::ops::WriteOp{o});
          return ApplyToShard(ShardIndex(key), op, ResolveEviction(key), seq);
        }
      },
      op);
}

core::Result<core::RespValue> ShardedHotStore::ApplyToShard(
    core::ShardId index, const core::ops::WriteOp& op, core::EvictionTTL eviction,
    core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  // Read before the lock: never a cold call under a shard lock.
  const auto horizon = Horizon(index);
  auto& shard = *shards_[index];
  std::unique_lock lock(shard.mutex);
  const size_t loads = shard.store.PendingLoads();
  auto result = shard.store.Apply(op, eviction, seq, horizon);
  const bool load_ended = shard.store.PendingLoads() != loads;
  lock.unlock();
  if (load_ended) shard.load_cv.notify_all();
  return result;
}

core::HotKeyPresence ShardedHotStore::Probe(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  std::shared_lock lock(shard.mutex);
  return shard.store.Probe(key);
}

void ShardedHotStore::SetReplayMode(bool replaying) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  for (auto& shard : shards_) {
    std::unique_lock lock(shard->mutex);
    shard->store.SetReplayMode(replaying);
  }
}

// Engine fan-out always issues Del{single key}. The vector iteration is
// preserved so resolver-materialised single-key Dels and any future single-key
// Del callers share the same path; multi-key Del WAL entries no longer occur.
core::Result<core::RespValue> ShardedHotStore::ApplyDel(const core::ops::Del& op,
                                                        core::SequenceId seq) {
  int64_t total_removed = 0;
  for (auto key : op.keys) {
    core::ops::Del shard_op{.keys = {key}};
    auto result =
        ApplyToShard(ShardIndex(key), core::ops::WriteOp{shard_op}, core::EvictionTTL{0}, seq);
    if (!result.has_value()) return std::unexpected(result.error());
    total_removed += result->AsInteger();
  }
  return core::RespValue::Integer(total_removed);
}

core::Result<void> ShardedHotStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                               core::SequenceId seq) {
  for (const auto& op : ops) {
    auto result = Apply(op, seq);
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
    total.expired_count += stats.expired_count;
    total.max_bytes += stats.max_bytes;
    total.stub_entries += stats.stub_entries;
    total.stub_bytes += stats.stub_bytes;
    total.stub_drops += stats.stub_drops;
    total.load_discards += stats.load_discards;
    total.unevictable_bytes += stats.unevictable_bytes;
    total.backpressured = total.backpressured || stats.backpressured;
  }
  return total;
}

core::Result<void> ShardedHotStore::Wipe(core::ShardId shard,
                                         core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  if (shard >= shards_.size()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument,
                                       "hot shard " + std::to_string(shard) + " out of range"});
  }
  Shard& target = *shards_[shard];
  {
    const std::unique_lock lock(target.mutex);
    target.store.Wipe(seq);
  }
  target.load_cv.notify_all();
  return {};
}

std::optional<LoadToken> ShardedHotStore::BeginLoad(std::string_view key)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::unique_lock lock(shard.mutex);
  return shard.store.BeginLoad(key);
}

bool ShardedHotStore::CompleteLoad(std::string_view key, LoadToken token,
                                   LoadedState state) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const auto index = ShardIndex(key);
  const auto eviction = ResolveEviction(key);
  const auto horizon = Horizon(index);
  auto& shard = *shards_[index];
  bool installed = false;
  {
    const std::unique_lock lock(shard.mutex);
    installed = shard.store.CompleteLoad(key, token, std::move(state), eviction, horizon);
  }
  shard.load_cv.notify_all();
  return installed;
}

void ShardedHotStore::AbortLoad(std::string_view key,
                                LoadToken token) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  {
    const std::unique_lock lock(shard.mutex);
    shard.store.AbortLoad(key, token);
  }
  shard.load_cv.notify_all();
}

bool ShardedHotStore::AwaitLoad(std::string_view key,
                                core::SteadyTime deadline) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  std::shared_lock lock(shard.mutex);
  return shard.load_cv.wait_until(lock, deadline,
                                  [&shard, key] { return !shard.store.LoadPending(key); });
}

std::optional<Stub> ShardedHotStore::FindStub(std::string_view key)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::shared_lock lock(shard.mutex);
  const Stub* stub = shard.store.FindStub(key);
  if (stub == nullptr) return std::nullopt;
  return *stub;
}

bool ShardedHotStore::DropStub(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::unique_lock lock(shard.mutex);
  return shard.store.DropStub(key);
}

bool ShardedHotStore::KnownAbsentAfterFlush(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const auto index = ShardIndex(key);
  const auto horizon = Horizon(index);
  auto& shard = *shards_[index];
  const std::shared_lock lock(shard.mutex);
  return shard.store.KnownAbsentAfterFlush(horizon);
}

void ShardedHotStore::DrainAccessBuffers(core::SteadyTime now) {
  for (auto& shard : shards_) {
    std::vector<std::string> keys;
    {
      std::scoped_lock access_lock(shard->access_mutex);
      keys.swap(shard->access_buffer);
      // Reset the per-tick de-dup set so the next interval starts fresh.
      shard->access_seen.clear();
    }
    if (keys.empty()) continue;
    std::unique_lock lock(shard->mutex);
    for (const auto& key : keys) {
      shard->store.RefreshAccess(key, now);
    }
  }
}

SingleShardStore::EvictExpiredReport ShardedHotStore::EvictExpired(core::SteadyTime now)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  SingleShardStore::EvictExpiredReport total;
  for (core::ShardId index = 0; index < config_.shard_count; ++index) {
    const auto horizon = Horizon(index);
    auto& shard = shards_[index];
    std::unique_lock lock(shard->mutex);
    const auto r = shard->store.EvictExpired(now, horizon);
    total.by_deadline += r.by_deadline;
    total.by_ttl += r.by_ttl;
  }
  return total;
}

size_t ShardedHotStore::EvictToMemoryTarget() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  // A budget of 0 means unlimited — never evict for memory pressure. (Guarded
  // here too because EvictLru(0) would otherwise evict every key.)
  if (config_.max_memory_bytes == 0) return 0;
  size_t evicted = 0;
  // Each shard owns max_memory_bytes / shard_count of the budget (set at
  // construction). Evict LRU down to that per-shard ceiling; data is safe in
  // queue/cold (invariant 2).
  const size_t per_shard = config_.max_memory_bytes / config_.shard_count;
  for (core::ShardId index = 0; index < config_.shard_count; ++index) {
    const auto horizon = Horizon(index);
    auto& shard = shards_[index];
    std::unique_lock lock(shard->mutex);
    evicted += shard->store.EvictLru(per_shard, horizon);
  }
  return evicted;
}

ShardedHotStore::AccessBufferStats ShardedHotStore::AccessBufferSnapshot() const {
  AccessBufferStats stats;
  for (const auto& shard : shards_) {
    std::scoped_lock access_lock(shard->access_mutex);
    stats.depth += shard->access_buffer.size();
    stats.dropped += shard->access_dropped;
  }
  return stats;
}

size_t ShardedHotStore::GcTombstones() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  size_t reclaimed = 0;
  for (core::ShardId shard = 0; shard < config_.shard_count; ++shard) {
    const auto horizon = Horizon(shard);
    auto& s = *shards_[shard];
    std::unique_lock lock(s.mutex);
    reclaimed += s.store.GcTombstones(horizon);
  }
  return reclaimed;
}

}  // namespace abyss::hot
