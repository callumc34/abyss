#include "abyss/consumer/cold_consumer_pool.h"

#include <stdexcept>
#include <string>

#include "abyss/core/shard_router.h"

namespace abyss::consumer {

ColdConsumerPool::ColdConsumerPool(core::Queue& queue, core::ColdStore& cold_store, Config config,
                                   const core::EvictionPolicy& eviction_policy,
                                   const core::SteadyClockFn& steady_clock,
                                   const core::WallClockFn& wall_clock) {
  if (config.shard_count == 0) {
    throw std::invalid_argument("ColdConsumerPool requires shard_count >= 1");
  }
  consumers_.reserve(config.shard_count);
  for (uint32_t shard = 0; shard < config.shard_count; ++shard) {
    consumers_.push_back(std::make_unique<ColdConsumer>(queue, cold_store, shard, config.consumer,
                                                        eviction_policy, steady_clock, wall_clock));
  }
}

ColdConsumerPool::~ColdConsumerPool() { Stop(); }

void ColdConsumerPool::Start() {
  for (auto& consumer : consumers_) {
    consumer->Start();
  }
}

void ColdConsumerPool::Stop() {
  for (auto& consumer : consumers_) {
    consumer->Stop();
  }
}

bool ColdConsumerPool::IsRunning() const {
  for (const auto& consumer : consumers_) {
    if (consumer->IsRunning()) return true;
  }
  return false;
}

core::Result<core::RespValue> ColdConsumerPool::Read(std::string_view key) const {
  const auto shard = ShardForKey(key);
  return consumers_[shard]->Buffer().Read(std::string(key));
}

core::ShardId ColdConsumerPool::ShardForKey(std::string_view key) const {
  return core::ComputeShard(key, static_cast<uint32_t>(consumers_.size()));
}

ColdConsumerPool::AggregateMetrics ColdConsumerPool::Snapshot() const {
  AggregateMetrics agg;
  for (const auto& consumer : consumers_) {
    auto m = consumer->Snapshot();
    agg.buffer_entries += m.buffer_entries;
    agg.buffer_bytes += m.buffer_bytes;
    agg.flushes_quiet += m.flushes_quiet;
    agg.flushes_deadline += m.flushes_deadline;
    agg.flushes_aggressive += m.flushes_aggressive;
    agg.ops_flushed += m.ops_flushed;
    agg.entries_dropped_abs_ttl += m.entries_dropped_abs_ttl;
    agg.apply_failures += m.apply_failures;
    agg.retry_attempts += m.retry_attempts;
    agg.parse_failures += m.parse_failures;
    if (m.mode == ColdConsumer::Mode::kAggressive) ++agg.shards_in_aggressive_mode;
  }
  return agg;
}

}  // namespace abyss::consumer
