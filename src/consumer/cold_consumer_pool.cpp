#include "abyss/consumer/cold_consumer_pool.h"

#include <stdexcept>
#include <string>

#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.cold.consumer")

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
  ABYSS_LOG_INFO("cold consumers started", {"count", static_cast<int64_t>(consumers_.size())});
}

void ColdConsumerPool::Stop() {
  for (auto& consumer : consumers_) {
    consumer->RequestStop();
  }
  for (auto& consumer : consumers_) {
    consumer->Join();
  }
}

bool ColdConsumerPool::IsRunning() const {
  for (const auto& consumer : consumers_) {
    if (consumer->IsRunning()) return true;
  }
  return false;
}

core::Result<core::RespValue> ColdConsumerPool::Exec(const core::ops::ReadOp& op,
                                                     std::optional<core::Duration> /*deadline*/) {
  if (const auto* exists = std::get_if<core::ops::Exists>(&op)) {
    int64_t total = 0;
    for (auto key : exists->keys) {
      auto sub = consumers_[ShardForKey(key)]->Buffer().Exec(
          core::ops::ReadOp{core::ops::Exists{.keys = {key}}});
      if (!sub.has_value()) {
        if (sub.error().code() == core::ErrorCode::kNotFound) continue;
        return std::unexpected(sub.error());
      }
      total += sub->AsInteger();
    }
    return core::RespValue::Integer(total);
  }
  const auto key = core::ops::PrimaryKey(op);
  if (key.empty()) {
    return std::unexpected(core::Error(core::ErrorCode::kInternal, "empty primary key"));
  }
  return consumers_[ShardForKey(key)]->Buffer().Exec(op);
}

core::Result<core::RespValue> ColdConsumerPool::Read(std::string_view key) const {
  const auto shard = ShardForKey(key);
  return consumers_[shard]->Buffer().Read(std::string(key));
}

BufferKeyPresence ColdConsumerPool::Probe(std::string_view key) const {
  const auto shard = ShardForKey(key);
  return consumers_[shard]->Buffer().Probe(key);
}

HashOverlay ColdConsumerPool::HashOverlayFor(std::string_view key) const {
  const auto shard = ShardForKey(key);
  return consumers_[shard]->Buffer().HashOverlayFor(key);
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
