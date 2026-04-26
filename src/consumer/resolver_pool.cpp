#include "abyss/consumer/resolver_pool.h"

#include <stdexcept>

#include "abyss/log/log.h"

namespace abyss::consumer {

namespace {
const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.resolver");
  return l;
}
}  // namespace

ResolverPool::ResolverPool(core::Queue& queue, core::ColdStore& cold,
                           CompactionBufferRouter& buffer_router, core::ConsumerRpc& rpc,
                           core::ApplyNotifier& apply_notifier, Config config) {
  if (config.shard_count == 0) {
    throw std::invalid_argument("ResolverPool requires shard_count >= 1");
  }
  consumers_.reserve(config.shard_count);
  for (uint32_t shard = 0; shard < config.shard_count; ++shard) {
    Resolver::Config per = config.consumer;
    per.shard = shard;
    consumers_.push_back(
        std::make_unique<Resolver>(queue, cold, buffer_router, rpc, apply_notifier, per));
  }
}

ResolverPool::~ResolverPool() { Stop(); }

core::Result<void> ResolverPool::ReplayForRecovery() {
  for (auto& consumer : consumers_) {
    auto r = consumer->ReplayForRecovery();
    if (!r.has_value()) return r;
  }
  return {};
}

void ResolverPool::Start() {
  for (auto& consumer : consumers_) consumer->Start();
  ABYSS_LOG_INFO(Log(), "resolvers started", {"count", static_cast<int64_t>(consumers_.size())});
}

void ResolverPool::Stop() {
  for (auto& consumer : consumers_) consumer->RequestStop();
  for (auto& consumer : consumers_) consumer->Join();
}

bool ResolverPool::IsRunning() const {
  for (const auto& consumer : consumers_) {
    if (consumer->Running()) return true;
  }
  return false;
}

ResolverPool::AggregateMetrics ResolverPool::Snapshot() const {
  AggregateMetrics agg;
  for (const auto& consumer : consumers_) {
    auto s = consumer->GetSnapshot();
    agg.conditionals_resolved += s.conditionals_resolved;
    agg.decisions_apply += s.decisions_apply;
    agg.decisions_skip += s.decisions_skip;
    agg.cache_hits += s.cache_hits;
    agg.buffer_hits += s.buffer_hits;
    agg.cold_hits += s.cold_hits;
    agg.cold_timeouts += s.cold_timeouts;
    agg.cold_errors += s.cold_errors;
    agg.apply_wait_timeouts += s.apply_wait_timeouts;
    agg.append_failures += s.append_failures;
    agg.parse_failures += s.parse_failures;
    agg.replayed_resolveds_emitted += s.replayed_resolveds_emitted;
    agg.cache_entries_total += s.cache_entries;
    agg.cache_bytes_total += s.cache_bytes;
  }
  return agg;
}

}  // namespace abyss::consumer
