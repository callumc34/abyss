#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/resolver.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class ResolverPool {
 public:
  struct Config {
    uint32_t shard_count = 0;
    Resolver::Config consumer;
  };

  ResolverPool(core::Queue& queue, core::ColdStore& cold, CompactionBufferRouter& buffer_router,
               core::ConsumerRpc& rpc, core::ApplyNotifier& apply_notifier, Config config);
  ~ResolverPool();

  ResolverPool(const ResolverPool&) = delete;
  ResolverPool& operator=(const ResolverPool&) = delete;
  ResolverPool(ResolverPool&&) = delete;
  ResolverPool& operator=(ResolverPool&&) = delete;

  void Start();
  void Stop();
  bool IsRunning() const;

  uint32_t ShardCount() const { return static_cast<uint32_t>(consumers_.size()); }
  Resolver& ConsumerFor(core::ShardId shard) { return *consumers_[shard]; }
  const Resolver& ConsumerFor(core::ShardId shard) const { return *consumers_[shard]; }

  struct AggregateMetrics {
    uint64_t conditionals_resolved = 0;
    uint64_t decisions_apply = 0;
    uint64_t decisions_skip = 0;
    uint64_t cache_hits = 0;
    uint64_t buffer_hits = 0;
    uint64_t cold_hits = 0;
    uint64_t cold_timeouts = 0;
    uint64_t cold_errors = 0;
    uint64_t apply_wait_timeouts = 0;
    uint64_t durable_wait_timeouts = 0;
    uint64_t append_failures = 0;
    uint64_t parse_failures = 0;
    uint64_t replayed_resolveds_emitted = 0;
    size_t cache_entries_total = 0;
    size_t cache_bytes_total = 0;
  };

  AggregateMetrics Snapshot() const;

 private:
  std::vector<std::unique_ptr<Resolver>> consumers_;
};

}  // namespace abyss::consumer
