#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class ColdConsumerPool : public CompactionBufferRouter {
 public:
  struct Config {
    uint32_t shard_count = 0;
    ColdConsumer::Config consumer;
  };

  struct AggregateMetrics {
    size_t buffer_entries = 0;
    size_t buffer_bytes = 0;
    uint64_t flushes_quiet = 0;
    uint64_t flushes_deadline = 0;
    uint64_t flushes_aggressive = 0;
    uint64_t ops_flushed = 0;
    uint64_t entries_dropped_abs_ttl = 0;
    uint64_t apply_failures = 0;
    uint64_t retry_attempts = 0;
    uint64_t parse_failures = 0;
    uint32_t shards_in_aggressive_mode = 0;
  };

  ColdConsumerPool(core::Queue& queue, core::ColdStore& cold_store, Config config,
                   const core::EvictionPolicy& eviction_policy,
                   const core::SteadyClockFn& steady_clock = core::DefaultSteadyClock,
                   const core::WallClockFn& wall_clock = core::DefaultWallClock);
  ~ColdConsumerPool() override;
  ColdConsumerPool(const ColdConsumerPool&) = delete;
  ColdConsumerPool& operator=(const ColdConsumerPool&) = delete;
  ColdConsumerPool(ColdConsumerPool&&) = delete;
  ColdConsumerPool& operator=(ColdConsumerPool&&) = delete;

  core::Result<core::RespValue> Read(std::string_view key) const override;

  void Start();
  void Stop();
  bool IsRunning() const;

  uint32_t ShardCount() const { return static_cast<uint32_t>(consumers_.size()); }

  ColdConsumer& ConsumerFor(core::ShardId shard) { return *consumers_[shard]; }
  const ColdConsumer& ConsumerFor(core::ShardId shard) const { return *consumers_[shard]; }

  AggregateMetrics Snapshot() const;

 private:
  core::ShardId ShardForKey(std::string_view key) const;

  std::vector<std::unique_ptr<ColdConsumer>> consumers_;
};

}  // namespace abyss::consumer
