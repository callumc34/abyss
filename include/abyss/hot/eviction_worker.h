#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"

namespace abyss::hot {

// Background thread that drives eviction maintenance on the hot store: drains
// the deferred access-refresh buffers, evicts by TTL/deadline, enforces the
// memory budget by LRU, reclaims tombstones, and publishes the hot-tier gauges.
class EvictionWorker {
 public:
  struct Config {
    std::chrono::milliseconds tick{1000};
  };

  EvictionWorker(ShardedHotStore& store, Config config,
                 core::SteadyClockFn steady_clock = core::DefaultSteadyClock);
  ~EvictionWorker();

  EvictionWorker(const EvictionWorker&) = delete;
  EvictionWorker& operator=(const EvictionWorker&) = delete;
  EvictionWorker(EvictionWorker&&) = delete;
  EvictionWorker& operator=(EvictionWorker&&) = delete;

  void Start();
  void Stop();
  bool Running() const { return running_.load(std::memory_order_acquire); }

  // Run one tick synchronously. Exposed for tests and one-shot tooling.
  void TickOnce();

 private:
  void Run();

  ShardedHotStore& store_;
  Config config_;
  core::SteadyClockFn steady_clock_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::thread thread_;
  std::mutex wake_mutex_;
  std::condition_variable wake_cv_;

  metrics::CounterHandle evicted_total_;
  metrics::CounterHandle ttl_expired_total_;
  metrics::CounterHandle tombstones_reclaimed_total_;
  metrics::CounterHandle memory_evicted_total_;
  metrics::CounterHandle access_buffer_dropped_total_;
  metrics::CounterHandle stub_drops_total_;
  metrics::CounterHandle load_discards_total_;
  metrics::GaugeHandle hot_memory_bytes_;
  metrics::GaugeHandle hot_keys_;
  metrics::GaugeHandle hot_max_memory_bytes_;
  metrics::GaugeHandle hot_access_buffer_depth_;
  metrics::GaugeHandle hot_stub_entries_;
  metrics::GaugeHandle hot_unevictable_bytes_;
  // Last cumulative access-buffer drop count published, so each tick increments
  // the monotonic counter by only the new drops since the previous tick.
  uint64_t reported_access_dropped_ = 0;
  uint64_t reported_stub_drops_ = 0;
  uint64_t reported_load_discards_ = 0;
};

}  // namespace abyss::hot
