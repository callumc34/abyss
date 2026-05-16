#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"

namespace abyss::hot {

// Background thread that drives eviction maintenance on the hot store.
//
// TODO: Memory pressure eviction
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
};

}  // namespace abyss::hot
