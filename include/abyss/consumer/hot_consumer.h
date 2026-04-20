#pragma once

#include <atomic>
#include <cstddef>
#include <thread>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"
#include "abyss/metrics/consumer_metrics.h"

namespace abyss::consumer {

// One thread per shard: tails the queue, applies writes to the hot store,
// fulfills the associated RPC, acks. Errors flow through Fulfill rather than
// wedging the loop.
class HotConsumer {
 public:
  struct Config {
    core::ShardId shard = 0;
    size_t read_batch_size = 256;
    // Bounds Stop() latency; loop wakes at this cadence to check stop flag.
    core::Duration read_timeout{100};
  };

  HotConsumer(core::Queue& queue, core::HotStore& store, core::ConsumerRpc& rpc, Config config,
              core::EvictionPolicy eviction_policy);
  ~HotConsumer();

  HotConsumer(const HotConsumer&) = delete;
  HotConsumer& operator=(const HotConsumer&) = delete;
  HotConsumer(HotConsumer&&) = delete;
  HotConsumer& operator=(HotConsumer&&) = delete;

  void Start();

  // Signal the worker to exit. Non-blocking; the thread wakes from its next
  // queue Read (bounded by config.read_timeout) and returns.
  void RequestStop();

  // Wait for the worker thread. Must be preceded by RequestStop.
  void Join();

  // RequestStop + Join. Pools owning many consumers should call the split
  // pair to avoid an O(N * read_timeout) serial teardown.
  void Stop();

  bool Running() const { return running_.load(std::memory_order_acquire); }
  core::ShardId shard() const { return config_.shard; }

  metrics::ConsumerSnapshot Snapshot() const { return metrics::SnapshotOf(counters_); }

 private:
  void Run();
  void ProcessEntry(core::QueueEntry& entry);
  core::RespValue ApplyWriteEntry(const core::RespCommand& cmd);

  core::Queue& queue_;
  core::HotStore& store_;
  core::ConsumerRpc& rpc_;
  Config config_;
  core::EvictionPolicy eviction_policy_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::thread thread_;

  metrics::ConsumerCounters counters_;
};

}  // namespace abyss::consumer
