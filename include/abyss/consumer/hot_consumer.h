#pragma once

#include <atomic>
#include <cstddef>
#include <thread>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

struct HotConsumerConfig {
  core::ShardId shard = 0;
  core::EvictionTTL default_eviction{86400};
  size_t read_batch_size = 256;
  // Bounds Stop() latency; loop wakes at this cadence to check stop flag.
  core::Duration read_timeout{100};
};

// One thread per shard: tails the queue, applies writes to the hot store,
// fulfills the associated RPC, acks. Errors (parse, WRONGTYPE, etc.) flow
// through Fulfill rather than wedging the loop.
class HotConsumer {
 public:
  HotConsumer(core::Queue& queue, core::HotStore& store, core::ConsumerRpc& rpc,
              HotConsumerConfig config);
  ~HotConsumer();

  HotConsumer(const HotConsumer&) = delete;
  HotConsumer& operator=(const HotConsumer&) = delete;
  HotConsumer(HotConsumer&&) = delete;
  HotConsumer& operator=(HotConsumer&&) = delete;

  void Start();
  void Stop();

  bool Running() const { return running_.load(std::memory_order_acquire); }
  core::ShardId shard() const { return config_.shard; }

 private:
  void Run();
  void ProcessEntry(core::QueueEntry& entry);
  core::RespValue ApplyWriteEntry(const core::RespCommand& cmd);

  core::Queue& queue_;
  core::HotStore& store_;
  core::ConsumerRpc& rpc_;
  HotConsumerConfig config_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::thread thread_;
};

}  // namespace abyss::consumer
