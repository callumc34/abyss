#include "abyss/consumer/hot_consumer_pool.h"

#include <stdexcept>

namespace abyss::consumer {

HotConsumerPool::HotConsumerPool(core::Queue& queue, core::HotStore& hot_store,
                                 core::ConsumerRpc& rpc, Config config,
                                 const core::EvictionPolicy& eviction_policy) {
  if (config.shard_count == 0) {
    throw std::invalid_argument("HotConsumerPool requires shard_count >= 1");
  }
  consumers_.reserve(config.shard_count);
  for (uint32_t shard = 0; shard < config.shard_count; ++shard) {
    HotConsumer::Config per_shard = config.consumer;
    per_shard.shard = shard;
    consumers_.push_back(
        std::make_unique<HotConsumer>(queue, hot_store, rpc, per_shard, eviction_policy));
  }
}

HotConsumerPool::~HotConsumerPool() { Stop(); }

void HotConsumerPool::Start() {
  for (auto& consumer : consumers_) {
    consumer->Start();
  }
}

void HotConsumerPool::Stop() {
  for (auto& consumer : consumers_) {
    consumer->RequestStop();
  }
  for (auto& consumer : consumers_) {
    consumer->Join();
  }
}

bool HotConsumerPool::IsRunning() const {
  for (const auto& consumer : consumers_) {
    if (consumer->Running()) return true;
  }
  return false;
}

}  // namespace abyss::consumer
