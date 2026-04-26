#include "abyss/consumer/hot_consumer_pool.h"

#include <stdexcept>

#include "abyss/log/log.h"

namespace abyss::consumer {

namespace {
const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.hot.consumer");
  return l;
}
}  // namespace

HotConsumerPool::HotConsumerPool(core::Queue& queue, core::HotStore& hot_store,
                                 core::ConsumerRpc& rpc, core::ApplyNotifier& apply_notifier,
                                 Config config, const core::EvictionPolicy& eviction_policy) {
  if (config.shard_count == 0) {
    throw std::invalid_argument("HotConsumerPool requires shard_count >= 1");
  }
  consumers_.reserve(config.shard_count);
  for (uint32_t shard = 0; shard < config.shard_count; ++shard) {
    HotConsumer::Config per_shard = config.consumer;
    per_shard.shard = shard;
    consumers_.push_back(std::make_unique<HotConsumer>(queue, hot_store, rpc, apply_notifier,
                                                       per_shard, eviction_policy));
  }
}

HotConsumerPool::~HotConsumerPool() { Stop(); }

void HotConsumerPool::Start() {
  for (auto& consumer : consumers_) {
    consumer->Start();
  }
  ABYSS_LOG_INFO(Log(), "hot consumers started",
                 {"count", static_cast<int64_t>(consumers_.size())});
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
