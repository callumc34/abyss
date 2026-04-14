#include "abyss/consumer/hot_consumer.h"

namespace abyss::consumer {

HotConsumer::HotConsumer(core::Queue& queue, core::HotStore& store, core::ShardId shard,
                         core::EvictionTTL default_eviction)
    : queue_(queue), store_(store), shard_(shard), default_eviction_(default_eviction) {}

void HotConsumer::Start() {
  // Will spawn dedicated consumer thread
}

void HotConsumer::Stop() {
  // Will signal thread to stop and join
}

}  // namespace abyss::consumer
