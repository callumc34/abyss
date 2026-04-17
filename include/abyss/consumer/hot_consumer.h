#pragma once

#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class HotConsumer {
 public:
  HotConsumer(core::Queue& queue, core::HotStore& store, core::ShardId shard,
              core::EvictionTTL default_eviction);

  void Start();
  void Stop();

 private:
  [[maybe_unused]] core::Queue& queue_;
  [[maybe_unused]] core::HotStore& store_;
  [[maybe_unused]] core::ShardId shard_;
  [[maybe_unused]] core::EvictionTTL default_eviction_;
};

}  // namespace abyss::consumer
