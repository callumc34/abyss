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
  core::Queue& queue_;
  core::HotStore& store_;
  core::ShardId shard_;
  core::EvictionTTL default_eviction_;
};

}  // namespace abyss::consumer
