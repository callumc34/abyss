#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "abyss/consumer/hot_consumer.h"
#include "abyss/consumer/hot_consumer_progress.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class HotConsumerPool : public HotConsumerProgress {
 public:
  struct Config {
    uint32_t shard_count = 0;
    HotConsumer::Config consumer;
  };

  // `eviction_policy` is borrowed by every per-shard consumer; must outlive
  // the pool. Owned by the server.
  HotConsumerPool(core::Queue& queue, core::HotStore& hot_store, core::ConsumerRpc& rpc,
                  core::ApplyNotifier& apply_notifier, Config config,
                  const core::EvictionPolicy& eviction_policy);
  ~HotConsumerPool() override;
  HotConsumerPool(const HotConsumerPool&) = delete;
  HotConsumerPool& operator=(const HotConsumerPool&) = delete;
  HotConsumerPool(HotConsumerPool&&) = delete;
  HotConsumerPool& operator=(HotConsumerPool&&) = delete;

  void Start();
  void Stop();
  bool IsRunning() const;

  uint32_t ShardCount() const { return static_cast<uint32_t>(consumers_.size()); }

  HotConsumer& ConsumerFor(core::ShardId shard) { return *consumers_[shard]; }
  const HotConsumer& ConsumerFor(core::ShardId shard) const { return *consumers_[shard]; }

  core::SequenceId HighestSettledSeq(core::ShardId shard) const override {
    if (shard >= consumers_.size()) return 0;
    return consumers_[shard]->HighestSettledSeq();
  }

 private:
  std::vector<std::unique_ptr<HotConsumer>> consumers_;
};

}  // namespace abyss::consumer
