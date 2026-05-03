#pragma once

#include <functional>

#include "abyss/engine/shard_scheduler.h"

namespace abyss::testing {

// Runs Submit() work synchronously on the calling thread. WaitAll() is a
// no-op. Useful in unit tests where the coordinator's threading is not the
// system under test.
class InlineShardScheduler : public engine::ShardScheduler {
 public:
  void Submit(core::ShardId /*shard*/, std::function<void()> work) override {
    if (work) work();
  }
  void WaitAll() override {}
};

}  // namespace abyss::testing
