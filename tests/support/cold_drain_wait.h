#pragma once

#include <chrono>
#include <string>
#include <thread>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::testing {

inline constexpr core::Duration kDrainPollStep{1};

// ok once `consumer` has drained through `through`; kTimeout at the
// deadline. A poll that asks the loop to drain at every step, so it
// links no waiter: none on backpressure's room list, whose one-at-a-
// time wakes a far target here would hold up.
inline core::Result<void> WaitDrained(consumer::ColdConsumer& consumer, core::SequenceId through,
                                      core::Duration timeout) {
  const core::SteadyTime deadline = core::SteadyClock::now() + timeout;
  for (;;) {
    if (consumer.LatestDrainedSeq() >= through) return {};
    if (core::SteadyClock::now() >= deadline) {
      return std::unexpected(core::Error{core::ErrorCode::kTimeout, "drain wait timed out"});
    }
    consumer.RequestDrain();
    std::this_thread::sleep_for(kDrainPollStep);
  }
}

inline core::Result<void> WaitDrained(consumer::ColdConsumerPool& pool, core::ShardId shard,
                                      core::SequenceId through, core::Duration timeout) {
  if (shard >= pool.ShardCount()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument,
                                       "cold shard " + std::to_string(shard) + " out of range"});
  }
  return WaitDrained(pool.ConsumerFor(shard), through, timeout);
}

}  // namespace abyss::testing
