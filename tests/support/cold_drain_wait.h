#pragma once

#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"
#include "blocking_wait.h"

namespace abyss::testing {

// ok once `consumer` has drained through `through`, by its
// WhenDrainedPast; kTimeout with no waiter left linked; kUnavailable
// once it stopped.
inline core::Result<void> WaitDrained(consumer::ColdConsumer& consumer, core::SequenceId through,
                                      core::Duration timeout) {
  if (through < core::kFirstSeq) return {};
  return AwaitWaiter(
      [&](core::Waiter& waiter, bool) { return consumer.WhenDrainedPast(through - 1, waiter); },
      [&](core::Waiter& waiter) { return consumer.CancelDrainWait(waiter); },
      [&](core::Waiter& waiter) { consumer.FinishDrainWait(waiter); },
      core::SteadyClock::now() + timeout);
}

// As above, through the pool's routing to `shard`'s consumer.
inline core::Result<void> WaitDrained(consumer::ColdConsumerPool& pool, core::ShardId shard,
                                      core::SequenceId through, core::Duration timeout) {
  if (through < core::kFirstSeq) return {};
  return AwaitWaiter(
      [&](core::Waiter& waiter, bool) { return pool.WhenDrainedPast(shard, through - 1, waiter); },
      [&](core::Waiter& waiter) { return pool.CancelDrainWait(shard, waiter); },
      [&](core::Waiter& waiter) { pool.FinishDrainWait(shard, waiter); },
      core::SteadyClock::now() + timeout);
}

}  // namespace abyss::testing
