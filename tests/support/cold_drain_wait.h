#pragma once

#include <string>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/result.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"
#include "blocking_wait.h"

namespace abyss::testing {

// ok once `consumer` has drained through `through`; kTimeout with no
// node left registered; kUnavailable once it stopped. A seq wait on
// the drained seq's own waiters, never on backpressure's room list,
// whose one-at-a-time wakes a far target here would hold up.
inline core::Result<void> WaitDrained(consumer::ColdConsumer& consumer, core::SequenceId through,
                                      core::Duration timeout) {
  if (through < core::kFirstSeq) return {};
  return AwaitNode(
      through - 1, timeout, [&](core::SeqWaiterNode& node) { return consumer.WhenDrained(node); },
      [&](core::SeqWaiterNode& node) { return consumer.CancelDrained(node); });
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
