#pragma once

#include <atomic>
#include <chrono>
#include <string>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/core/queue.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::testing {

// Replays `consumer`'s shard from its cursor to `end`, exclusive, as
// recovery's Scan feeds it, then finishes the replay.
inline core::Result<void> ReplayCold(core::Queue& queue, consumer::ColdConsumer& consumer,
                                     core::SequenceId end, const std::atomic<bool>& cancel) {
  auto from = consumer.BeginReplay();
  if (!from.has_value()) return std::unexpected(from.error());
  for (core::SequenceId next = *from; next < end;) {
    auto read = queue.Read(consumer.Shard(), next, end - next, std::chrono::milliseconds(1000),
                           queue.AckDurability());
    if (!read.has_value()) return std::unexpected(read.error());
    if (read->empty()) {
      return std::unexpected(
          core::Error{core::ErrorCode::kTimeout, "nothing durable at seq " + std::to_string(next)});
    }
    if (auto applied = consumer.ApplyReplayBatch(*read, cancel); !applied.has_value()) {
      return applied;
    }
    next = read->back().seq + 1;
  }
  return consumer.FinishReplay(end, cancel);
}

}  // namespace abyss::testing
