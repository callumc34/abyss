#pragma once

#include <array>
#include <chrono>
#include <span>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"
#include "abyss/queue/reservation.h"
#include "blocking_wait.h"

// Appends for tests, through Reserve and Complete as the sequencer
// appends, and durable waits through WhenDurable as its acks wait.
namespace abyss::testing {

inline constexpr core::Duration kAdmitWithin = std::chrono::seconds{5};
inline constexpr core::Duration kDurableWithin = std::chrono::seconds{30};

// Until `wait` on `shard` is ready: ok, kTimeout at `deadline`, or the
// error that ends every wait.
inline core::Result<void> WaitReady(core::Queue& queue, core::QueueWait wait, core::ShardId shard,
                                    core::SteadyTime deadline) {
  return AwaitWaiter(
      [&](core::Waiter& waiter, bool first) { return queue.WhenReady(wait, shard, waiter, first); },
      [&](core::Waiter& waiter) { return queue.CancelWait(wait, shard, waiter); },
      [&](core::Waiter& waiter) { queue.FinishWait(wait, shard, waiter); }, deadline);
}

inline core::SteadyTime AdmitBy() { return core::SteadyClock::now() + kAdmitWithin; }

// Appends `entries` to `shard` as one batch, moving from them, and
// returns its seqs. A full window waits for admission, and a missing
// spare segment for one, until `admit_by`; a deadline already past
// waits for neither. At the deadline: AdmissionTimedOut for the
// window, and Reserve's own refusal otherwise, which is kUnavailable
// for a missing spare and for a queue shutting down.
inline core::Result<queue::ReservedRange> AppendOwned(core::Queue& queue, core::ShardId shard,
                                                      std::span<core::QueueEntry> entries,
                                                      core::SteadyTime admit_by = AdmitBy()) {
  const std::array parts{queue::ShardEntries{.shard = shard, .entries = entries}};
  for (;;) {
    // A refused Reserve takes none of the entries.
    auto reserved = queue.Reserve(parts);
    if (reserved.has_value()) return queue.Complete(*std::move(reserved)).front();
    core::Error refusal = reserved.error();
    const bool window = refusal.code() == core::ErrorCode::kResourceExhausted;
    if ((!window && refusal.code() != core::ErrorCode::kUnavailable) ||
        core::SteadyClock::now() >= admit_by) {
      return std::unexpected(std::move(refusal));
    }
    if (window) {
      auto admitted = WaitReady(queue, core::QueueWait::kAdmission, shard, admit_by);
      if (admitted.has_value()) continue;
      if (admitted.error().code() == core::ErrorCode::kTimeout) {
        return std::unexpected(queue.AdmissionTimedOut(shard));
      }
      return std::unexpected(admitted.error());
    }
    if (!WaitReady(queue, core::QueueWait::kSpare, shard, admit_by).has_value()) {
      return std::unexpected(std::move(refusal));
    }
  }
}

inline core::Result<queue::ReservedRange> AppendBatch(core::Queue& queue, core::ShardId shard,
                                                      std::span<const core::QueueEntry> entries,
                                                      core::SteadyTime admit_by = AdmitBy()) {
  std::vector<core::QueueEntry> owned(entries.begin(), entries.end());
  return AppendOwned(queue, shard, owned, admit_by);
}

inline core::Result<core::SequenceId> Append(core::Queue& queue, core::ShardId shard,
                                             core::QueueEntry entry,
                                             core::SteadyTime admit_by = AdmitBy()) {
  auto appended = AppendOwned(queue, shard, std::span(&entry, 1), admit_by);
  if (!appended.has_value()) return std::unexpected(appended.error());
  return appended->first;
}

// ok once `seq` on `shard` is durable at `durability`. kTimeout leaves
// no waiter registered; once the queue stops that class, its error,
// kUnavailable.
inline core::Result<void> WaitDurable(core::Queue& queue, core::ShardId shard, core::SequenceId seq,
                                      core::Durability durability,
                                      core::Duration timeout = kDurableWithin) {
  return AwaitNode(
      seq, timeout,
      [&](core::SeqWaiterNode& node) { return queue.WhenDurable(shard, durability, node); },
      [&](core::SeqWaiterNode& node) { return queue.CancelDurable(shard, durability, node); });
}

// As WaitDurable, at the class the queue acknowledges at.
inline core::Result<void> WaitAcked(core::Queue& queue, core::ShardId shard, core::SequenceId seq,
                                    core::Duration timeout = kDurableWithin) {
  return WaitDurable(queue, shard, seq, queue.AckDurability(), timeout);
}

}  // namespace abyss::testing
