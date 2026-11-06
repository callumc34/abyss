#pragma once

#include <cstddef>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"
#include "abyss/queue/durability_window.h"
#include "abyss/queue/log.h"
#include "blocking_wait.h"

// Blocking waits for tests on the WAL's capacity lists, registered as
// the sequencer's waiters register.
namespace abyss::testing {

// True once `log` has a spare segment; false at `deadline`, or once it
// is shutting down.
inline bool WaitForSpare(queue::Log& log, core::SteadyTime deadline) {
  return AwaitWaiter([&](core::Waiter& waiter, bool first) { return log.WhenSpare(waiter, first); },
                     [&](core::Waiter& waiter) { return log.CancelSpare(waiter); },
                     [&](core::Waiter& waiter) { log.FinishSpare(waiter); }, deadline)
      .has_value();
}

// Room on `log` by `deadline`: ok; kResourceExhausted at the deadline,
// counted as a rejection; kUnavailable once the window shuts down. A
// deadline already past checks once, uncounted.
inline core::Result<void> AwaitAdmission(queue::DurabilityWindow& window, std::size_t log,
                                         core::SteadyTime deadline) {
  if (window.Admissible(log) || deadline <= core::SteadyClock::now()) return window.Admit(log);
  core::Result<void> admitted = AwaitWaiter(
      [&](core::Waiter& waiter, bool first) { return window.WhenAdmissible(log, waiter, first); },
      [&](core::Waiter& waiter) { return window.CancelAdmission(log, waiter); },
      [&](core::Waiter& waiter) { window.FinishAdmission(log, waiter); }, deadline);
  if (TimedOut(admitted)) return std::unexpected(window.Rejected(log));
  return admitted;
}

}  // namespace abyss::testing
