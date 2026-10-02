#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace abyss::perf {

// Open-loop send schedule under wrk2 discipline: one intended send time
// per operation at the worker's target rate. Callers issue every
// operation, late if necessary, and measure from its intended send
// time, so a stall is charged once to each operation it delays.
//
// target_rate_ops_per_worker == 0 selects closed-loop mode: there is no
// schedule, IsOpenLoop() returns false, and callers measure from the
// send.
class CoScheduler {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  // Longest final spin of a wait. macOS stretches a sleep by up to a
  // quarter of its length (timer coalescing); Linux sleeps to within the
  // 1ns timer slack the driver sets.
#ifdef __APPLE__
  static constexpr std::chrono::microseconds kMaxSpinWindow{200};
  // A macOS sleep this short wakes late by about its own length.
  static constexpr std::chrono::microseconds kMinSleepStep{50};
#else
  static constexpr std::chrono::microseconds kMaxSpinWindow{20};
#endif

  // `ops_per_slot` requests share each send slot (burst arrivals);
  // `threads` driver threads each run such a schedule, thread `index`
  // offset by index/threads of a slot so their sends interleave rather
  // than coincide.
  CoScheduler(uint64_t target_rate_ops_per_worker, TimePoint worker_start,
              uint64_t ops_per_slot = 1, size_t threads = 1, size_t index = 0);

  bool IsOpenLoop() const { return interval_ns_ > 0; }

  // Intended send time for the i-th operation (0-indexed).
  TimePoint IntendedSendTime(uint64_t op_index) const;

  // The final stretch of each wait that is spun rather than slept.
  std::chrono::nanoseconds SpinWindow() const { return spin_window_; }

  // At most kMaxSpinWindow, 1/20 of a slot, and half a core of spinning
  // summed over all driver threads.
  static std::chrono::nanoseconds SpinWindowFor(std::chrono::nanoseconds slot_interval,
                                                size_t threads);

  // End of the next sleep towards `intended`, or nullopt inside the spin
  // window. On macOS each step covers half the time left before it.
  std::optional<TimePoint> SleepStepEnd(TimePoint now, TimePoint intended) const;

  // Sleeps in SleepStepEnd steps, then spins to `intended`. Never returns
  // before it; returns the wakeup time.
  TimePoint SleepUntil(TimePoint intended) const;

  static void CpuRelax() noexcept;

 private:
  int64_t interval_ns_ = 0;
  std::chrono::nanoseconds spin_window_{0};
  TimePoint worker_start_;
};

}  // namespace abyss::perf
