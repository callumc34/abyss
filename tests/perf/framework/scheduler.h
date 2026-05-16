#pragma once

#include <chrono>
#include <cstdint>

namespace abyss::perf {

// Open-loop send schedule under wrk2 discipline. Given a worker's target
// rate, produces intended send times for each operation. Callers measure
// latency from the intended send time, not the actual send time, and feed
// the expected interval to Histogram::RecordCorrected so a stall produces
// synthetic samples at the correct pseudo-latencies.
//
// target_rate_ops_per_worker == 0 selects closed-loop mode: there is no
// schedule, IsOpenLoop() returns false, and callers should Record latency
// without correction.
class CoScheduler {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  CoScheduler(uint64_t target_rate_ops_per_worker, TimePoint worker_start);

  bool IsOpenLoop() const { return interval_ns_ > 0; }
  int64_t ExpectedIntervalNs() const { return interval_ns_; }
  TimePoint WorkerStart() const { return worker_start_; }

  // Intended send time for the i-th operation (0-indexed).
  TimePoint IntendedSendTime(uint64_t op_index) const;

  // Sleep until the intended send time. No-op if already passed.
  // Returns the actual wakeup TimePoint.
  static TimePoint SleepUntil(TimePoint intended);

 private:
  int64_t interval_ns_ = 0;
  TimePoint worker_start_;
};

}  // namespace abyss::perf
