#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/wait_list.h"
#include "abyss/metrics/metrics.h"

namespace abyss::queue {

// Bounds what is filled but not yet power-durable: unflushed bytes
// across every log, and each log's oldest unflushed age. Appenders are
// admitted before taking their shard lock; flushers make room.
class DurabilityWindow {
 public:
  using Clock = std::chrono::steady_clock;

  // When a log's oldest unflushed frame may have been reserved: an
  // upper bound on its age, needing no per-frame timestamps. Lock-free:
  // the appender's (tail, age) and the flusher's (age, tail) form a
  // Dekker pair, so every operation is seq_cst.
  class LogAge {
   public:
    // Appender, after Reserve: starts the clock iff it is clear.
    void Start(Clock::time_point now) noexcept;
    // Flusher, after a flush whose filled-prefix snapshot `flushed_to`
    // was taken after `snapshot_at`. `reserved_tail` reads the log's.
    void Flushed(Clock::time_point snapshot_at, uint64_t flushed_to,
                 const std::function<uint64_t()>& reserved_tail);
    // Zero when nothing is unflushed.
    Clock::duration Age(Clock::time_point now) const noexcept;

   private:
    static constexpr int64_t kClear = std::numeric_limits<int64_t>::min();
    std::atomic<int64_t> since_ns_{kClear};
  };

  DurabilityWindow(uint64_t max_bytes, Clock::duration max_age, std::size_t log_count);

  DurabilityWindow(const DurabilityWindow&) = delete;
  DurabilityWindow& operator=(const DurabilityWindow&) = delete;
  DurabilityWindow(DurabilityWindow&&) = delete;
  DurabilityWindow& operator=(DurabilityWindow&&) = delete;
  ~DurabilityWindow() = default;

  // Log `log`'s: its appenders start it and its flusher moves it.
  LogAge& age(std::size_t log) noexcept { return logs_[log].age; }
  const LogAge& age(std::size_t log) const noexcept { return logs_[log].age; }

  // Waits for room on log `log` until `deadline`. An empty window
  // always admits. kResourceExhausted at the deadline; kUnavailable on
  // Shutdown. A deadline already past checks once, uncounted.
  core::Result<void> Admit(std::size_t log, Clock::time_point deadline);
  // Admit's lock-free check, which its waiters make again.
  bool Admissible(std::size_t log) const noexcept;
  // Log `log`'s Admit waiters, for one that registers itself. Its
  // condition is stopped() || Admissible(log).
  core::WaitList& admission(std::size_t log) noexcept { return logs_[log].admission; }
  // Admit's wait for a waiter that registers itself: counted when
  // `first`; outcomes as WaitList::Register's, kUnavailable once
  // stopped.
  std::optional<core::Result<void>> WhenAdmissible(std::size_t log, core::Waiter& waiter,
                                                   bool first);
  // For a waiter WhenAdmissible linked, as WaitList's.
  bool CancelAdmission(std::size_t log, core::Waiter& waiter) noexcept {
    return admission(log).Cancel(waiter);
  }
  void FinishAdmission(std::size_t log, core::Waiter& waiter) noexcept {
    admission(log).Finish(waiter);
  }
  // Admit's error at its deadline, counted as a rejection.
  core::Error Rejected(std::size_t log);
  bool stopped() const noexcept { return stopped_.load(std::memory_order_seq_cst); }

  // Entry frame bytes: after Reserve, before Commit.
  void Add(uint64_t bytes) noexcept;
  // After a flush, once the log's age is updated. The bytes are every
  // log's, so it wakes each log whose waiters would now pass.
  void Release(uint64_t bytes) noexcept;
  // Fails every admission wait, now and later, with kUnavailable.
  void Shutdown();

  uint64_t UnflushedBytes() const noexcept {
    return unflushed_bytes_.load(std::memory_order_acquire);
  }

  // Runs in Admit, and a first WhenAdmissible, between the check and
  // the wait.
  void SetAdmitHookForTesting(std::function<void()> hook) { admit_hook_ = std::move(hook); }

 private:
  bool Admissible(std::size_t log, Clock::time_point now) const noexcept;

  const uint64_t max_bytes_;
  const Clock::duration max_age_;
  std::atomic<uint64_t> unflushed_bytes_{0};
  std::atomic<bool> stopped_{false};
  metrics::CounterHandle waits_;
  metrics::CounterHandle rejections_;
  struct Log {
    LogAge age;
    // Every waiter here tests the same bytes and this log's age, never
    // its own size, so none passes a head that fails. A wait ends by
    // admit_by, as kResourceExhausted and a rejection counted.
    core::WaitList admission;
  };
  // One per log: a log's waiters wait on its own age, so one held by
  // another log's age never stands in front of them.
  std::vector<Log> logs_;
  std::function<void()> admit_hook_;
};

}  // namespace abyss::queue
