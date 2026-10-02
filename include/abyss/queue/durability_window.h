#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
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

  DurabilityWindow(uint64_t max_bytes, Clock::duration max_age);

  DurabilityWindow(const DurabilityWindow&) = delete;
  DurabilityWindow& operator=(const DurabilityWindow&) = delete;
  DurabilityWindow(DurabilityWindow&&) = delete;
  DurabilityWindow& operator=(DurabilityWindow&&) = delete;
  ~DurabilityWindow() = default;

  // Waits for room on the log `age` tracks until `deadline`. An empty
  // window always admits. kResourceExhausted at the deadline;
  // kUnavailable on Shutdown. A deadline already past checks once,
  // uncounted.
  core::Result<void> Admit(const LogAge& age, Clock::time_point deadline);

  // Entry frame bytes: after Reserve, before Commit.
  void Add(uint64_t bytes) noexcept;
  // After a flush, once the log's age is updated. Wakes waiters.
  void Release(uint64_t bytes) noexcept;
  void Shutdown();

  uint64_t UnflushedBytes() const noexcept {
    return unflushed_bytes_.load(std::memory_order_acquire);
  }

 private:
  bool Admissible(const LogAge& age) const noexcept;

  const uint64_t max_bytes_;
  const Clock::duration max_age_;
  std::atomic<uint64_t> unflushed_bytes_{0};
  std::atomic<uint64_t> waiters_{0};
  metrics::CounterHandle waits_;
  metrics::CounterHandle rejections_;

  std::mutex mu_;
  std::condition_variable cv_;
  bool stopped_ ABYSS_GUARDED_BY(mu_) = false;
};

}  // namespace abyss::queue
