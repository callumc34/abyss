#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/metrics/metrics.h"

namespace abyss::queue {

// Bounds what is published but not yet power-durable: unflushed bytes
// across every shard, and each shard's oldest unflushed age. Appenders
// are admitted before taking their shard lock; flushers make room.
class DurabilityWindow {
 public:
  using Clock = std::chrono::steady_clock;

  // When a shard's oldest unflushed entry may have been published: an
  // upper bound on its age, needing no per-entry timestamps.
  class ShardAge {
   public:
    // Appender, under the shard lock: starts the clock if it is clear.
    void Start(Clock::time_point now) noexcept;
    // Flusher, under the shard lock, after a flush.
    void Set(Clock::time_point since) noexcept;
    void Clear() noexcept;
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

  // Waits for room for `shard` until `deadline`. An empty window always
  // admits. kResourceExhausted at the deadline; kUnavailable on Shutdown.
  // A deadline already past checks once, uncounted.
  core::Result<void> Admit(const ShardAge& shard, Clock::time_point deadline);

  void Add(uint64_t bytes) noexcept;
  // After a flush, once the shard's age is updated. Wakes waiters.
  void Release(uint64_t bytes) noexcept;
  void Shutdown();

  uint64_t UnflushedBytes() const noexcept {
    return unflushed_bytes_.load(std::memory_order_acquire);
  }

 private:
  bool Admissible(const ShardAge& shard) const noexcept;

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
