#include "abyss/queue/durability_window.h"

#include <string>

#include "abyss/metrics/names.h"

namespace abyss::queue {

namespace {

int64_t ToNanos(DurabilityWindow::Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

int64_t ToMillis(DurabilityWindow::Clock::duration d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

}  // namespace

void DurabilityWindow::LogAge::Start(Clock::time_point now) noexcept {
  if (since_ns_.load(std::memory_order_seq_cst) != kClear) return;
  int64_t clear = kClear;
  since_ns_.compare_exchange_strong(clear, ToNanos(now), std::memory_order_seq_cst);
}

// An appender reserves, then starts the clock; this clears, then reads
// the tail. In the total order one of them sees the other, so a frame
// reserved past `flushed_to` never leaves the age clear.
void DurabilityWindow::LogAge::Flushed(Clock::time_point snapshot_at, uint64_t flushed_to,
                                       const std::function<uint64_t()>& reserved_tail) {
  const int64_t since = ToNanos(snapshot_at);
  if (reserved_tail() > flushed_to) {
    since_ns_.store(since, std::memory_order_seq_cst);
    return;
  }
  since_ns_.store(kClear, std::memory_order_seq_cst);
  if (reserved_tail() > flushed_to) since_ns_.store(since, std::memory_order_seq_cst);
}

DurabilityWindow::Clock::duration DurabilityWindow::LogAge::Age(
    Clock::time_point now) const noexcept {
  const int64_t since = since_ns_.load(std::memory_order_seq_cst);
  if (since == kClear) return Clock::duration::zero();
  const auto age = std::chrono::nanoseconds{ToNanos(now) - since};
  return age > Clock::duration::zero() ? std::chrono::duration_cast<Clock::duration>(age)
                                       : Clock::duration::zero();
}

DurabilityWindow::DurabilityWindow(uint64_t max_bytes, Clock::duration max_age)
    : max_bytes_(max_bytes),
      max_age_(max_age),
      waits_(metrics::Registry::Instance().Counter(metrics::names::kWalBackpressureWaitsTotal)),
      rejections_(
          metrics::Registry::Instance().Counter(metrics::names::kWalBackpressureRejectionsTotal)) {}

bool DurabilityWindow::Admissible(const LogAge& age) const noexcept {
  const uint64_t bytes = unflushed_bytes_.load(std::memory_order_seq_cst);
  if (bytes == 0) return true;
  return bytes < max_bytes_ && age.Age(Clock::now()) < max_age_;
}

core::Result<void> DurabilityWindow::Admit(const LogAge& age, Clock::time_point deadline) {
  if (Admissible(age)) return {};
  // A caller that cannot wait (a best-effort append) is not backpressure.
  if (deadline <= Clock::now()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kResourceExhausted, "WAL durability window full"});
  }
  waits_.Increment();

  std::unique_lock lock(mu_);
  waiters_.fetch_add(1, std::memory_order_seq_cst);
  const bool admitted = cv_.wait_until(
      lock, deadline, [this, &age] ABYSS_REQUIRES(mu_) { return stopped_ || Admissible(age); });
  waiters_.fetch_sub(1, std::memory_order_relaxed);
  if (stopped_) {
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
  }
  if (admitted) return {};

  rejections_.Increment();
  return std::unexpected(core::Error{
      core::ErrorCode::kResourceExhausted,
      "WAL durability window full: the device is not keeping up with writes (unflushed " +
          std::to_string(UnflushedBytes()) + " of " + std::to_string(max_bytes_) +
          " bytes, oldest " + std::to_string(ToMillis(age.Age(Clock::now()))) + " of " +
          std::to_string(ToMillis(max_age_)) + " ms)"});
}

void DurabilityWindow::Add(uint64_t bytes) noexcept {
  unflushed_bytes_.fetch_add(bytes, std::memory_order_relaxed);
}

void DurabilityWindow::Release(uint64_t bytes) noexcept {
  unflushed_bytes_.fetch_sub(bytes, std::memory_order_seq_cst);
  // Pairs with the waiter's increment before its predicate check.
  if (waiters_.load(std::memory_order_seq_cst) == 0) return;
  const std::scoped_lock lock(mu_);
  cv_.notify_all();
}

void DurabilityWindow::Shutdown() {
  {
    const std::scoped_lock lock(mu_);
    stopped_ = true;
  }
  cv_.notify_all();
}

}  // namespace abyss::queue
