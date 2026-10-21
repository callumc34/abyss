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

void DurabilityWindow::ShardAge::Start(Clock::time_point now) noexcept {
  if (since_ns_.load(std::memory_order_relaxed) == kClear) {
    since_ns_.store(ToNanos(now), std::memory_order_seq_cst);
  }
}

void DurabilityWindow::ShardAge::Set(Clock::time_point since) noexcept {
  since_ns_.store(ToNanos(since), std::memory_order_seq_cst);
}

void DurabilityWindow::ShardAge::Clear() noexcept {
  since_ns_.store(kClear, std::memory_order_seq_cst);
}

DurabilityWindow::Clock::duration DurabilityWindow::ShardAge::Age(
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

bool DurabilityWindow::Admissible(const ShardAge& shard) const noexcept {
  const uint64_t bytes = unflushed_bytes_.load(std::memory_order_seq_cst);
  if (bytes == 0) return true;
  return bytes < max_bytes_ && shard.Age(Clock::now()) < max_age_;
}

core::Result<void> DurabilityWindow::Admit(const ShardAge& shard, Clock::time_point deadline) {
  if (Admissible(shard)) return {};
  // A caller that cannot wait (a best-effort append) is not backpressure.
  if (deadline <= Clock::now()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kResourceExhausted, "WAL durability window full"});
  }
  waits_.Increment();

  std::unique_lock lock(mu_);
  waiters_.fetch_add(1, std::memory_order_seq_cst);
  const bool admitted = cv_.wait_until(
      lock, deadline, [this, &shard] ABYSS_REQUIRES(mu_) { return stopped_ || Admissible(shard); });
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
          " bytes, oldest " + std::to_string(ToMillis(shard.Age(Clock::now()))) + " of " +
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
