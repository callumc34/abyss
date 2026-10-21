#include "abyss/queue/durability_window.h"

#include <cstddef>
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

DurabilityWindow::DurabilityWindow(uint64_t max_bytes, Clock::duration max_age,
                                   std::size_t log_count)
    : max_bytes_(max_bytes),
      max_age_(max_age),
      waits_(metrics::Registry::Instance().Counter(metrics::names::kWalBackpressureWaitsTotal)),
      rejections_(
          metrics::Registry::Instance().Counter(metrics::names::kWalBackpressureRejectionsTotal)),
      logs_(log_count) {}

bool DurabilityWindow::Admissible(std::size_t log) const noexcept {
  return Admissible(log, Clock::now());
}

bool DurabilityWindow::Admissible(std::size_t log, Clock::time_point now) const noexcept {
  const uint64_t bytes = unflushed_bytes_.load(std::memory_order_seq_cst);
  if (bytes == 0) return true;
  return bytes < max_bytes_ && age(log).Age(now) < max_age_;
}

core::Result<void> DurabilityWindow::Admit(std::size_t log, Clock::time_point deadline) {
  if (Admissible(log)) return {};
  // A caller that cannot wait (a best-effort append) is not backpressure.
  if (deadline <= Clock::now()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kResourceExhausted, "WAL durability window full"});
  }
  waits_.Increment();
  if (admit_hook_) admit_hook_();
  auto admitted =
      admission(log).Await([this, log] { return stopped() || Admissible(log); }, deadline);
  if (stopped()) {
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
  }
  if (admitted.has_value()) return {};
  if (admitted.error().code() != core::ErrorCode::kTimeout) return admitted;
  return std::unexpected(Rejected(log));
}

std::optional<core::Result<void>> DurabilityWindow::WhenAdmissible(std::size_t log,
                                                                   core::Waiter& waiter,
                                                                   bool first) {
  if (first) {
    waits_.Increment();
    if (admit_hook_) admit_hook_();
  }
  auto admitted =
      admission(log).Register(waiter, [this, log] { return stopped() || Admissible(log); });
  if (admitted.has_value() && admitted->has_value() && stopped()) {
    return core::Result<void>{
        std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"})};
  }
  return admitted;
}

core::Error DurabilityWindow::Rejected(std::size_t log) {
  rejections_.Increment();
  return core::Error{
      core::ErrorCode::kResourceExhausted,
      "WAL durability window full: the device is not keeping up with writes (unflushed " +
          std::to_string(UnflushedBytes()) + " of " + std::to_string(max_bytes_) +
          " bytes, oldest " + std::to_string(ToMillis(age(log).Age(Clock::now()))) + " of " +
          std::to_string(ToMillis(max_age_)) + " ms)"};
}

void DurabilityWindow::Add(uint64_t bytes) noexcept {
  unflushed_bytes_.fetch_add(bytes, std::memory_order_relaxed);
}

// The flusher moved the age, then the bytes, seq_cst, before Notify
// loads the waiter count. A log turns admissible only here, after one
// of those, and Add, Start and time only close it, so a log that fails
// now has no waiter this Release could admit.
void DurabilityWindow::Release(uint64_t bytes) noexcept {
  unflushed_bytes_.fetch_sub(bytes, std::memory_order_seq_cst);
  const auto now = Clock::now();
  for (std::size_t log = 0; log < logs_.size(); ++log) {
    if (Admissible(log, now)) logs_[log].admission.Notify();
  }
}

void DurabilityWindow::Shutdown() {
  stopped_.store(true, std::memory_order_seq_cst);
  const core::Error error{core::ErrorCode::kUnavailable, "queue shutting down"};
  for (Log& each : logs_) each.admission.FailAll(error);
}

}  // namespace abyss::queue
