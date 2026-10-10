#include "scheduler.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <ctime>
#include <optional>
#include <thread>

namespace abyss::perf {

namespace {

constexpr int64_t kNsPerSecond = 1'000'000'000LL;
// A spin may take at most this share of each slot.
constexpr int64_t kSlotsPerSpin = 20;
// Summed over all driver threads, spinning stays under half a core so
// it cannot crowd out a server sharing the host.
constexpr double kMaxSpinCores = 0.5;

// The platform sleep primitive; the split between platforms lives here
// and in SleepStepEnd.
void SleepTo(CoScheduler::TimePoint wake) {
#ifdef __linux__
  // steady_clock is CLOCK_MONOTONIC on Linux.
  const auto since_epoch = wake.time_since_epoch();
  const auto secs = std::chrono::duration_cast<std::chrono::seconds>(since_epoch);
  const timespec ts{
      .tv_sec = static_cast<time_t>(secs.count()),
      .tv_nsec = static_cast<long>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch - secs).count()),
  };
  while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr) == EINTR) {
  }
#else
  std::this_thread::sleep_until(wake);
#endif
}

}  // namespace

CoScheduler::CoScheduler(uint64_t target_rate_ops_per_worker, TimePoint worker_start,
                         uint64_t ops_per_slot, size_t threads, size_t index)
    : worker_start_(worker_start) {
  if (target_rate_ops_per_worker > 0) {
    interval_ns_ = kNsPerSecond / static_cast<int64_t>(target_rate_ops_per_worker);
    if (interval_ns_ <= 0) {
      interval_ns_ = 1;
    }
    const std::chrono::nanoseconds slot{interval_ns_ *
                                        static_cast<int64_t>(std::max<uint64_t>(ops_per_slot, 1))};
    const auto spread = static_cast<int64_t>(std::max<size_t>(threads, 1));
    worker_start_ += slot * static_cast<int64_t>(index % static_cast<size_t>(spread)) / spread;
    spin_window_ = SpinWindowFor(slot, threads);
  }
}

std::chrono::nanoseconds CoScheduler::SpinWindowFor(std::chrono::nanoseconds slot_interval,
                                                    size_t threads) {
  const auto host_share = std::chrono::nanoseconds{
      static_cast<int64_t>(static_cast<double>(slot_interval.count()) * kMaxSpinCores /
                           static_cast<double>(std::max<size_t>(threads, 1)))};
  return std::min(
      {std::chrono::nanoseconds{kMaxSpinWindow}, slot_interval / kSlotsPerSpin, host_share});
}

CoScheduler::TimePoint CoScheduler::IntendedSendTime(uint64_t op_index) const {
  return worker_start_ + std::chrono::nanoseconds{interval_ns_ * static_cast<int64_t>(op_index)};
}

std::optional<CoScheduler::TimePoint> CoScheduler::SleepStepEnd(TimePoint now,
                                                                TimePoint intended) const {
  const auto spin_from = intended - spin_window_;
  if (now >= spin_from) return std::nullopt;
#ifdef __APPLE__
  // A coalesced step overshoots by at most a quarter of its length, so a
  // half step cannot pass the spin window; below the floor, spin.
  if (spin_from - now < kMinSleepStep) return std::nullopt;
  return now + ((spin_from - now) / 2);
#else
  return spin_from;
#endif
}

CoScheduler::TimePoint CoScheduler::SleepUntil(TimePoint intended) const {
  while (const auto wake = SleepStepEnd(Clock::now(), intended)) {
    SleepTo(*wake);
  }
  auto now = Clock::now();
  while (now < intended) {
    CpuRelax();
    now = Clock::now();
  }
  return now;
}

void CoScheduler::CpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elifdef __aarch64__
  __asm__ __volatile__("yield");
#endif
}

}  // namespace abyss::perf
