#include "scheduler.h"

#include <chrono>
#include <thread>

namespace abyss::perf {

namespace {
constexpr int64_t kNsPerSecond = 1'000'000'000LL;
}

CoScheduler::CoScheduler(uint64_t target_rate_ops_per_worker, TimePoint worker_start)
    : worker_start_(worker_start) {
  if (target_rate_ops_per_worker > 0) {
    interval_ns_ = kNsPerSecond / static_cast<int64_t>(target_rate_ops_per_worker);
    if (interval_ns_ <= 0) {
      interval_ns_ = 1;
    }
  }
}

CoScheduler::TimePoint CoScheduler::IntendedSendTime(uint64_t op_index) const {
  return worker_start_ + std::chrono::nanoseconds{interval_ns_ * static_cast<int64_t>(op_index)};
}

CoScheduler::TimePoint CoScheduler::SleepUntil(TimePoint intended) {
  const auto now = Clock::now();
  if (now < intended) {
    std::this_thread::sleep_until(intended);
  }
  return Clock::now();
}

}  // namespace abyss::perf
