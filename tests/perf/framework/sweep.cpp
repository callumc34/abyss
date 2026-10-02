#include "sweep.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>

namespace abyss::perf {

namespace {

constexpr double kMinAchievedFraction = 0.95;
// Bisection stops once the bracket is within 1/kResolution of its
// floor.
constexpr uint64_t kResolution = 20;
constexpr uint64_t kMaxHalvings = 1024;

}  // namespace

RateSweep::RateSweep(uint64_t start_ops, int64_t p99_bound_ns, uint64_t min_ops)
    : start_ops_(std::max<uint64_t>(start_ops, 1)),
      floor_ops_(std::max({start_ops_ / kMaxHalvings, min_ops, uint64_t{1}})),
      p99_bound_ns_(p99_bound_ns) {}

std::optional<uint64_t> RateSweep::Next() const {
  if (steps_.size() >= kMaxSteps) return std::nullopt;
  if (!lowest_fail_.has_value()) {
    if (!highest_pass_.has_value()) return start_ops_;
    if (*highest_pass_ > std::numeric_limits<uint64_t>::max() / 2) return std::nullopt;
    return *highest_pass_ * 2;
  }
  if (!highest_pass_.has_value()) {
    const uint64_t lower = *lowest_fail_ / 2;
    if (lower < floor_ops_) return std::nullopt;
    return lower;
  }
  const uint64_t lo = *highest_pass_;
  const uint64_t hi = *lowest_fail_;
  if (hi - lo <= std::max<uint64_t>(lo / kResolution, 1)) return std::nullopt;
  return lo + ((hi - lo) / 2);
}

const SweepStep& RateSweep::Record(double achieved_ops, int64_t p99_ns, uint64_t errors) {
  const uint64_t offered = Next().value_or(0);
  const bool pass = errors == 0 && p99_ns <= p99_bound_ns_ &&
                    achieved_ops >= kMinAchievedFraction * static_cast<double>(offered);
  if (pass) {
    highest_pass_ = std::max(highest_pass_.value_or(0), offered);
  } else {
    lowest_fail_ = std::min(lowest_fail_.value_or(offered), offered);
  }
  steps_.push_back({.offered_ops = offered,
                    .effective_offered_ops = offered,
                    .achieved_ops = achieved_ops,
                    .p99_ns = p99_ns,
                    .errors = errors,
                    .pass = pass});
  return steps_.back();
}

}  // namespace abyss::perf
