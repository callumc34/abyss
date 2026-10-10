#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace abyss::perf {

struct SweepStep {
  uint64_t offered_ops = 0;
  // What the driver's per-connection rates actually summed to.
  uint64_t effective_offered_ops = 0;
  double achieved_ops = 0.0;
  int64_t p99_ns = 0;
  uint64_t errors = 0;
  bool pass = false;
  // Server queue position after the step, where the server reports it.
  std::optional<double> queue_entries;
  std::optional<double> queue_bytes;
};

// Finds the highest open-loop rate that holds a p99 bound: doubles from
// the start rate until a step fails (or halves until one passes, down
// to 1/1024 of the start), then bisects between the highest passing and
// lowest failing rate until they are within 5%. A step passes with no
// errors, p99 within the bound, and at least 95% of the offered rate
// achieved. Pure; the caller runs each step.
class RateSweep {
 public:
  // `min_ops` raises the halving floor, e.g. to one request/s per
  // connection.
  RateSweep(uint64_t start_ops, int64_t p99_bound_ns, uint64_t min_ops = 1);

  // The rate to run next, or nullopt once the sweep has converged.
  std::optional<uint64_t> Next() const;

  // Judges the step just run at Next() and records it.
  const SweepStep& Record(double achieved_ops, int64_t p99_ns, uint64_t errors);

  // Highest passing offered rate; nullopt when none passed.
  std::optional<uint64_t> Result() const { return highest_pass_; }
  const std::vector<SweepStep>& Steps() const { return steps_; }

 private:
  static constexpr size_t kMaxSteps = 40;

  uint64_t start_ops_;
  uint64_t floor_ops_;
  int64_t p99_bound_ns_;
  std::optional<uint64_t> highest_pass_;
  std::optional<uint64_t> lowest_fail_;
  std::vector<SweepStep> steps_;
};

}  // namespace abyss::perf
