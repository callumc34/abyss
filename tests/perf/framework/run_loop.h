#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <string_view>

#include "histogram.h"
#include "key_dist.h"
#include "workload.h"

namespace abyss::perf {

struct RunLoopConfig {
  int workers = 1;
  std::chrono::seconds duration{30};
  std::chrono::seconds warmup{0};
  // Per-worker target rate. The harness divides workload-level
  // target_rate_ops by worker count when populating this from a WorkloadConfig.
  uint64_t target_rate_ops_per_worker = 0;
  uint64_t key_count = 1;
  KeyDistConfig key_distribution;
  uint64_t value_size_bytes = 64;
  OperationMix mix;
};

struct RunLoopResult {
  std::map<std::string, Histogram> per_op_histograms;
  std::map<std::string, uint64_t> per_op_counts;
  std::chrono::nanoseconds measured_duration{0};
};

// Operation callback. Invoked once per scheduled iteration on the chosen op
// name with the chosen key index. The function must not throw.
using OpFn = std::function<void(int worker_id, std::string_view op_name, uint64_t key_index)>;

// Drives workers according to config and invokes op() once per iteration.
// Per-worker histograms are merged into the returned result.
RunLoopResult RunLoop(const RunLoopConfig& config, const OpFn& op);

// Materialise a RunLoopConfig from a WorkloadConfig. The total target rate is
// divided across workers; the workload's mix and key distribution flow through.
RunLoopConfig RunLoopConfigFromWorkload(const WorkloadConfig& workload);

}  // namespace abyss::perf
