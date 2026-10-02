#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>

#include "abyss/core/result.h"
#include "key_dist.h"

namespace abyss::perf {

struct OperationMix {
  std::map<std::string, double> weights;
};

struct TargetSpec {
  std::optional<int64_t> p50_us;
  std::optional<int64_t> p99_us;
  std::optional<int64_t> p999_us;
};

struct WorkloadTargets {
  std::map<std::string, TargetSpec> per_op;
  std::optional<int64_t> throughput_ops;
};

struct PreloadConfig {
  bool enabled = false;
  uint64_t key_count = 0;
  uint64_t value_size_bytes = 64;
};

// How open-loop requests arrive: one per slot, or a pipeline-depth
// burst per slot at the same request rate.
enum class Arrival : uint8_t { kSteady, kBurst };

struct WorkloadConfig {
  std::string name;
  std::string description;
  std::chrono::seconds duration{0};
  std::chrono::seconds warmup{0};
  int workers = 1;
  int connections_per_worker = 1;
  // Requests each connection keeps in flight.
  int pipeline_depth = 1;
  Arrival arrival = Arrival::kSteady;
  uint64_t target_rate_ops = 0;
  uint64_t key_count = 0;
  KeyDistConfig key_distribution;
  uint64_t value_size_bytes = 64;
  OperationMix mix;
  PreloadConfig preload;
  WorkloadTargets targets;
};

core::Result<WorkloadConfig> ParseWorkloadYaml(const std::string& yaml);
core::Result<WorkloadConfig> LoadWorkloadFile(const std::string& path);

}  // namespace abyss::perf
