#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "abyss/core/result.h"
#include "reporter.h"
#include "run_loop.h"
#include "workload.h"

namespace CLI {
class App;
}

namespace abyss::perf::probe {

// Common CLI surface for in-process probe binaries.
struct ProbeArgs {
  std::chrono::seconds duration{30};
  std::chrono::seconds warmup{5};
  int workers = 1;
  uint64_t target_rate_ops = 0;
  uint64_t key_count = 100'000;
  uint64_t value_size_bytes = 64;
  std::string distribution = "uniform";
  double zipf_theta = 0.99;
  uint64_t seed = 42;
  std::string mix_spec;
  std::string output_path;
  std::string hgrm_dir;
  bool gate = false;
  std::string run_id;
  std::string workload_name;
};

// Register the standard probe CLI options on a CLI11 App.
void RegisterCliOptions(CLI::App& app, ProbeArgs& args);

// Build a RunLoopConfig from ProbeArgs and a default mix. The mix is used if
// args.mix_spec is empty.
RunLoopConfig MakeRunLoopConfig(const ProbeArgs& args, const OperationMix& default_mix);

// Parse "GET=0.95,SET=0.05" into an OperationMix.
core::Result<OperationMix> ParseMixSpec(const std::string& spec);

// Build a minimal WorkloadConfig describing this probe run (for inclusion in
// the JSON report). The targets map is populated from `targets`.
WorkloadConfig MakeWorkloadConfig(const ProbeArgs& args, const OperationMix& mix,
                                  const WorkloadTargets& targets);

// Assemble a RunReport from a RunLoopResult plus probe metadata. Calls
// EvaluateTargets internally.
RunReport BuildReport(const ProbeArgs& args, const WorkloadConfig& workload,
                      const RunLoopResult& result);

// Emit JSON to args.output_path (or stdout if empty) and a summary line
// to stderr. Returns false on I/O failure.
bool WriteOutputs(const RunReport& report, const ProbeArgs& args, const RunLoopResult& result);

// Exit status when any measured op failed; errors are never timed.
inline constexpr int kExitOpErrors = 4;

}  // namespace abyss::perf::probe
