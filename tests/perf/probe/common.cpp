#include "common.h"

#include <CLI/CLI.hpp>
#include <charconv>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <system_error>

#include "abyss/core/result.h"

namespace abyss::perf::probe {

namespace {

using core::Error;
using core::ErrorCode;
using core::Result;

KeyDistConfig::Kind ParseDistKind(const std::string& s) {
  if (s == "uniform") return KeyDistConfig::Kind::kUniform;
  if (s == "zipfian") return KeyDistConfig::Kind::kZipfian;
  if (s == "latest") return KeyDistConfig::Kind::kLatest;
  return KeyDistConfig::Kind::kUniform;
}

}  // namespace

void RegisterCliOptions(CLI::App& app, ProbeArgs& args) {
  app.add_option("--duration", args.duration, "Measurement duration (seconds)");
  app.add_option("--warmup", args.warmup, "Warmup duration before measurement (seconds)");
  app.add_option("--workers", args.workers, "Worker thread count");
  app.add_option("--target-rate-ops", args.target_rate_ops,
                 "Aggregate target rate (0 = closed-loop)");
  app.add_option("--key-count", args.key_count, "Key space size");
  app.add_option("--value-size-bytes", args.value_size_bytes, "Value size in bytes");
  app.add_option("--distribution", args.distribution,
                 "Key distribution: uniform | zipfian | latest");
  app.add_option("--zipf-theta", args.zipf_theta, "Zipfian/Latest theta in [0, 1)");
  app.add_option("--seed", args.seed, "Base random seed");
  app.add_option("--mix", args.mix_spec, "Override op mix (e.g. \"GET=0.95,SET=0.05\")");
  app.add_option("--output", args.output_path, "JSON report path (omit for stdout)");
  app.add_option("--hgrm-dir", args.hgrm_dir, "Directory to write per-op .hgrm files");
  app.add_flag("--gate", args.gate, "Exit non-zero if any target fails");
  app.add_option("--run-id", args.run_id, "Optional run identifier");
  app.add_option("--workload-name", args.workload_name, "Workload name for reporting");
}

core::Result<OperationMix> ParseMixSpec(const std::string& spec) {
  OperationMix mix;
  if (spec.empty()) {
    return mix;
  }
  double sum = 0.0;
  std::stringstream stream{spec};
  std::string token;
  while (std::getline(stream, token, ',')) {
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      return std::unexpected(Error(ErrorCode::kInvalidArgument, "bad mix token: " + token));
    }
    auto name = token.substr(0, eq);
    auto weight_str = token.substr(eq + 1);
    double weight = 0.0;
    const auto [ptr, ec] =
        std::from_chars(weight_str.data(), weight_str.data() + weight_str.size(), weight);
    if (ec != std::errc{} || ptr != weight_str.data() + weight_str.size()) {
      return std::unexpected(Error(ErrorCode::kInvalidArgument, "bad mix weight: " + weight_str));
    }
    mix.weights[name] = weight;
    sum += weight;
  }
  if (std::abs(sum - 1.0) > 1e-6) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument,
                                 "mix weights must sum to 1.0, got " + std::to_string(sum)));
  }
  return mix;
}

RunLoopConfig MakeRunLoopConfig(const ProbeArgs& args, const OperationMix& default_mix) {
  RunLoopConfig cfg;
  cfg.workers = args.workers;
  cfg.duration = args.duration;
  cfg.warmup = args.warmup;
  cfg.target_rate_ops = args.target_rate_ops;
  cfg.key_count = args.key_count;
  cfg.value_size_bytes = args.value_size_bytes;
  cfg.key_distribution.kind = ParseDistKind(args.distribution);
  cfg.key_distribution.theta = args.zipf_theta;
  cfg.key_distribution.seed = args.seed;
  if (!args.mix_spec.empty()) {
    auto parsed = ParseMixSpec(args.mix_spec);
    if (parsed.has_value()) {
      cfg.mix = *parsed;
    } else {
      cfg.mix = default_mix;
    }
  } else {
    cfg.mix = default_mix;
  }
  return cfg;
}

WorkloadConfig MakeWorkloadConfig(const ProbeArgs& args, const OperationMix& mix,
                                  const WorkloadTargets& targets) {
  WorkloadConfig wl;
  wl.name = args.workload_name.empty() ? "probe" : args.workload_name;
  wl.description = "in-process probe";
  wl.duration = args.duration;
  wl.warmup = args.warmup;
  wl.workers = args.workers;
  wl.connections_per_worker = 1;
  wl.target_rate_ops = args.target_rate_ops;
  wl.key_count = args.key_count;
  wl.value_size_bytes = args.value_size_bytes;
  wl.key_distribution.kind = ParseDistKind(args.distribution);
  wl.key_distribution.theta = args.zipf_theta;
  wl.key_distribution.seed = args.seed;
  wl.mix = mix;
  wl.targets = targets;
  return wl;
}

RunReport BuildReport(const ProbeArgs& args, const WorkloadConfig& workload,
                      const RunLoopResult& result) {
  RunReport report;
  report.run_id = args.run_id.empty()
                      ? std::to_string(std::chrono::system_clock::now().time_since_epoch().count())
                      : args.run_id;
  report.started_at = std::chrono::system_clock::now() - workload.duration;
  report.duration = workload.duration;
  report.build = CurrentBuildInfo();
  report.host = CurrentHostInfo();
  report.classification = DetectClassification();
  report.workload = workload;
  for (const auto& [name, hist] : result.per_op_histograms) {
    const auto count = result.per_op_counts.at(name);
    report.operations[name] = StatsFromHistogram(hist, count, result.measured_duration);
    report.operations[name].errors = result.per_op_errors.at(name);
  }
  report.driver = MakeDriverStats(result.send_lag, result.open_loop, result.driver_cpu,
                                  result.measured_duration, workload.targets);
  EvaluateTargets(report);
  return report;
}

bool WriteOutputs(const RunReport& report, const ProbeArgs& args, const RunLoopResult& result) {
  if (args.output_path.empty()) {
    WriteReportJson(report, std::cout);
    std::cout << '\n';
  } else {
    std::ofstream out{args.output_path};
    if (!out.is_open()) {
      std::cerr << "failed to open output: " << args.output_path << '\n';
      return false;
    }
    WriteReportJson(report, out);
  }

  if (!args.hgrm_dir.empty()) {
    std::error_code ec;
    std::filesystem::create_directories(args.hgrm_dir, ec);
    if (ec) {
      std::cerr << "failed to create hgrm dir: " << ec.message() << '\n';
      return false;
    }
    for (const auto& [name, hist] : result.per_op_histograms) {
      const auto path = std::filesystem::path{args.hgrm_dir} / (name + ".hgrm");
      std::ofstream out{path};
      if (!out.is_open()) {
        std::cerr << "failed to open hgrm: " << path << '\n';
        return false;
      }
      hist.WriteHgrmTo(out);
    }
  }

  WriteSummary(report, std::cerr);
  return true;
}

}  // namespace abyss::perf::probe
