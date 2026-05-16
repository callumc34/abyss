#pragma once

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

#include "histogram.h"
#include "workload.h"

namespace abyss::perf {

struct BuildInfo {
  std::string commit;
  std::string preset;
  std::string compiler;
  std::string build_type;
  std::string sanitizer;
};

struct HostInfo {
  std::string os;
  std::string kernel;
  std::string cpu_model;
  std::string hostname;
};

enum class HostClassification {
  kIndicative,
  kAuthoritative,
};

struct OperationStats {
  uint64_t count = 0;
  double throughput_ops = 0.0;
  int64_t p50_ns = 0;
  int64_t p99_ns = 0;
  int64_t p999_ns = 0;
  int64_t max_ns = 0;
  double noise_floor_cv = 0.0;
  bool saturated = false;
  std::string histogram_b64;
};

struct MetricSnapshot {
  std::string phase;
  std::map<std::string, double> metrics;
};

struct TargetEvaluation {
  std::string metric;
  double target = 0.0;
  double actual = 0.0;
  bool pass = false;
};

struct RunReport {
  static constexpr int kSchemaVersion = 1;

  std::string run_id;
  std::chrono::system_clock::time_point started_at;
  std::chrono::seconds duration{0};
  BuildInfo build;
  HostInfo host;
  HostClassification classification = HostClassification::kIndicative;
  WorkloadConfig workload;
  std::map<std::string, OperationStats> operations;
  std::vector<MetricSnapshot> server_metrics;
  std::vector<TargetEvaluation> targets;
  bool pass = false;
};

OperationStats StatsFromHistogram(const Histogram& h, uint64_t count,
                                  std::chrono::nanoseconds wallclock_duration);

void EvaluateTargets(RunReport& report);

HostClassification DetectClassification();
BuildInfo CurrentBuildInfo();
HostInfo CurrentHostInfo();

void WriteReportJson(const RunReport& report, std::ostream& out);

}  // namespace abyss::perf
