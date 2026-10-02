#pragma once

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "histogram.h"
#include "server_identity.h"
#include "sweep.h"
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
  // Failed ops: counted here, absent from the latency histogram.
  uint64_t errors = 0;
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
  bool evaluated = true;
  bool pass = false;
};

// How closely the driver kept its open-loop schedule, and what it cost.
struct DriverStats {
  bool open_loop = false;
  uint64_t send_lag_count = 0;
  int64_t send_lag_p50_ns = 0;
  int64_t send_lag_p99_ns = 0;
  int64_t send_lag_max_ns = 0;
  // Send lag too large for the latency figures to be trusted.
  bool lagging = false;
  // Driver threads only, over the measured window.
  double cpu_seconds = 0.0;
  double cpu_per_wall_second = 0.0;
  double overload_threshold_cores = 0.0;
  // Busy enough to compete with a server sharing the host.
  bool overloaded = false;
  // CPUs the driver may run on, e.g. "0-3,8"; empty where unknown.
  std::string cpu_affinity;
  uint32_t cpu_affinity_count = 0;
};

struct RunReport {
  static constexpr int kSchemaVersion = 1;

  std::string run_id;
  std::chrono::system_clock::time_point started_at;
  std::chrono::seconds duration{0};
  BuildInfo build;
  HostInfo host;
  HostClassification classification = HostClassification::kIndicative;
  // The server a load run measured; absent for in-process probes.
  std::optional<ServerIdentity> server;
  // Effective configuration of the measured system, as key/value strings.
  std::map<std::string, std::string> config;
  WorkloadConfig workload;
  std::map<std::string, OperationStats> operations;
  DriverStats driver;
  std::vector<MetricSnapshot> server_metrics;
  // Non-empty: targets are listed unevaluated and pass stays false.
  std::string targets_not_evaluated;
  std::vector<TargetEvaluation> targets;
  std::vector<SweepStep> sweep;
  std::optional<uint64_t> sweep_result_ops;
  bool pass = false;
};

OperationStats StatsFromHistogram(const Histogram& h, uint64_t count,
                                  std::chrono::nanoseconds wallclock_duration);

void EvaluateTargets(RunReport& report);

// Lagging when send-lag p99 exceeds 50us or 10% of a per-op p99 target.
// Overloaded when driver CPU per wall-second exceeds the larger of one
// core and 10% of the host's hardware threads.
DriverStats MakeDriverStats(const Histogram& send_lag, bool open_loop,
                            std::chrono::nanoseconds driver_cpu,
                            std::chrono::nanoseconds measured_duration,
                            const WorkloadTargets& targets);

// One human-readable line per run, plus any driver-lag warning.
void WriteSummary(const RunReport& report, std::ostream& out);

HostClassification DetectClassification();
BuildInfo CurrentBuildInfo();
HostInfo CurrentHostInfo();

void WriteReportJson(const RunReport& report, std::ostream& out);

}  // namespace abyss::perf
