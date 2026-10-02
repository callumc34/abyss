#include "reporter.h"

#include <sys/utsname.h>
#include <unistd.h>

#ifdef __linux__
#include <sched.h>
#endif

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "abyss/version.h"

namespace abyss::perf {

namespace {

constexpr int64_t kNsPerUs = 1000;
constexpr int64_t kMaxSendLagNs = 50'000;
// Lag above this fraction of a p99 target would show in the target.
constexpr double kMaxSendLagTargetFraction = 0.1;

// The driver may use up to the larger of one core and this share of the
// host's hardware threads before it competes with the server.
constexpr double kOverloadHostShare = 0.1;

double Us(int64_t ns) { return static_cast<double>(ns) / kNsPerUs; }

// "0-3,8" style list of the CPUs this process may run on.
void ReadCpuAffinity(DriverStats& d) {
#ifdef __linux__
  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) != 0) return;
  std::string list;
  int run_start = -1;
  for (int cpu = 0; cpu <= CPU_SETSIZE; ++cpu) {
    const bool in = cpu < CPU_SETSIZE && CPU_ISSET(cpu, &set);
    if (in && run_start < 0) run_start = cpu;
    if (!in && run_start >= 0) {
      if (!list.empty()) list += ',';
      list += std::to_string(run_start);
      if (cpu - 1 > run_start) list += '-' + std::to_string(cpu - 1);
      run_start = -1;
    }
  }
  d.cpu_affinity = list;
  d.cpu_affinity_count = static_cast<uint32_t>(CPU_COUNT(&set));
#else
  (void)d;
#endif
}

std::string EscapeJson(std::string_view in) {
  std::string out;
  out.reserve(in.size() + 2);
  for (const char c : in) {
    switch (c) {
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          std::array<char, 8> buf{};
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
          std::snprintf(buf.data(), buf.size(), "\\u%04x", static_cast<unsigned char>(c));
          out.append(buf.data());
        } else {
          out.push_back(c);
        }
    }
  }
  return out;
}

void WriteString(std::ostream& out, std::string_view s) { out << '"' << EscapeJson(s) << '"'; }

void WriteKey(std::ostream& out, std::string_view key) {
  WriteString(out, key);
  out << ':';
}

std::string FormatTimestamp(std::chrono::system_clock::time_point t) {
  const auto tt = std::chrono::system_clock::to_time_t(t);
  std::tm buf{};
#ifdef _WIN32
  gmtime_s(&buf, &tt);
#else
  gmtime_r(&tt, &buf);
#endif
  std::array<char, 32> formatted{};
  std::strftime(formatted.data(), formatted.size(), "%Y-%m-%dT%H:%M:%SZ", &buf);
  return std::string{formatted.data()};
}

std::string CompilerString() {
#ifdef __clang__
  return std::string{"clang "} + __clang_version__;
#elifdef __GNUC__
  return std::string{"gcc "} + std::to_string(__GNUC__) + "." + std::to_string(__GNUC_MINOR__) +
         "." + std::to_string(__GNUC_PATCHLEVEL__);
#elifdef _MSC_VER
  return std::string{"msvc "} + std::to_string(_MSC_VER);
#else
  return "unknown";
#endif
}

std::string OsString(const utsname& info) {
  std::string sys{info.sysname};
  std::ranges::transform(sys, sys.begin(),
                         [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return sys;
}

std::string ReadCpuModelLinux() {
  std::ifstream f{"/proc/cpuinfo"};
  if (!f.is_open()) return {};
  std::string line;
  while (std::getline(f, line)) {
    constexpr std::string_view kPrefix = "model name";
    if (line.starts_with(kPrefix)) {
      const auto colon = line.find(':');
      if (colon == std::string::npos) continue;
      auto value = line.substr(colon + 1);
      const auto first = value.find_first_not_of(" \t");
      if (first != std::string::npos) value = value.substr(first);
      return value;
    }
  }
  return {};
}

std::string SanitizerName() {
#ifdef __SANITIZE_ADDRESS__
  return "address";
#elifdef __SANITIZE_THREAD__
  return "thread";
#else
#ifdef __has_feature
#if __has_feature(address_sanitizer)
  return "address";
#elif __has_feature(thread_sanitizer)
  return "thread";
#elif __has_feature(undefined_behavior_sanitizer)
  return "undefined";
#endif
#endif
  return "none";
#endif
}

const char* ClassificationString(HostClassification c) {
  switch (c) {
    case HostClassification::kIndicative:
      return "indicative";
    case HostClassification::kAuthoritative:
      return "authoritative";
  }
  return "indicative";
}

void WriteMetricsMap(std::ostream& out, const std::map<std::string, double>& m) {
  out << '{';
  bool first = true;
  for (const auto& [k, v] : m) {
    if (!first) out << ',';
    WriteKey(out, k);
    out << v;
    first = false;
  }
  out << '}';
}

void WriteKeyDistConfig(std::ostream& out, const KeyDistConfig& kd) {
  out << '{';
  WriteKey(out, "kind");
  switch (kd.kind) {
    case KeyDistConfig::Kind::kUniform:
      WriteString(out, "uniform");
      break;
    case KeyDistConfig::Kind::kZipfian:
      WriteString(out, "zipfian");
      break;
    case KeyDistConfig::Kind::kLatest:
      WriteString(out, "latest");
      break;
  }
  out << ',';
  WriteKey(out, "theta");
  out << kd.theta;
  out << ',';
  WriteKey(out, "seed");
  out << kd.seed;
  out << '}';
}

void WriteWorkload(std::ostream& out, const WorkloadConfig& w) {
  out << '{';
  WriteKey(out, "name");
  WriteString(out, w.name);
  out << ',';
  WriteKey(out, "description");
  WriteString(out, w.description);
  out << ',';
  WriteKey(out, "duration_seconds");
  out << w.duration.count();
  out << ',';
  WriteKey(out, "warmup_seconds");
  out << w.warmup.count();
  out << ',';
  WriteKey(out, "workers");
  out << w.workers;
  out << ',';
  WriteKey(out, "connections_per_worker");
  out << w.connections_per_worker;
  out << ',';
  WriteKey(out, "pipeline_depth");
  out << w.pipeline_depth;
  out << ',';
  WriteKey(out, "arrival");
  WriteString(out, w.arrival == Arrival::kBurst ? "burst" : "steady");
  out << ',';
  WriteKey(out, "target_rate_ops");
  out << w.target_rate_ops;
  out << ',';
  WriteKey(out, "key_count");
  out << w.key_count;
  out << ',';
  WriteKey(out, "value_size_bytes");
  out << w.value_size_bytes;
  out << ',';
  WriteKey(out, "key_distribution");
  WriteKeyDistConfig(out, w.key_distribution);
  out << ',';
  WriteKey(out, "mix");
  out << '{';
  bool first = true;
  for (const auto& [name, weight] : w.mix.weights) {
    if (!first) out << ',';
    WriteKey(out, name);
    out << weight;
    first = false;
  }
  out << '}';
  out << '}';
}

void WriteOperation(std::ostream& out, const std::string& name, const OperationStats& s) {
  WriteKey(out, name);
  out << '{';
  WriteKey(out, "count");
  out << s.count;
  out << ',';
  WriteKey(out, "errors");
  out << s.errors;
  out << ',';
  WriteKey(out, "throughput_ops");
  out << s.throughput_ops;
  out << ',';
  WriteKey(out, "latency_us");
  out << '{';
  WriteKey(out, "p50");
  out << (static_cast<double>(s.p50_ns) / kNsPerUs);
  out << ',';
  WriteKey(out, "p99");
  out << (static_cast<double>(s.p99_ns) / kNsPerUs);
  out << ',';
  WriteKey(out, "p999");
  out << (static_cast<double>(s.p999_ns) / kNsPerUs);
  out << ',';
  WriteKey(out, "max");
  out << (static_cast<double>(s.max_ns) / kNsPerUs);
  out << ',';
  WriteKey(out, "noise_floor_cv");
  out << s.noise_floor_cv;
  out << '}';
  out << ',';
  WriteKey(out, "saturated");
  out << (s.saturated ? "true" : "false");
  out << ',';
  WriteKey(out, "histogram_b64");
  WriteString(out, s.histogram_b64);
  out << '}';
}

void WriteStringMap(std::ostream& out, const std::map<std::string, std::string>& m) {
  out << '{';
  bool first = true;
  for (const auto& [k, v] : m) {
    if (!first) out << ',';
    WriteKey(out, k);
    WriteString(out, v);
    first = false;
  }
  out << '}';
}

void WriteDriver(std::ostream& out, const DriverStats& d) {
  out << '{';
  WriteKey(out, "open_loop");
  out << (d.open_loop ? "true" : "false");
  out << ',';
  WriteKey(out, "send_lag_us");
  out << '{';
  WriteKey(out, "count");
  out << d.send_lag_count;
  out << ',';
  WriteKey(out, "p50");
  out << Us(d.send_lag_p50_ns);
  out << ',';
  WriteKey(out, "p99");
  out << Us(d.send_lag_p99_ns);
  out << ',';
  WriteKey(out, "max");
  out << Us(d.send_lag_max_ns);
  out << '}';
  out << ',';
  WriteKey(out, "lagging");
  out << (d.lagging ? "true" : "false");
  out << ',';
  WriteKey(out, "cpu_seconds");
  out << d.cpu_seconds;
  out << ',';
  WriteKey(out, "cpu_per_wall_second");
  out << d.cpu_per_wall_second;
  out << ',';
  WriteKey(out, "overload_threshold_cores");
  out << d.overload_threshold_cores;
  out << ',';
  WriteKey(out, "overloaded");
  out << (d.overloaded ? "true" : "false");
  out << ',';
  WriteKey(out, "cpu_affinity");
  out << '{';
  WriteKey(out, "available");
  out << (d.cpu_affinity.empty() ? "false" : "true");
  if (!d.cpu_affinity.empty()) {
    out << ',';
    WriteKey(out, "count");
    out << d.cpu_affinity_count;
    out << ',';
    WriteKey(out, "cpus");
    WriteString(out, d.cpu_affinity);
  }
  out << '}';
  out << '}';
}

void WriteSweep(std::ostream& out, const std::vector<SweepStep>& steps) {
  out << '[';
  bool first = true;
  for (const auto& step : steps) {
    if (!first) out << ',';
    out << '{';
    WriteKey(out, "offered_ops");
    out << step.offered_ops;
    out << ',';
    WriteKey(out, "effective_offered_ops");
    out << step.effective_offered_ops;
    out << ',';
    WriteKey(out, "achieved_ops");
    out << step.achieved_ops;
    out << ',';
    WriteKey(out, "p99_us");
    out << Us(step.p99_ns);
    out << ',';
    WriteKey(out, "errors");
    out << step.errors;
    out << ',';
    if (step.queue_entries.has_value()) {
      WriteKey(out, "queue_entries");
      out << *step.queue_entries;
      out << ',';
    }
    if (step.queue_bytes.has_value()) {
      WriteKey(out, "queue_bytes");
      out << *step.queue_bytes;
      out << ',';
    }
    WriteKey(out, "pass");
    out << (step.pass ? "true" : "false");
    out << '}';
    first = false;
  }
  out << ']';
}

// The durability setting, whichever spelling the source reported.
std::string DurabilitySetting(const std::map<std::string, std::string>& config) {
  for (const char* key : {"fsync_policy", "wal-fsync-policy", "appendfsync"}) {
    if (const auto it = config.find(key); it != config.end()) return key + ("=" + it->second);
  }
  return "durability=unknown";
}

}  // namespace

OperationStats StatsFromHistogram(const Histogram& h, uint64_t count,
                                  std::chrono::nanoseconds wallclock_duration) {
  OperationStats stats;
  stats.count = count;
  if (wallclock_duration.count() > 0) {
    stats.throughput_ops =
        static_cast<double>(count) * 1e9 / static_cast<double>(wallclock_duration.count());
  }
  stats.p50_ns = h.PercentileNs(50.0);
  stats.p99_ns = h.PercentileNs(99.0);
  stats.p999_ns = h.PercentileNs(99.9);
  stats.max_ns = h.MaxNs();
  stats.saturated = h.Saturated();
  if (h.MeanNs() > 0.0) {
    stats.noise_floor_cv = h.StdDevNs() / h.MeanNs();
  }
  stats.histogram_b64 = h.EncodeBase64();
  return stats;
}

void EvaluateTargets(RunReport& report) {
  report.targets.clear();
  bool overall = true;
  const auto& wt = report.workload.targets;
  for (const auto& [op_name, spec] : wt.per_op) {
    const auto it = report.operations.find(op_name);
    if (it == report.operations.end()) continue;
    const auto& s = it->second;
    if (spec.p50_us.has_value()) {
      TargetEvaluation eval;
      eval.metric = "operations." + op_name + ".p50_us";
      eval.target = static_cast<double>(*spec.p50_us);
      eval.actual = static_cast<double>(s.p50_ns) / kNsPerUs;
      eval.pass = eval.actual <= eval.target;
      report.targets.push_back(eval);
      overall = overall && eval.pass;
    }
    if (spec.p99_us.has_value()) {
      TargetEvaluation eval;
      eval.metric = "operations." + op_name + ".p99_us";
      eval.target = static_cast<double>(*spec.p99_us);
      eval.actual = static_cast<double>(s.p99_ns) / kNsPerUs;
      eval.pass = eval.actual <= eval.target;
      report.targets.push_back(eval);
      overall = overall && eval.pass;
    }
    if (spec.p999_us.has_value()) {
      TargetEvaluation eval;
      eval.metric = "operations." + op_name + ".p999_us";
      eval.target = static_cast<double>(*spec.p999_us);
      eval.actual = static_cast<double>(s.p999_ns) / kNsPerUs;
      eval.pass = eval.actual <= eval.target;
      report.targets.push_back(eval);
      overall = overall && eval.pass;
    }
  }
  if (wt.throughput_ops.has_value()) {
    double total = 0.0;
    for (const auto& [_, s] : report.operations) total += s.throughput_ops;
    TargetEvaluation eval;
    eval.metric = "throughput_ops";
    eval.target = static_cast<double>(*wt.throughput_ops);
    eval.actual = total;
    eval.pass = eval.actual >= eval.target;
    report.targets.push_back(eval);
    overall = overall && eval.pass;
  }
  if (!report.targets_not_evaluated.empty()) {
    for (auto& t : report.targets) {
      t.evaluated = false;
      t.pass = false;
    }
    overall = false;
  }
  report.pass = overall && !report.targets.empty();
}

DriverStats MakeDriverStats(const Histogram& send_lag, bool open_loop,
                            std::chrono::nanoseconds driver_cpu,
                            std::chrono::nanoseconds measured_duration,
                            const WorkloadTargets& targets) {
  DriverStats d;
  d.open_loop = open_loop;
  d.send_lag_count = static_cast<uint64_t>(send_lag.Count());
  d.send_lag_p50_ns = send_lag.PercentileNs(50.0);
  d.send_lag_p99_ns = send_lag.PercentileNs(99.0);
  d.send_lag_max_ns = send_lag.MaxNs();
  if (open_loop && d.send_lag_count > 0) {
    d.lagging = d.send_lag_p99_ns > kMaxSendLagNs;
    for (const auto& [_, spec] : targets.per_op) {
      if (spec.p99_us.has_value() &&
          Us(d.send_lag_p99_ns) > kMaxSendLagTargetFraction * static_cast<double>(*spec.p99_us)) {
        d.lagging = true;
      }
    }
  }
  d.cpu_seconds = std::chrono::duration<double>(driver_cpu).count();
  const double wall_seconds = std::chrono::duration<double>(measured_duration).count();
  if (wall_seconds > 0.0) d.cpu_per_wall_second = d.cpu_seconds / wall_seconds;
  const auto hw_threads = std::max(std::thread::hardware_concurrency(), 1U);
  d.overload_threshold_cores = std::max(1.0, kOverloadHostShare * static_cast<double>(hw_threads));
  d.overloaded = d.cpu_per_wall_second > d.overload_threshold_cores;
  ReadCpuAffinity(d);
  return d;
}

void WriteSummary(const RunReport& report, std::ostream& out) {
  out << "summary: "
      << (report.server.has_value() ? report.server->kind + ' ' + report.server->version
                                    : std::string{"in-process"})
      << ' ' << DurabilitySetting(report.config);
  for (const auto& [name, s] : report.operations) {
    out << " | " << name << " n=" << s.count << " errors=" << s.errors << " p50=" << Us(s.p50_ns)
        << "us p99=" << Us(s.p99_ns) << "us p999=" << Us(s.p999_ns) << "us";
  }
  out << " | driver cpu=" << report.driver.cpu_per_wall_second
      << " cores send_lag_p99=" << Us(report.driver.send_lag_p99_ns) << "us\n";
  for (const auto& step : report.sweep) {
    out << "sweep: offered=" << step.offered_ops << " achieved=" << step.achieved_ops
        << " p99=" << Us(step.p99_ns) << "us errors=" << step.errors
        << (step.pass ? " pass" : " fail") << '\n';
  }
  if (!report.sweep.empty()) {
    out << "sweep result: "
        << (report.sweep_result_ops.has_value() ? std::to_string(*report.sweep_result_ops)
                                                : std::string{"no rate held the bound"})
        << '\n';
  }
  if (!report.targets_not_evaluated.empty())
    out << "targets: " << report.targets_not_evaluated << '\n';
  if (report.driver.overloaded) {
    out << "warning: driver used " << report.driver.cpu_per_wall_second << " cores, over the "
        << report.driver.overload_threshold_cores
        << "-core threshold; it competes with a server on this host\n";
  }
  if (report.driver.lagging) {
    out << "warning: driver send lag p99 " << Us(report.driver.send_lag_p99_ns)
        << "us exceeds 50us or 10% of a p99 target; latency figures include driver delay\n";
  }
}

HostClassification DetectClassification() {
  utsname info{};
  if (uname(&info) != 0) return HostClassification::kIndicative;
  const std::string sys = OsString(info);
  if (sys == "linux") {
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables): false positive on if-init.
    if (const char* override_env = std::getenv("ABYSS_PERF_AUTHORITATIVE");
        override_env != nullptr && std::strcmp(override_env, "1") == 0) {
      return HostClassification::kAuthoritative;
    }
  }
  return HostClassification::kIndicative;
}

BuildInfo CurrentBuildInfo() {
  BuildInfo b;
  b.commit = kBuildCommit;
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables): false positive on if-init.
  if (const char* preset = std::getenv("ABYSS_PRESET"); preset != nullptr) {
    b.preset = preset;
  }
  b.compiler = CompilerString();
#ifdef NDEBUG
  b.build_type = "Release";
#else
  b.build_type = "Debug";
#endif
  b.sanitizer = SanitizerName();
  return b;
}

HostInfo CurrentHostInfo() {
  HostInfo h;
  utsname info{};
  if (uname(&info) == 0) {
    h.os = OsString(info);
    h.kernel = info.release;
  }
  std::array<char, 256> hostname{};
  if (gethostname(hostname.data(), hostname.size()) == 0) {
    h.hostname = hostname.data();
  }
  if (h.os == "linux") {
    h.cpu_model = ReadCpuModelLinux();
  }
  return h;
}

void WriteReportJson(const RunReport& report, std::ostream& out) {
  out << '{';
  WriteKey(out, "schema_version");
  out << RunReport::kSchemaVersion;
  out << ',';
  WriteKey(out, "run_id");
  WriteString(out, report.run_id);
  out << ',';
  WriteKey(out, "started_at");
  WriteString(out, FormatTimestamp(report.started_at));
  out << ',';
  WriteKey(out, "duration_seconds");
  out << report.duration.count();
  out << ',';
  WriteKey(out, "classification");
  WriteString(out, ClassificationString(report.classification));
  out << ',';
  WriteKey(out, "build");
  out << '{';
  WriteKey(out, "commit");
  WriteString(out, report.build.commit);
  out << ',';
  WriteKey(out, "preset");
  WriteString(out, report.build.preset);
  out << ',';
  WriteKey(out, "compiler");
  WriteString(out, report.build.compiler);
  out << ',';
  WriteKey(out, "build_type");
  WriteString(out, report.build.build_type);
  out << ',';
  WriteKey(out, "sanitizer");
  WriteString(out, report.build.sanitizer);
  out << '}';
  out << ',';
  WriteKey(out, "host");
  out << '{';
  WriteKey(out, "os");
  WriteString(out, report.host.os);
  out << ',';
  WriteKey(out, "kernel");
  WriteString(out, report.host.kernel);
  out << ',';
  WriteKey(out, "cpu_model");
  WriteString(out, report.host.cpu_model);
  out << ',';
  WriteKey(out, "hostname");
  WriteString(out, report.host.hostname);
  out << '}';
  out << ',';
  if (report.server.has_value()) {
    WriteKey(out, "server");
    out << '{';
    WriteKey(out, "kind");
    WriteString(out, report.server->kind);
    out << ',';
    WriteKey(out, "version");
    WriteString(out, report.server->version);
    out << '}';
    out << ',';
  }
  WriteKey(out, "config");
  WriteStringMap(out, report.config);
  out << ',';
  WriteKey(out, "workload");
  WriteWorkload(out, report.workload);
  out << ',';
  WriteKey(out, "operations");
  out << '{';
  bool first = true;
  for (const auto& [name, stats] : report.operations) {
    if (!first) out << ',';
    WriteOperation(out, name, stats);
    first = false;
  }
  out << '}';
  out << ',';
  WriteKey(out, "driver");
  WriteDriver(out, report.driver);
  out << ',';
  WriteKey(out, "server_metrics");
  out << '[';
  first = true;
  for (const auto& snap : report.server_metrics) {
    if (!first) out << ',';
    out << '{';
    WriteKey(out, "phase");
    WriteString(out, snap.phase);
    out << ',';
    WriteKey(out, "metrics");
    WriteMetricsMap(out, snap.metrics);
    out << '}';
    first = false;
  }
  out << ']';
  out << ',';
  WriteKey(out, "targets");
  out << '[';
  first = true;
  for (const auto& t : report.targets) {
    if (!first) out << ',';
    out << '{';
    WriteKey(out, "metric");
    WriteString(out, t.metric);
    out << ',';
    WriteKey(out, "target");
    out << t.target;
    out << ',';
    WriteKey(out, "actual");
    out << t.actual;
    out << ',';
    WriteKey(out, "evaluated");
    out << (t.evaluated ? "true" : "false");
    out << ',';
    WriteKey(out, "pass");
    out << (t.pass ? "true" : "false");
    out << '}';
    first = false;
  }
  out << ']';
  out << ',';
  WriteKey(out, "targets_evaluated");
  out << (report.targets_not_evaluated.empty() ? "true" : "false");
  out << ',';
  if (!report.targets_not_evaluated.empty()) {
    WriteKey(out, "targets_note");
    WriteString(out, report.targets_not_evaluated);
    out << ',';
  }
  if (!report.sweep.empty()) {
    WriteKey(out, "sweep");
    WriteSweep(out, report.sweep);
    out << ',';
    WriteKey(out, "sweep_result_ops");
    if (report.sweep_result_ops.has_value()) {
      out << *report.sweep_result_ops;
    } else {
      out << "null";
    }
    out << ',';
  }
  WriteKey(out, "pass");
  out << (report.pass ? "true" : "false");
  out << '}';
}

}  // namespace abyss::perf
