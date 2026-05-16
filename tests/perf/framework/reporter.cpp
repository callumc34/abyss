#include "reporter.h"

#include <sys/utsname.h>
#include <unistd.h>

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

namespace abyss::perf {

namespace {

constexpr int64_t kNsPerUs = 1000;

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
  report.pass = overall && !report.targets.empty();
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
    WriteKey(out, "pass");
    out << (t.pass ? "true" : "false");
    out << '}';
    first = false;
  }
  out << ']';
  out << ',';
  WriteKey(out, "pass");
  out << (report.pass ? "true" : "false");
  out << '}';
}

}  // namespace abyss::perf
