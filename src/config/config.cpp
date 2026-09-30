#include "abyss/config/config.h"

#include <yaml-cpp/yaml.h>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include "abyss/core/result.h"
#include "abyss/log/log.h"
#include "validator.h"
#include "yaml_decode.h"

namespace abyss::config {

namespace {

using internal::SectionDecoder;
using internal::YamlCursor;

core::Result<void> ParseEvictionOverride(const YamlCursor& cur, EvictionOverride& out) {
  return SectionDecoder(cur)
      .Required("prefix", out.prefix)
      .Required("eviction_seconds", out.eviction)
      .Finish();
}

core::Result<void> ParseHot(const YamlCursor& cur, HotConfig& out) {
  return SectionDecoder(cur)
      .Optional("backend", out.backend)
      .Optional("max_memory_bytes", out.max_memory_bytes)
      .Optional("default_eviction_seconds", out.default_eviction)
      .Optional("eviction_tick_ms", out.eviction_tick)
      .Optional("shard_count", out.shard_count)
      .OptionalSequence("eviction_overrides", out.eviction_overrides, ParseEvictionOverride)
      .Finish();
}

core::Result<void> ParseColdTtlScanner(const YamlCursor& cur, cold::TtlScanner::Config& out) {
  return SectionDecoder(cur)
      .Optional("enabled", out.enabled)
      .Optional("base_sample_size", out.base_sample_size)
      .Optional("min_sample_size", out.min_sample_size)
      .Optional("max_sample_size", out.max_sample_size)
      .Optional("base_interval_ms", out.base_interval)
      .Optional("min_interval_ms", out.min_interval)
      .Optional("max_interval_ms", out.max_interval)
      .Optional("high_threshold", out.high_threshold)
      .Optional("low_threshold", out.low_threshold)
      .Optional("rate_increase_factor", out.rate_increase_factor)
      .Optional("rate_decrease_factor", out.rate_decrease_factor)
      .Optional("disk_pressure_threshold", out.disk_pressure_threshold)
      .Optional("disk_pressure_release_threshold", out.disk_pressure_release_threshold)
      .Optional("max_cpu_fraction", out.max_cpu_fraction)
      .Optional("cpu_ewma_window_seconds", out.cpu_ewma_window)
      .Finish();
}

core::Result<void> ParseCold(const YamlCursor& cur, ColdConfig& out) {
  return SectionDecoder(cur)
      .Optional("backend", out.backend)
      .Optional("data_path", out.data_path)
      .Optional("write_buffer_size_bytes", out.write_buffer_size_bytes)
      .OptionalSection("ttl_scanner", out.ttl_scanner, ParseColdTtlScanner)
      .Finish();
}

// YAML uses wal_fsync_policy (the name operators see in configs); the struct
// field stays fsync_policy to match queue::FsyncPolicyFromString.
core::Result<void> ParseQueue(const YamlCursor& cur, QueueConfig& out) {
  return SectionDecoder(cur)
      .Optional("backend", out.backend)
      .Optional("wal_path", out.wal_path)
      .Optional("segment_size_bytes", out.segment_size_bytes)
      .Optional("max_value_size_bytes", out.max_value_size_bytes)
      .Optional("min_retention_seconds", out.min_retention)
      .Optional("wal_fsync_policy", out.fsync_policy)
      .Optional("group_commit_interval_us", out.group_commit_interval_us)
      .Optional("group_commit_max_bytes", out.group_commit_max_bytes)
      .Finish();
}

core::Result<void> ParseHotConsumer(const YamlCursor& cur, HotConsumerConfig& out) {
  return SectionDecoder(cur)
      .Optional("read_batch_size", out.read_batch_size)
      .Optional("read_timeout_ms", out.read_timeout)
      .Finish();
}

core::Result<void> ParseColdConsumer(const YamlCursor& cur, ColdConsumerConfig& out) {
  return SectionDecoder(cur)
      .Optional("quiet_threshold_seconds", out.quiet_threshold)
      .Optional("safety_margin_seconds", out.safety_margin)
      .Optional("jitter_fraction", out.jitter_fraction)
      .Optional("buffer_high_water_bytes", out.buffer_high_water_bytes)
      .Optional("buffer_low_water_bytes", out.buffer_low_water_bytes)
      .Optional("max_flush_batch_size", out.max_flush_batch_size)
      .Optional("queue_read_max_count", out.queue_read_max_count)
      .Optional("queue_read_timeout_ms", out.queue_read_timeout)
      .Optional("retry_initial_backoff_ms", out.retry_initial_backoff)
      .Optional("retry_max_backoff_ms", out.retry_max_backoff)
      .Optional("checkpoint_max_flushes", out.checkpoint_max_flushes)
      .Optional("checkpoint_min_interval_ms", out.checkpoint_min_interval)
      .Optional("loop_initial_backoff_ms", out.loop_initial_backoff)
      .Optional("loop_max_backoff_ms", out.loop_max_backoff)
      .Optional("drain_grace_seconds", out.drain_grace)
      .Finish();
}

core::Result<void> ParseRecovery(const YamlCursor& cur, RecoveryConfig& out) {
  return SectionDecoder(cur)
      .Optional("replay_parallelism", out.replay_parallelism)
      .Optional("hot_replay_batch_size", out.hot_replay_batch_size)
      .Optional("cold_replay_batch_size", out.cold_replay_batch_size)
      .Finish();
}

core::Result<void> ParseConsumerRpc(const YamlCursor& cur, ConsumerRpcConfig& out) {
  return SectionDecoder(cur)
      .Optional("registry_shard_count", out.registry_shard_count)
      .Optional("default_timeout_ms", out.default_timeout)
      .Finish();
}

core::Result<void> ParseEngine(const YamlCursor& cur, EngineConfig& out) {
  return SectionDecoder(cur)
      .Optional("write_timeout_ms", out.write_timeout)
      .Optional("min_rpc_wait_fraction", out.min_rpc_wait_fraction)
      .Optional("buffer_consistency_wait_timeout_ms", out.buffer_consistency_wait_timeout)
      .Finish();
}

core::Result<void> ParseNet(const YamlCursor& cur, NetConfig& out) {
  return SectionDecoder(cur)
      .Optional("bind", out.bind)
      .Optional("port", out.port)
      .Optional("max_connections", out.max_connections)
      .Optional("idle_timeout_seconds", out.idle_timeout)
      .Optional("io_threads", out.io_threads)
      .Optional("accept_queue", out.accept_queue)
      .Optional("max_read_buffer_bytes", out.max_read_buffer_bytes)
      .Optional("write_backpressure_bytes", out.write_backpressure_bytes)
      .Optional("write_resume_bytes", out.write_resume_bytes)
      .Optional("write_hard_limit_bytes", out.write_hard_limit_bytes)
      .Optional("shutdown_grace_seconds", out.shutdown_grace)
      .Optional("reaper_tick_ms", out.reaper_tick)
      .Finish();
}

core::Result<void> ParseMetrics(const YamlCursor& cur, MetricsConfig& out) {
  return SectionDecoder(cur)
      .Optional("enabled", out.enabled)
      .Optional("bind", out.bind)
      .Optional("port", out.port)
      .Optional("snapshot_interval_ms", out.snapshot_interval)
      .Finish();
}

core::Result<void> ParseAdmin(const YamlCursor& cur, AdminConfig& out) {
  return SectionDecoder(cur)
      .Optional("enabled", out.enabled)
      .Optional("bind", out.bind)
      .Optional("port", out.port)
      .Finish();
}

core::Result<log::Level> DecodeLogLevel(const YamlCursor& cur) {
  auto text = internal::DecodeString(cur);
  if (!text.has_value()) return std::unexpected(text.error());
  log::Level level = log::Level::kInfo;
  if (!log::ParseLevel(*text, level)) {
    return std::unexpected(cur.MakeError("unknown log level"));
  }
  return level;
}

core::Result<void> ParseComponentLevel(const YamlCursor& cur, ComponentLevel& out) {
  if (auto r = cur.RequireMap(); !r) return r;
  if (auto r = cur.RejectUnknownKeys({"component", "level"}); !r) return r;

  auto comp_cur = cur.Child("component");
  if (!comp_cur.node().IsDefined() || comp_cur.node().IsNull()) {
    return std::unexpected(comp_cur.MakeError("required field is missing"));
  }
  auto comp_str = internal::DecodeString(comp_cur);
  if (!comp_str.has_value()) return std::unexpected(comp_str.error());
  out.component = std::move(*comp_str);

  auto level_cur = cur.Child("level");
  if (!level_cur.node().IsDefined() || level_cur.node().IsNull()) {
    return std::unexpected(level_cur.MakeError("required field is missing"));
  }
  auto level = DecodeLogLevel(level_cur);
  if (!level.has_value()) return std::unexpected(level.error());
  out.level = *level;
  return {};
}

core::Result<void> ParseLog(const YamlCursor& cur, LogConfig& out) {
  if (auto r = cur.RequireMap(); !r) return r;
  if (auto r = cur.RejectUnknownKeys({"level", "format", "sink", "component_levels"}); !r) {
    return r;
  }

  if (auto n = cur.Child("level"); n.node().IsDefined() && !n.node().IsNull()) {
    auto level = DecodeLogLevel(n);
    if (!level.has_value()) return std::unexpected(level.error());
    out.default_level = *level;
  }
  if (auto n = cur.Child("format"); n.node().IsDefined() && !n.node().IsNull()) {
    auto s = internal::DecodeString(n);
    if (!s.has_value()) return std::unexpected(s.error());
    out.format = std::move(*s);
  }
  if (auto n = cur.Child("sink"); n.node().IsDefined() && !n.node().IsNull()) {
    auto s = internal::DecodeString(n);
    if (!s.has_value()) return std::unexpected(s.error());
    out.sink = std::move(*s);
  }
  if (auto n = cur.Child("component_levels"); n.node().IsDefined() && !n.node().IsNull()) {
    if (!n.node().IsSequence()) {
      return std::unexpected(n.MakeError("expected a sequence"));
    }
    out.component_levels.clear();
    for (size_t i = 0; i < n.node().size(); ++i) {
      ComponentLevel entry;
      if (auto r = ParseComponentLevel(n.Index(i), entry); !r) return r;
      out.component_levels.push_back(std::move(entry));
    }
  }
  return {};
}

}  // namespace

Config Config::Defaults() { return {}; }

core::Result<Config> Config::ParseFromYaml(std::string_view yaml_text) {
  YAML::Node root;
  try {
    root = YAML::Load(std::string(yaml_text));
  } catch (const YAML::Exception& e) {
    std::string msg = "YAML parse error: ";
    msg += e.what();
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, std::move(msg)});
  }

  Config config = Defaults();

  if (!root.IsDefined() || root.IsNull()) {
    if (auto r = config.ApplyEnvironmentOverrides(); !r) return std::unexpected(r.error());
    if (auto r = config.Validate(); !r) return std::unexpected(r.error());
    return config;
  }

  const YamlCursor root_cur(root);
  if (auto r = root_cur.RequireMap(); !r) return std::unexpected(r.error());
  if (auto r = root_cur.RejectUnknownKeys({"profile", "hot", "cold", "queue", "hot_consumer",
                                           "cold_consumer", "consumer_rpc", "engine", "recovery",
                                           "net", "metrics", "admin", "log"});
      !r) {
    return std::unexpected(r.error());
  }

  if (auto profile_node = root_cur.Child("profile");
      profile_node.node().IsDefined() && !profile_node.node().IsNull()) {
    auto decoded = internal::DecodeString(profile_node);
    if (!decoded.has_value()) return std::unexpected(decoded.error());
    config.profile = std::move(*decoded);
  }

  struct Section {
    std::string_view name;
    core::Result<void> (*parse)(const YamlCursor&, Config&);
  };

  const std::array<Section, 12> sections = {{
      {"hot", [](const YamlCursor& c, Config& cfg) { return ParseHot(c, cfg.hot); }},
      {"cold", [](const YamlCursor& c, Config& cfg) { return ParseCold(c, cfg.cold); }},
      {"queue", [](const YamlCursor& c, Config& cfg) { return ParseQueue(c, cfg.queue); }},
      {"hot_consumer",
       [](const YamlCursor& c, Config& cfg) { return ParseHotConsumer(c, cfg.hot_consumer); }},
      {"cold_consumer",
       [](const YamlCursor& c, Config& cfg) { return ParseColdConsumer(c, cfg.cold_consumer); }},
      {"consumer_rpc",
       [](const YamlCursor& c, Config& cfg) { return ParseConsumerRpc(c, cfg.consumer_rpc); }},
      {"engine", [](const YamlCursor& c, Config& cfg) { return ParseEngine(c, cfg.engine); }},
      {"recovery", [](const YamlCursor& c, Config& cfg) { return ParseRecovery(c, cfg.recovery); }},
      {"net", [](const YamlCursor& c, Config& cfg) { return ParseNet(c, cfg.net); }},
      {"metrics", [](const YamlCursor& c, Config& cfg) { return ParseMetrics(c, cfg.metrics); }},
      {"admin", [](const YamlCursor& c, Config& cfg) { return ParseAdmin(c, cfg.admin); }},
      {"log", [](const YamlCursor& c, Config& cfg) { return ParseLog(c, cfg.log); }},
  }};

  for (const auto& section : sections) {
    auto child = root_cur.Child(section.name);
    if (!child.node().IsDefined() || child.node().IsNull()) continue;
    if (auto r = section.parse(child, config); !r) return std::unexpected(r.error());
  }

  if (auto r = config.ApplyEnvironmentOverrides(); !r) return std::unexpected(r.error());
  if (auto r = config.Validate(); !r) return std::unexpected(r.error());
  return config;
}

core::Result<Config> Config::LoadFromFile(const std::filesystem::path& path) {
  const std::ifstream stream(path);
  if (!stream) {
    std::string msg = "failed to open config file '";
    msg += path.string();
    msg += "': ";
    msg += std::strerror(errno);
    return std::unexpected(core::Error{core::ErrorCode::kNotFound, std::move(msg)});
  }
  std::stringstream buffer;
  buffer << stream.rdbuf();
  if (stream.bad()) {
    std::string msg = "failed to read config file '";
    msg += path.string();
    msg += "'";
    return std::unexpected(core::Error{core::ErrorCode::kInternal, std::move(msg)});
  }
  return ParseFromYaml(buffer.str());
}

namespace {

const char* GetEnv(const char* name) {
  // NOLINTNEXTLINE(concurrency-mt-unsafe,cppcoreguidelines-init-variables)
  const char* v = std::getenv(name);
  return (v != nullptr && *v != '\0') ? v : nullptr;
}

// NOLINTNEXTLINE(misc-unused-parameters)
bool ParseEnvBool(const char* text, bool& out) {
  std::string s(text);
  for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (s == "1" || s == "true" || s == "yes" || s == "on") {
    out = true;
    return true;
  }
  if (s == "0" || s == "false" || s == "no" || s == "off") {
    out = false;
    return true;
  }
  return false;
}

// A present env var whose value cannot be parsed is rejected, not dropped: the
// operator's intent must never be replaced by a silent default.
core::Error EnvError(std::string_view name, const char* value, std::string_view reason) {
  std::string msg(name);
  msg += "='";
  msg += value;
  msg += "' ";
  msg += reason;
  return {core::ErrorCode::kInvalidArgument, std::move(msg)};
}

}  // namespace

core::Result<void> Config::ApplyEnvironmentOverrides() {
  if (const char* v = GetEnv("ABYSS_PROFILE")) this->profile = v;

  if (const char* v = GetEnv("ABYSS_LOG_LEVEL")) {
    log::Level parsed = log::Level::kInfo;
    if (!log::ParseLevel(v, parsed)) {
      return std::unexpected(EnvError("ABYSS_LOG_LEVEL", v, "is not a valid log level"));
    }
    this->log.default_level = parsed;
  }
  // format/sink pass through verbatim; the validator's OneOf checks reject any
  // value these do not name, so re-checking here would duplicate that rule.
  if (const char* v = GetEnv("ABYSS_LOG_FORMAT")) this->log.format = v;
  if (const char* v = GetEnv("ABYSS_LOG_SINK")) this->log.sink = v;

  if (const char* v = GetEnv("ABYSS_METRICS_ENABLED")) {
    bool enabled = true;
    if (!ParseEnvBool(v, enabled)) {
      return std::unexpected(EnvError("ABYSS_METRICS_ENABLED", v,
                                      "is not a valid boolean (1/0, true/false, yes/no, on/off)"));
    }
    this->metrics.enabled = enabled;
  }
  if (const char* v = GetEnv("ABYSS_METRICS_BIND")) this->metrics.bind = v;
  if (const char* v = GetEnv("ABYSS_METRICS_PORT")) {
    char* end = nullptr;
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const long port = std::strtol(v, &end, 10);
    if (end == v || *end != '\0' || port <= 0 || port > 0xFFFF) {
      return std::unexpected(EnvError("ABYSS_METRICS_PORT", v, "is not a valid port"));
    }
    this->metrics.port = static_cast<uint16_t>(port);
  }
  return {};
}

core::Result<void> Config::Validate() const { return internal::Validate(*this); }

}  // namespace abyss::config
