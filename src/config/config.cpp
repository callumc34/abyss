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
      .OptionalSequence("eviction_overrides", out.eviction_overrides, ParseEvictionOverride)
      .Finish();
}

core::Result<void> ParseCold(const YamlCursor& cur, ColdConfig& out) {
  return SectionDecoder(cur)
      .Optional("backend", out.backend)
      .Optional("data_path", out.data_path)
      .Optional("write_buffer_size_bytes", out.write_buffer_size_bytes)
      .Finish();
}

// YAML uses wal_fsync_policy (the name operators see in configs); the struct
// field stays fsync_policy to match queue::FsyncPolicyFromString.
core::Result<void> ParseQueue(const YamlCursor& cur, QueueConfig& out) {
  return SectionDecoder(cur)
      .Optional("backend", out.backend)
      .Optional("wal_path", out.wal_path)
      .Optional("segment_size_bytes", out.segment_size_bytes)
      .Optional("min_retention_seconds", out.min_retention)
      .Optional("wal_fsync_policy", out.fsync_policy)
      .Optional("group_commit_interval_us", out.group_commit_interval_us)
      .Optional("group_commit_max_bytes", out.group_commit_max_bytes)
      .Finish();
}

core::Result<void> ParseColdConsumer(const YamlCursor& cur, ColdConsumerConfig& out) {
  return SectionDecoder(cur)
      .Optional("quiet_threshold_seconds", out.quiet_threshold)
      .Optional("safety_margin_seconds", out.safety_margin)
      .Optional("deadline_jitter_ratio", out.deadline_jitter_ratio)
      .Optional("buffer_high_water_bytes", out.buffer_high_water_bytes)
      .Optional("max_flush_batch_size", out.max_flush_batch_size)
      .Finish();
}

core::Result<void> ParseRecovery(const YamlCursor& cur, RecoveryConfig& out) {
  return SectionDecoder(cur)
      .Optional("replay_parallelism", out.replay_parallelism)
      .Optional("hot_replay_batch_size", out.hot_replay_batch_size)
      .Optional("cold_replay_batch_size", out.cold_replay_batch_size)
      .Finish();
}

core::Result<void> ParseResp(const YamlCursor& cur, RespConfig& out) {
  return SectionDecoder(cur)
      .Optional("bind", out.bind)
      .Optional("port", out.port)
      .Optional("max_connections", out.max_connections)
      .Optional("idle_timeout_seconds", out.idle_timeout)
      .Finish();
}

core::Result<void> ParseBindPort(const YamlCursor& cur, std::string& bind, uint16_t& port) {
  return SectionDecoder(cur).Optional("bind", bind).Optional("port", port).Finish();
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
    config.ApplyEnvironmentOverrides();
    if (auto r = config.Validate(); !r) return std::unexpected(r.error());
    return config;
  }

  const YamlCursor root_cur(root);
  if (auto r = root_cur.RequireMap(); !r) return std::unexpected(r.error());
  if (auto r = root_cur.RejectUnknownKeys({"profile", "hot", "cold", "queue", "cold_consumer",
                                           "recovery", "resp", "metrics", "admin"});
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

  const std::array<Section, 8> sections = {{
      {"hot", [](const YamlCursor& c, Config& cfg) { return ParseHot(c, cfg.hot); }},
      {"cold", [](const YamlCursor& c, Config& cfg) { return ParseCold(c, cfg.cold); }},
      {"queue", [](const YamlCursor& c, Config& cfg) { return ParseQueue(c, cfg.queue); }},
      {"cold_consumer",
       [](const YamlCursor& c, Config& cfg) { return ParseColdConsumer(c, cfg.cold_consumer); }},
      {"recovery", [](const YamlCursor& c, Config& cfg) { return ParseRecovery(c, cfg.recovery); }},
      {"resp", [](const YamlCursor& c, Config& cfg) { return ParseResp(c, cfg.resp); }},
      {"metrics", [](const YamlCursor& c,
                     Config& cfg) { return ParseBindPort(c, cfg.metrics.bind, cfg.metrics.port); }},
      {"admin", [](const YamlCursor& c,
                   Config& cfg) { return ParseBindPort(c, cfg.admin.bind, cfg.admin.port); }},
  }};

  for (const auto& section : sections) {
    auto child = root_cur.Child(section.name);
    if (!child.node().IsDefined() || child.node().IsNull()) continue;
    if (auto r = section.parse(child, config); !r) return std::unexpected(r.error());
  }

  config.ApplyEnvironmentOverrides();
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

void Config::ApplyEnvironmentOverrides() {
  if (const char* profile = std::getenv("ABYSS_PROFILE"); profile != nullptr && *profile != '\0') {
    this->profile = profile;
  }
}

core::Result<void> Config::Validate() const { return internal::Validate(*this); }

}  // namespace abyss::config
