#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/result.h"

namespace abyss::config {

struct EvictionOverride {
  std::string prefix;
  std::chrono::seconds eviction;
};

struct HotConfig {
  std::string backend = "builtin_hashmap";
  size_t max_memory_bytes = 4294967296;
  std::chrono::seconds default_eviction{86400};
  std::vector<EvictionOverride> eviction_overrides;
};

struct ColdConfig {
  std::string backend = "builtin_rocksdb";
  std::string data_path = "/data/cold";
  size_t write_buffer_size_bytes = 67108864;
};

struct QueueConfig {
  std::string backend = "builtin_wal";
  std::string wal_path = "/data/wal";
  size_t segment_size_bytes = 67108864;
  std::chrono::seconds min_retention{86400};
  std::string fsync_policy = "group_commit";
  uint32_t group_commit_interval_us = 1000;
  size_t group_commit_max_bytes = 1048576;
};

struct ColdConsumerConfig {
  std::chrono::seconds quiet_threshold{30};
  std::chrono::seconds safety_margin{300};
  double deadline_jitter_ratio = 0.5;
  size_t buffer_high_water_bytes = 536870912;
  size_t max_flush_batch_size = 10000;
};

struct RecoveryConfig {
  uint32_t replay_parallelism = 4;
  size_t hot_replay_batch_size = 10000;
  size_t cold_replay_batch_size = 50000;
};

struct RespConfig {
  std::string bind = "0.0.0.0";
  uint16_t port = 6379;
  uint32_t max_connections = 1024;
  std::chrono::seconds idle_timeout{300};
};

using ConsumerRpcConfig = core::ConsumerRpcConfig;

struct EngineConfig {
  std::chrono::milliseconds write_timeout{5000};
};

struct MetricsConfig {
  std::string bind = "0.0.0.0";
  uint16_t port = 9090;
};

struct AdminConfig {
  std::string bind = "0.0.0.0";
  uint16_t port = 8080;
};

struct Config {
  std::string profile = "embedded";
  HotConfig hot;
  ColdConfig cold;
  QueueConfig queue;
  ColdConsumerConfig cold_consumer;
  ConsumerRpcConfig consumer_rpc;
  EngineConfig engine;
  RecoveryConfig recovery;
  RespConfig resp;
  MetricsConfig metrics;
  AdminConfig admin;

  // Load config from a YAML file.
  static core::Result<Config> LoadFromFile(const std::filesystem::path& path);

  // Parse config from an in-memory YAML document.
  static core::Result<Config> ParseFromYaml(std::string_view yaml_text);

  // Hard-coded defaults. Equivalent to a default-constructed Config.
  static Config Defaults();

  // Apply overrides from environment variables. Currently ABYSS_PROFILE.
  // Called automatically by LoadFromFile; exposed for callers that need to
  // apply env overlays to a hand-built Config.
  void ApplyEnvironmentOverrides();

  // Validate the current config.
  core::Result<void> Validate() const;
};

}  // namespace abyss::config
