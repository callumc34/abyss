#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/cold/ttl_scanner.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/result.h"
#include "abyss/log/log.h"

namespace abyss::config {

struct EvictionOverride {
  std::string prefix;
  std::chrono::seconds eviction;
};

struct HotConfig {
  std::string backend = "builtin_hashmap";
  size_t max_memory_bytes = 4294967296;
  std::chrono::seconds default_eviction{86400};
  std::chrono::milliseconds eviction_tick{1000};
  // Queue and consumer pools are opened with the same count.
  uint32_t shard_count = 64;
  std::vector<EvictionOverride> eviction_overrides;
};

struct ColdConfig {
  std::string backend = "builtin_rocksdb";
  std::string data_path = "/data/cold";
  size_t write_buffer_size_bytes = 67108864;
  cold::TtlScanner::Config ttl_scanner{};
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

struct HotConsumerConfig {
  size_t read_batch_size = 256;
  std::chrono::milliseconds read_timeout{100};
};

struct ColdConsumerConfig {
  std::chrono::seconds quiet_threshold{30};
  std::chrono::seconds safety_margin{300};
  double jitter_fraction = 0.1;
  size_t buffer_high_water_bytes = 536870912;
  size_t buffer_low_water_bytes = 0;  // 0 = auto (3/4 of high_water).
  size_t max_flush_batch_size = 10000;
  size_t queue_read_max_count = 1024;
  std::chrono::milliseconds queue_read_timeout{50};
  std::chrono::milliseconds retry_initial_backoff{50};
  std::chrono::milliseconds retry_max_backoff{30000};
};

struct RecoveryConfig {
  // Concurrent shard-replay tasks scheduled at startup. Capped operationally
  // to keep thread count below shard count on small pods. Per-shard threads
  // resume normal independent operation after recovery completes.
  uint32_t replay_parallelism = 4;

  // Bigger batches than steady-state amortise queue Read syscalls during a
  // long catch-up backlog. Steady-state batch sizes (hot_consumer.read_batch_size,
  // cold_consumer.queue_read_max_count) are tuned for low-latency tailing,
  // not bulk drain.
  size_t hot_replay_batch_size = 10000;
  size_t cold_replay_batch_size = 50000;

  // Internal default — not exposed in YAML. The resolver scan is CPU-bound
  // cache updates on the common path; tuning won't materially move recovery
  // latency.
  size_t resolver_replay_batch_size = 5000;
};

struct NetConfig {
  std::string bind = "0.0.0.0";
  uint16_t port = 6379;
  uint32_t max_connections = 1024;
  std::chrono::seconds idle_timeout{300};
  // 0 = auto: min(hardware_concurrency, 16). All threads run identical
  // reactor loops; reactor 0 also owns the listening fd.
  uint32_t io_threads = 0;
  uint32_t accept_queue = 128;
  size_t max_read_buffer_bytes = 67108864;
  size_t write_backpressure_bytes = 4194304;
  size_t write_resume_bytes = 1048576;
  size_t write_hard_limit_bytes = 16777216;
  std::chrono::seconds shutdown_grace{30};
  std::chrono::milliseconds reaper_tick{1000};
};

using ConsumerRpcConfig = core::ConsumerRpcConfig;

struct EngineConfig {
  std::chrono::milliseconds write_timeout{5000};

  // Lower bound on the consumer-apply budget after the durable wait completes.
  // Prevents a slow fsync from starving the RPC wait to ~0ms. The total write
  // latency is therefore bounded by `write_timeout * (1 + min_rpc_wait_fraction)`.
  double min_rpc_wait_fraction = 0.5;
};

struct MetricsConfig {
  bool enabled = true;
  std::string bind = "0.0.0.0";
  uint16_t port = 9090;
};

struct ComponentLevel {
  std::string component;
  log::Level level;
};

struct LogConfig {
#ifdef NDEBUG
  log::Level default_level = log::Level::kInfo;
#else
  log::Level default_level = log::Level::kDebug;
#endif
  std::string format = "json";
  std::string sink = "stdout";
  std::vector<ComponentLevel> component_levels;
};

struct AdminConfig {
  bool enabled = true;
  std::string bind = "0.0.0.0";
  uint16_t port = 8080;
};

struct Config {
  std::string profile = "embedded";
  HotConfig hot;
  ColdConfig cold;
  QueueConfig queue;
  HotConsumerConfig hot_consumer;
  ColdConsumerConfig cold_consumer;
  ConsumerRpcConfig consumer_rpc;
  EngineConfig engine;
  RecoveryConfig recovery;
  NetConfig net;
  MetricsConfig metrics;
  AdminConfig admin;
  LogConfig log;

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
