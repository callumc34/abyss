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
#include "abyss/core/durability.h"
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
  // 128 MiB: large enough to hold one max-size value plus framing so a max-size
  // value never needs a variable-size segment (segments stay fixed-size).
  size_t segment_size_bytes = 134217728;
  // Largest single value accepted, decoupled from segment_size_bytes. Default
  // 64 MiB; settable up to Redis's 512 MiB proto-max-bulk-len. The validator
  // requires segment_size_bytes to hold one max-size entry, so larger values
  // are rejected with kValueTooLarge rather than tied to the segment knob.
  size_t max_value_size_bytes = 67108864;
  // Physical logs on the data volume, a power of two <=
  // hot.shard_count; shard s writes to log s % log_count.
  uint32_t log_count = 1;
  // Per-shard offset ring slots, a power of two in [2^12, 2^24]. Reads
  // further back than the ring locate frames through the sparse index.
  uint32_t ring_entries = 65536;
  std::chrono::seconds min_retention{86400};
  // Cadence of the committed-offset checkpoint. A crash replays at most this
  // much past the last checkpoint; WAL reclamation trails commits by it.
  std::chrono::milliseconds offset_fsync_interval{1000};
  // What an acknowledged write survives.
  core::Durability durability = core::Durability::kProcessCrash;
  // Bounds on written but not yet power-durable data: bytes across
  // logs, and the oldest entry's age per log. Appends wait, then fail,
  // past it.
  uint64_t durability_window_bytes = 67108864;
  std::chrono::milliseconds durability_window{1000};
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
  // Bounded checkpoint cadence: the cold store is fsynced at most once every
  // `checkpoint_max_flushes` applied batches or `checkpoint_min_interval`,
  // whichever comes first. Lowering either tightens the durable frontier at the
  // cost of more fsyncs; raising either widens the WAL replay window on crash.
  size_t checkpoint_max_flushes = 32;
  std::chrono::milliseconds checkpoint_min_interval{50};
  // Capped exponential backoff applied when a drain/flush iteration makes no
  // progress (idle, poisoned, or unwritable). Reset on progress.
  std::chrono::milliseconds loop_initial_backoff{1};
  std::chrono::milliseconds loop_max_backoff{1000};
  // Per-shard budget for the graceful SIGTERM drain: each cold consumer
  // flushes + checkpoints its buffer to durable storage before stopping,
  // bounded by this deadline (drains run in parallel across shards). On expiry
  // the remaining slice replays from the WAL. Keep below the K8s
  // terminationGracePeriodSeconds minus the /ready-flip propagation window.
  std::chrono::seconds drain_grace{15};
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

  // Bounds how long DispatchHashRead waits for the cold consumer to catch up
  // to hot's settled seq before snapshotting the buffer overlay.
  std::chrono::milliseconds buffer_consistency_wait_timeout{100};
};

struct MetricsConfig {
  bool enabled = true;
  std::string bind = "0.0.0.0";
  uint16_t port = 9090;
  // Cadence for pushing snapshot-style gauges. Deliberately decoupled from the
  // server's much faster stop-poll: each tick reads store statistics, including
  // RocksDB property lookups, which are cheap but not free.
  std::chrono::milliseconds snapshot_interval{1000};
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

  // Apply overrides from ABYSS_* environment variables. Called automatically by
  // LoadFromFile/ParseFromYaml; exposed for callers that need to apply env
  // overlays to a hand-built Config. A variable that is present but whose value
  // cannot be parsed is an error naming the variable — an override is never
  // silently discarded. An absent variable is not an error.
  [[nodiscard]] core::Result<void> ApplyEnvironmentOverrides();

  // Validate the current config.
  core::Result<void> Validate() const;
};

}  // namespace abyss::config
