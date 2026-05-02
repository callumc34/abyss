#pragma once

#include <cstdint>
#include <string>

namespace abyss::admin {

struct StatusBuildInfo {
  std::string version;
  std::string commit;
  std::string date;
};

struct StatusServerInfo {
  std::string node_id;
  uint64_t started_at_unix_ms = 0;
  uint64_t uptime_seconds = 0;
  uint64_t process_id = 0;
  bool ready = false;
  bool loading = false;
  bool shutting_down = false;
  std::string mode;
  std::string role;
};

struct StatusConfigInfo {
  std::string profile;
  uint32_t shard_count = 0;
  std::string fsync_policy;
  uint64_t default_eviction_seconds = 0;
};

struct StatusEndpoint {
  std::string bind;
  uint16_t port = 0;
  std::string advertise_address;
  bool enabled = true;
  bool emit_enabled = false;
  bool emit_advertise = false;
};

struct StatusEndpointsInfo {
  StatusEndpoint resp;
  StatusEndpoint admin;
  StatusEndpoint metrics;
};

struct StatusQueueInfo {
  std::string backend;
  uint64_t head_seq = 0;
  uint64_t tail_seq = 0;
  uint64_t total_entries = 0;
  uint64_t total_bytes = 0;
};

struct StatusHotInfo {
  std::string backend;
  uint64_t key_count = 0;
  uint64_t memory_bytes = 0;
};

struct StatusColdBufferInfo {
  uint64_t entries = 0;
  uint64_t bytes = 0;
};

struct StatusColdInfo {
  std::string backend;
  uint64_t key_count = 0;
  StatusColdBufferInfo buffer;
};

struct StatusHotConsumerInfo {
  uint64_t highest_settled_seq_min = 0;
  uint64_t highest_settled_seq_max = 0;
};

struct StatusColdConsumerInfo {
  uint64_t last_ack_seq_min = 0;
  uint64_t last_ack_seq_max = 0;
};

struct StatusResolverInfo {
  uint64_t last_ack_seq_min = 0;
  uint64_t last_ack_seq_max = 0;
  uint64_t cache_entries = 0;
  uint64_t cache_bytes = 0;
};

struct StatusConsumersInfo {
  StatusHotConsumerInfo hot;
  StatusColdConsumerInfo cold;
  StatusResolverInfo resolver;
};

struct StatusLagInfo {
  uint64_t hot_max_entries = 0;
  uint64_t cold_max_entries = 0;
  uint64_t resolver_max_entries = 0;
};

struct StatusConnectionsInfo {
  uint64_t active = 0;
};

// Foundational schema. Stability rules in docs/operations/observability.md:
// fields are append-only, never renamed, never typed-changed without a
// schema_version bump. Unset/inapplicable fields render as JSON null or as
// the type's neutral value (0, "", false), never as missing keys.
struct StatusSnapshot {
  uint32_t schema_version = 1;
  StatusBuildInfo build;
  StatusServerInfo server;
  StatusConfigInfo config;
  StatusEndpointsInfo endpoints;
  StatusQueueInfo queue;
  StatusHotInfo hot;
  StatusColdInfo cold;
  StatusConsumersInfo consumers;
  StatusLagInfo lag;
  StatusConnectionsInfo connections;
};

// Implementations must be thread-safe. The handler may invoke Snapshot() from
// any thread under HTTP request load.
class StatusProvider {
 public:
  StatusProvider() = default;
  virtual ~StatusProvider() = default;
  StatusProvider(const StatusProvider&) = delete;
  StatusProvider& operator=(const StatusProvider&) = delete;
  StatusProvider(StatusProvider&&) = delete;
  StatusProvider& operator=(StatusProvider&&) = delete;

  virtual StatusSnapshot Snapshot() const = 0;
};

}  // namespace abyss::admin
