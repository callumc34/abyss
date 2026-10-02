#pragma once

#include <cstdint>
#include <string_view>

namespace abyss::resp {

struct ServerStats {
  uint64_t hot_key_count = 0;
  uint64_t cold_key_count = 0;
  uint64_t hot_memory_bytes = 0;
  uint64_t queue_total_entries = 0;
  uint64_t queue_total_bytes = 0;
  uint64_t queue_head_seq = 0;
  uint64_t queue_first_seq = 0;
  uint64_t connected_clients = 0;
  uint64_t process_id = 0;
  uint32_t uptime_seconds = 0;
  // Owned-shard count, so the cluster handlers can resolve slot ranges by
  // owning shard (ADP-014). 0 falls back to a single full-range entry.
  uint32_t shard_count = 0;
  uint16_t tcp_port = 0;
  std::string_view version;
  std::string_view bind_address;

  // Falls back to bind_address when empty. Set this when bind is wildcard.
  std::string_view advertise_address;

  std::string_view mode;  // "standalone" or "cluster"
  std::string_view role;  // "master"
};

// Implementations must be thread-safe.
class ServerStatsProvider {
 public:
  ServerStatsProvider() = default;
  virtual ~ServerStatsProvider() = default;
  ServerStatsProvider(const ServerStatsProvider&) = delete;
  ServerStatsProvider& operator=(const ServerStatsProvider&) = delete;
  ServerStatsProvider(ServerStatsProvider&&) = delete;
  ServerStatsProvider& operator=(ServerStatsProvider&&) = delete;

  virtual ServerStats Snapshot() const = 0;
};

}  // namespace abyss::resp
