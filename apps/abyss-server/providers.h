#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "abyss/admin/status_provider.h"
#include "abyss/config/config.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/engine/recovery_coordinator.h"
#include "abyss/resp/config_provider.h"
#include "abyss/resp/loading_state.h"
#include "abyss/resp/node_identity.h"
#include "abyss/resp/server_stats.h"

namespace abyss::server {

// String fields backing ServerStats string_views are owned here and must
// outlive every pipeline that reads them.
class ServerStatsImpl : public resp::ServerStatsProvider {
 public:
  using ConnectionCountFn = std::function<size_t()>;

  ServerStatsImpl(core::Queue& queue, core::HotStore& hot, core::ColdStore* cold,
                  std::string version, std::string bind_address, std::string advertise_address,
                  std::string mode, uint16_t tcp_port, uint32_t shard_count);

  void SetTcpPort(uint16_t port) noexcept;

  // Read-through to the live reactor connection count; no push-side counter to drift.
  void set_connection_count_provider(ConnectionCountFn fn);

  resp::ServerStats Snapshot() const override;

 private:
  core::Queue& queue_;
  core::HotStore& hot_;
  core::ColdStore* cold_;
  std::string version_;
  std::string bind_address_;
  std::string advertise_address_;
  std::string mode_;
  std::atomic<uint16_t> tcp_port_;
  uint32_t shard_count_;
  ConnectionCountFn connection_count_;
  std::chrono::steady_clock::time_point started_at_;
  uint64_t process_id_;
};

class ConfigProviderImpl : public resp::ConfigProvider {
 public:
  explicit ConfigProviderImpl(const config::Config& cfg);
  std::vector<Entry> Entries() const override { return entries_; }

 private:
  std::vector<Entry> entries_;
  std::vector<std::string> values_;  // Stable backing for Entry.second views.
};

// Predicate indirection so this stays decoupled from the queue backend.
class LoadingStateImpl : public resp::LoadingStateProvider {
 public:
  using Predicate = std::function<bool()>;
  explicit LoadingStateImpl(Predicate predicate) : predicate_(std::move(predicate)) {}
  bool IsLoading() const override { return predicate_(); }

 private:
  Predicate predicate_;
};

// Aggregates a /status snapshot from the live data-plane components. All
// pointers are non-owning and must outlive this provider; functors are
// captured by value and may be empty (treated as "not available").
class StatusProviderImpl : public admin::StatusProvider {
 public:
  using BoolFn = std::function<bool()>;
  using SizeFn = std::function<size_t()>;
  using PortFn = std::function<uint16_t()>;
  using CountFn = std::function<uint64_t()>;

  struct Deps {
    const config::Config* config = nullptr;
    core::Queue* queue = nullptr;
    core::HotStore* hot_store = nullptr;
    core::ColdStore* cold_store = nullptr;
    consumer::ColdConsumerPool* cold_pool = nullptr;
    const engine::RecoveryCoordinator* recovery_coordinator = nullptr;
    const resp::NodeIdentity* node_identity = nullptr;
    BoolFn ready;
    BoolFn loading;
    BoolFn shutting_down;
    SizeFn connection_count;
    // Segment retention health. Functors rather than a WalQueue* because these
    // are embedded-WAL concepts with no meaning for an external broker.
    CountFn reaper_failures;
    CountFn oldest_eligible_unreaped_age_ms;
    CountFn unflushed_bytes;
    CountFn durability_lag_ms;
    CountFn read_buffer_high_water_bytes;
    PortFn resp_port;
    PortFn admin_port;
    PortFn metrics_port;
  };

  explicit StatusProviderImpl(Deps deps);

  admin::StatusSnapshot Snapshot() const override;

 private:
  Deps deps_;
  std::chrono::system_clock::time_point started_at_;
  uint64_t process_id_ = 0;
};

}  // namespace abyss::server
