#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "abyss/config/config.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/resp/config_provider.h"
#include "abyss/resp/loading_state.h"
#include "abyss/resp/server_stats.h"

namespace abyss::server {

// String fields backing ServerStats string_views are owned here and must
// outlive every pipeline that reads them.
class ServerStatsImpl : public resp::ServerStatsProvider {
 public:
  ServerStatsImpl(core::Queue& queue, core::HotStore& hot, core::ColdStore* cold,
                  std::string version, std::string bind_address, std::string advertise_address,
                  std::string mode, uint16_t tcp_port);

  void IncrementConnectedClients() noexcept;
  void DecrementConnectedClients() noexcept;
  void SetTcpPort(uint16_t port) noexcept;

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
  std::atomic<uint64_t> connected_clients_{0};
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

}  // namespace abyss::server
