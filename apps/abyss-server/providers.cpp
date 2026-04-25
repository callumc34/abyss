#include "providers.h"

#include <chrono>
#include <string>
#include <utility>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace abyss::server {

namespace {

uint64_t CurrentProcessId() noexcept {
#ifdef _WIN32
  return static_cast<uint64_t>(::_getpid());
#else
  return static_cast<uint64_t>(::getpid());
#endif
}

void AppendBoolEntry(std::vector<resp::ConfigProvider::Entry>& entries,
                     std::vector<std::string>& storage, std::string_view key, bool value) {
  storage.emplace_back(value ? "yes" : "no");
  entries.emplace_back(key, storage.back());
}

void AppendStringEntry(std::vector<resp::ConfigProvider::Entry>& entries,
                       std::vector<std::string>& storage, std::string_view key, std::string value) {
  storage.push_back(std::move(value));
  entries.emplace_back(key, storage.back());
}

void AppendNumericEntry(std::vector<resp::ConfigProvider::Entry>& entries,
                        std::vector<std::string>& storage, std::string_view key, uint64_t value) {
  AppendStringEntry(entries, storage, key, std::to_string(value));
}

}  // namespace

ServerStatsImpl::ServerStatsImpl(core::Queue& queue, core::HotStore& hot, core::ColdStore* cold,
                                 std::string version, std::string bind_address,
                                 std::string advertise_address, std::string mode, uint16_t tcp_port)
    : queue_(queue),
      hot_(hot),
      cold_(cold),
      version_(std::move(version)),
      bind_address_(std::move(bind_address)),
      advertise_address_(std::move(advertise_address)),
      mode_(std::move(mode)),
      tcp_port_(tcp_port),
      started_at_(std::chrono::steady_clock::now()),
      process_id_(CurrentProcessId()) {}

void ServerStatsImpl::IncrementConnectedClients() noexcept {
  connected_clients_.fetch_add(1, std::memory_order_relaxed);
}

void ServerStatsImpl::DecrementConnectedClients() noexcept {
  connected_clients_.fetch_sub(1, std::memory_order_relaxed);
}

void ServerStatsImpl::SetTcpPort(uint16_t port) noexcept {
  tcp_port_.store(port, std::memory_order_relaxed);
}

resp::ServerStats ServerStatsImpl::Snapshot() const {
  resp::ServerStats out{};
  if (auto stats = hot_.Stats(); stats.has_value()) {
    out.hot_key_count = stats->key_count;
    out.hot_memory_bytes = stats->used_bytes;
  }
  if (cold_ != nullptr) {
    if (auto stats = cold_->Stats(); stats.has_value()) {
      out.cold_key_count = stats->key_count;
    }
  }
  (void)queue_;  // Reserved for queue depth/age fields.

  out.connected_clients = connected_clients_.load(std::memory_order_relaxed);
  out.process_id = process_id_;
  const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::steady_clock::now() - started_at_)
                          .count();
  out.uptime_seconds = static_cast<uint32_t>(uptime > 0 ? uptime : 0);
  out.tcp_port = tcp_port_.load(std::memory_order_relaxed);
  out.version = version_;
  out.bind_address = bind_address_;
  out.advertise_address = advertise_address_;
  out.mode = mode_;
  out.role = "master";
  return out;
}

ConfigProviderImpl::ConfigProviderImpl(const config::Config& cfg) {
  // Reserve up front: reallocation would invalidate Entry string_views into values_.
  values_.reserve(32);
  entries_.reserve(32);

  AppendStringEntry(entries_, values_, "profile", cfg.profile);
  AppendStringEntry(entries_, values_, "bind", cfg.net.bind);
  AppendNumericEntry(entries_, values_, "port", cfg.net.port);
  AppendNumericEntry(entries_, values_, "maxclients", cfg.net.max_connections);
  AppendNumericEntry(entries_, values_, "maxmemory", cfg.hot.max_memory_bytes);
  AppendNumericEntry(entries_, values_, "shard-count", cfg.hot.shard_count);
  AppendNumericEntry(entries_, values_, "default-eviction-seconds",
                     static_cast<uint64_t>(cfg.hot.default_eviction.count()));
  AppendStringEntry(entries_, values_, "wal-path", cfg.queue.wal_path);
  AppendStringEntry(entries_, values_, "wal-fsync-policy", cfg.queue.fsync_policy);
  AppendNumericEntry(entries_, values_, "wal-segment-size-bytes", cfg.queue.segment_size_bytes);
  AppendStringEntry(entries_, values_, "cold-data-path", cfg.cold.data_path);
  AppendBoolEntry(entries_, values_, "metrics-enabled", cfg.metrics.enabled);
  AppendNumericEntry(entries_, values_, "metrics-port", cfg.metrics.port);
}

}  // namespace abyss::server
