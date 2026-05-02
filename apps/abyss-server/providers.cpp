#include "providers.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "abyss/version.h"

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

void ServerStatsImpl::SetTcpPort(uint16_t port) noexcept {
  tcp_port_.store(port, std::memory_order_relaxed);
}

void ServerStatsImpl::set_connection_count_provider(ConnectionCountFn fn) {
  connection_count_ = std::move(fn);
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
  if (auto stats = queue_.Stats(); stats.has_value()) {
    out.queue_total_entries = stats->total_entries;
    out.queue_total_bytes = stats->total_bytes;
    out.queue_head_seq = stats->head_seq;
    out.queue_tail_seq = stats->tail_seq;
  }

  out.connected_clients = connection_count_ ? connection_count_() : 0;
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

StatusProviderImpl::StatusProviderImpl(Deps deps)
    : deps_(std::move(deps)),
      started_at_(std::chrono::system_clock::now()),
      process_id_(CurrentProcessId()) {}

admin::StatusSnapshot StatusProviderImpl::Snapshot() const {
  admin::StatusSnapshot s;

  s.build.version = std::string{kVersion};
  s.build.commit = std::string{kBuildCommit};
  s.build.date = std::string{kBuildDate};

  if (deps_.node_identity != nullptr) {
    s.server.node_id = std::string{deps_.node_identity->Id()};
  }
  s.server.started_at_unix_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(started_at_.time_since_epoch())
          .count());
  const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now() - started_at_);
  s.server.uptime_seconds = static_cast<uint64_t>(std::max<int64_t>(uptime.count(), 0));
  s.server.process_id = process_id_;
  s.server.ready = deps_.ready ? deps_.ready() : false;
  s.server.loading = deps_.loading ? deps_.loading() : false;
  s.server.shutting_down = deps_.shutting_down ? deps_.shutting_down() : false;
  s.server.mode = "standalone";
  s.server.role = "master";

  if (deps_.config != nullptr) {
    s.config.profile = deps_.config->profile;
    s.config.shard_count = deps_.config->hot.shard_count;
    s.config.fsync_policy = deps_.config->queue.fsync_policy;
    s.config.default_eviction_seconds =
        static_cast<uint64_t>(deps_.config->hot.default_eviction.count());

    s.endpoints.resp.bind = deps_.config->net.bind;
    s.endpoints.resp.port = deps_.resp_port ? deps_.resp_port() : deps_.config->net.port;

    s.endpoints.admin.bind = deps_.config->admin.bind;
    s.endpoints.admin.port = deps_.admin_port ? deps_.admin_port() : deps_.config->admin.port;
    s.endpoints.admin.enabled = deps_.config->admin.enabled;
    s.endpoints.admin.emit_enabled = true;

    s.endpoints.metrics.bind = deps_.config->metrics.bind;
    s.endpoints.metrics.port =
        deps_.metrics_port ? deps_.metrics_port() : deps_.config->metrics.port;
    s.endpoints.metrics.enabled = deps_.config->metrics.enabled;
    s.endpoints.metrics.emit_enabled = true;

    s.queue.backend = deps_.config->queue.backend;
    s.hot.backend = deps_.config->hot.backend;
    s.cold.backend = deps_.config->cold.backend;
  }

  if (deps_.queue != nullptr) {
    if (auto qs = deps_.queue->Stats(); qs.has_value()) {
      s.queue.head_seq = qs->head_seq;
      s.queue.tail_seq = qs->tail_seq;
      s.queue.total_entries = qs->total_entries;
      s.queue.total_bytes = qs->total_bytes;
    }
  }

  if (deps_.hot_store != nullptr) {
    if (auto hs = deps_.hot_store->Stats(); hs.has_value()) {
      s.hot.key_count = hs->key_count;
      s.hot.memory_bytes = hs->used_bytes;
    }
  }

  if (deps_.cold_store != nullptr) {
    if (auto cs = deps_.cold_store->Stats(); cs.has_value()) {
      s.cold.key_count = cs->key_count;
    }
  }

  if (deps_.cold_pool != nullptr) {
    auto agg = deps_.cold_pool->Snapshot();
    s.cold.buffer.entries = agg.buffer_entries;
    s.cold.buffer.bytes = agg.buffer_bytes;
  }

  // Aggregate per-shard consumer positions. Min/max captures shard skew
  // without exploding cardinality on the JSON payload.
  auto collect_lag = [this](uint32_t shard_count, auto&& seq_for_shard, uint64_t& min_out,
                            uint64_t& max_out, uint64_t& lag_out) {
    uint64_t min_v = std::numeric_limits<uint64_t>::max();
    uint64_t max_v = 0;
    uint64_t lag_v = 0;
    for (uint32_t shard = 0; shard < shard_count; ++shard) {
      const uint64_t seq = seq_for_shard(shard);
      min_v = std::min(min_v, seq);
      max_v = std::max(max_v, seq);
      if (deps_.queue != nullptr) {
        if (auto t = deps_.queue->TailSeq(shard); t.has_value() && *t > seq) {
          lag_v = std::max(lag_v, *t - seq);
        }
      }
    }
    min_out = shard_count == 0 ? 0 : min_v;
    max_out = max_v;
    lag_out = lag_v;
  };

  if (deps_.hot_pool != nullptr) {
    collect_lag(
        deps_.hot_pool->ShardCount(),
        [this](uint32_t shard) -> uint64_t {
          return deps_.hot_pool->ConsumerFor(shard).HighestSettledSeq();
        },
        s.consumers.hot.highest_settled_seq_min, s.consumers.hot.highest_settled_seq_max,
        s.lag.hot_max_entries);
  }

  if (deps_.cold_pool != nullptr) {
    collect_lag(
        deps_.cold_pool->ShardCount(),
        [this](uint32_t shard) -> uint64_t {
          return deps_.cold_pool->ConsumerFor(shard).Snapshot().last_ack_seq;
        },
        s.consumers.cold.last_ack_seq_min, s.consumers.cold.last_ack_seq_max,
        s.lag.cold_max_entries);
  }

  if (deps_.resolver_pool != nullptr) {
    auto agg = deps_.resolver_pool->Snapshot();
    s.consumers.resolver.cache_entries = agg.cache_entries_total;
    s.consumers.resolver.cache_bytes = agg.cache_bytes_total;

    collect_lag(
        deps_.resolver_pool->ShardCount(),
        [this](uint32_t shard) -> uint64_t {
          return deps_.resolver_pool->ConsumerFor(shard).GetSnapshot().last_ack_seq;
        },
        s.consumers.resolver.last_ack_seq_min, s.consumers.resolver.last_ack_seq_max,
        s.lag.resolver_max_entries);
  }

  s.connections.active = deps_.connection_count ? deps_.connection_count() : 0;

  return s;
}

}  // namespace abyss::server
