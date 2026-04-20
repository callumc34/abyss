#include "server.h"

#ifdef _WIN32
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/resp/request_pipeline.h"
#include "abyss/version.h"

namespace abyss::server {

namespace {

const log::Logger& ServerLog() {
  static const log::Logger l = log::Get("abyss.server");
  return l;
}
const log::Logger& ListenerLog() {
  static const log::Logger l = log::Get("abyss.server.listener");
  return l;
}
const log::Logger& ConnLog() {
  static const log::Logger l = log::Get("abyss.server.conn");
  return l;
}

inline int CloseSocket(socket_t s) {
#ifdef _WIN32
  return closesocket(s);
#else
  return close(s);
#endif
}

#ifdef _WIN32
constexpr int kShutdownRdwr = SD_BOTH;
#else
constexpr int kShutdownRdwr = SHUT_RDWR;
#endif
#ifdef _WIN32
std::string GetSocketError() { return std::to_string(WSAGetLastError()); }
bool IsSocketInterrupted() { return WSAGetLastError() == WSAEINTR; }
#else
std::string GetSocketError() { return std::strerror(errno); }
bool IsSocketInterrupted() { return errno == EINTR; }
#endif
}  // namespace

Server::Server(config::Config config) : config_(std::move(config)) {}

Server::~Server() { Shutdown(); }

bool Server::Initialize() {
#ifdef _WIN32
  WSADATA wsa_data;
  if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
    ABYSS_LOG_CRITICAL(ServerLog(), "WSAStartup failed");
    return false;
  }
#endif

  std::error_code ec;
  std::filesystem::create_directories(config_.queue.wal_path, ec);
  if (ec) {
    ABYSS_LOG_CRITICAL(ServerLog(), "create WAL directory failed",
                       {"path", std::string_view{config_.queue.wal_path}}, {"err", ec.message()});
    return false;
  }

  hot_store_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = config_.hot.max_memory_bytes,
      .shard_count = config_.hot.shard_count,
  });

  auto fsync_policy = queue::FsyncPolicyFromString(config_.queue.fsync_policy);
  if (!fsync_policy.has_value()) {
    ABYSS_LOG_CRITICAL(ServerLog(), "invalid fsync_policy",
                       {"value", std::string_view{config_.queue.fsync_policy}},
                       {"err", std::string_view{fsync_policy.error().message()}});
    return false;
  }

  auto queue_result = queue::WalQueue::Open(queue::WalConfig{
      .wal_path = config_.queue.wal_path,
      .segment_size_bytes = config_.queue.segment_size_bytes,
      .shard_count = hot_store_->shard_count(),
      .commit =
          {
              .policy = *fsync_policy,
              .interval = std::chrono::microseconds{config_.queue.group_commit_interval_us},
              .max_bytes = config_.queue.group_commit_max_bytes,
          },
      .min_retention = config_.queue.min_retention,
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
  });
  if (queue_result.has_value() && (*queue_result)->IsRecovering()) {
    ABYSS_LOG_CRITICAL(ServerLog(), "WAL opened but still recovering; refusing to start");
    return false;
  }
  if (!queue_result.has_value()) {
    ABYSS_LOG_CRITICAL(ServerLog(), "WAL open failed",
                       {"path", std::string_view{config_.queue.wal_path}},
                       {"err", std::string_view{queue_result.error().message()}});
    return false;
  }
  queue_ = std::move(*queue_result);

  consumer_rpc_ = std::make_unique<core::ConsumerRpc>(config_.consumer_rpc);

#ifdef ABYSS_HAVE_ROCKSDB
  std::filesystem::create_directories(config_.cold.data_path, ec);
  if (ec) {
    ABYSS_LOG_CRITICAL(ServerLog(), "create cold store directory failed",
                       {"path", std::string_view{config_.cold.data_path}}, {"err", ec.message()});
    return false;
  }

  auto cold_result = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
      .data_path = config_.cold.data_path,
      .write_buffer_size_bytes = config_.cold.write_buffer_size_bytes,
  });
  if (!cold_result.has_value()) {
    ABYSS_LOG_CRITICAL(ServerLog(), "cold store open failed",
                       {"path", std::string_view{config_.cold.data_path}},
                       {"err", std::string_view{cold_result.error().message()}});
    return false;  // NOLINT(readability-simplify-boolean-expr)
  }
  cold_store_ = std::move(*cold_result);

  std::vector<core::EvictionRule> overrides;
  overrides.reserve(config_.hot.eviction_overrides.size());
  for (const auto& o : config_.hot.eviction_overrides) {
    overrides.push_back({.prefix = o.prefix, .eviction = o.eviction});
  }
  core::EvictionPolicy eviction_policy{config_.hot.default_eviction, std::move(overrides)};

  cold_pool_ = std::make_unique<consumer::ColdConsumerPool>(
      *queue_, *cold_store_,
      consumer::ColdConsumerPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer =
              consumer::ColdConsumer::Config{
                  .quiet_threshold = config_.cold_consumer.quiet_threshold,
                  .safety_margin = config_.cold_consumer.safety_margin,
                  .jitter_fraction = config_.cold_consumer.jitter_fraction,
                  .buffer_high_water_bytes = config_.cold_consumer.buffer_high_water_bytes,
                  .buffer_low_water_bytes = config_.cold_consumer.buffer_low_water_bytes,
                  .max_flush_batch_size = config_.cold_consumer.max_flush_batch_size,
                  .queue_read_max_count = config_.cold_consumer.queue_read_max_count,
                  .queue_read_timeout = config_.cold_consumer.queue_read_timeout,
                  .retry_initial_backoff = config_.cold_consumer.retry_initial_backoff,
                  .retry_max_backoff = config_.cold_consumer.retry_max_backoff,
              },
      },
      eviction_policy);

  engine_ = std::make_unique<engine::TieringEngine>(
      *queue_, *hot_store_, *cold_store_, *cold_pool_, *consumer_rpc_,
      engine::TieringEngineConfig{
          .shard_count = hot_store_->shard_count(),
          .write_timeout = config_.engine.write_timeout,
          .min_rpc_wait_fraction = config_.engine.min_rpc_wait_fraction,
      });

  hot_pool_ = std::make_unique<consumer::HotConsumerPool>(
      *queue_, *hot_store_, *consumer_rpc_,
      consumer::HotConsumerPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer =
              consumer::HotConsumer::Config{
                  .read_batch_size = config_.hot_consumer.read_batch_size,
                  .read_timeout = config_.hot_consumer.read_timeout,
              },
      },
      eviction_policy);

  hot_eviction_worker_ = std::make_unique<hot::EvictionWorker>(
      *hot_store_,
      hot::EvictionWorker::Config{
          .tick = config_.hot.eviction_tick,
          .default_eviction =
              core::EvictionTTL{static_cast<uint64_t>(config_.hot.default_eviction.count())},
      });
  hot_eviction_worker_->Start();

  hot_pool_->Start();
  cold_pool_->Start();
#endif

  ready_.store(true, std::memory_order_release);
  ABYSS_LOG_INFO(ServerLog(), "server ready",
                 {"shard_count", static_cast<int64_t>(hot_store_->shard_count())});
  return true;
}

bool Server::SetupListener() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ == kInvalidSocket) {
    ABYSS_LOG_CRITICAL(ListenerLog(), "socket creation failed", {"err", GetSocketError()});
    return false;
  }

  int on = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&on), sizeof(on));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  if (config_.resp.bind == "0.0.0.0") {
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  } else {
    inet_pton(AF_INET, config_.resp.bind.c_str(), &addr.sin_addr);
  }
  addr.sin_port = htons(config_.resp.port);

  if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ABYSS_LOG_CRITICAL(ListenerLog(), "bind failed", {"bind", std::string_view{config_.resp.bind}},
                       {"port", static_cast<int64_t>(config_.resp.port)},
                       {"err", GetSocketError()});
    CloseSocket(listen_fd_);
    listen_fd_ = kInvalidSocket;
    return false;
  }

  sockaddr_in bound_addr{};
#ifdef _WIN32
  int bound_len = sizeof(bound_addr);
#else
  socklen_t bound_len = sizeof(bound_addr);
#endif
  if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound_addr), &bound_len) < 0) {
    ABYSS_LOG_CRITICAL(ListenerLog(), "getsockname failed", {"err", GetSocketError()});
    CloseSocket(listen_fd_);
    listen_fd_ = kInvalidSocket;
    return false;
  }
  config_.resp.port = ntohs(bound_addr.sin_port);

  if (listen(listen_fd_, 128) < 0) {
    ABYSS_LOG_CRITICAL(ListenerLog(), "listen failed", {"err", GetSocketError()});
    CloseSocket(listen_fd_);
    listen_fd_ = kInvalidSocket;
    return false;
  }

  return true;
}

void Server::Run(const std::atomic<bool>& stop) {
  if (!SetupListener()) return;

  std::cout << "abyss-ready bind=" << config_.resp.bind << " port=" << config_.resp.port << "\n"
            << std::flush;
  ABYSS_LOG_INFO(ListenerLog(), "listening", {"version", std::string_view{kVersion}},
                 {"bind", std::string_view{config_.resp.bind}},
                 {"port", static_cast<int64_t>(config_.resp.port)});

  bool saturated = false;

  while (!stop.load(std::memory_order_acquire)) {
    pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};

#ifdef _WIN32
    int ret = WSAPoll(&pfd, 1, 100);
#else
    int ret = poll(&pfd, 1, 100);
#endif

    if (ret < 0) {
      if (IsSocketInterrupted()) continue;
      ABYSS_LOG_ERROR(ListenerLog(), "poll failed", {"err", GetSocketError()});
      break;
    }
    if (ret == 0) continue;

    sockaddr_in client_addr{};
#ifdef _WIN32
    int client_len = sizeof(client_addr);
    socket_t client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd == kInvalidSocket) {
#else
    socklen_t client_len = sizeof(client_addr);
    socket_t client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd == kInvalidSocket) {
#endif
      if (IsSocketInterrupted()) continue;
      ABYSS_LOG_ERROR(ListenerLog(), "accept failed", {"err", GetSocketError()});
      break;
    }

    int on = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&on), sizeof(on));

#ifndef _WIN32
#ifdef __APPLE__
    setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
#endif

    CleanFinishedConnections();

    {
      std::lock_guard lock(connections_mutex_);
      if (connections_.size() >= config_.resp.max_connections) {
        if (!saturated) {
          saturated = true;
          ABYSS_LOG_WARN(ListenerLog(), "max_connections reached; rejecting new clients",
                         {"limit", static_cast<int64_t>(config_.resp.max_connections)});
        }
        CloseSocket(client_fd);
        continue;
      }
      if (saturated) {
        saturated = false;
        ABYSS_LOG_INFO(ListenerLog(), "connection pressure cleared",
                       {"in_use", static_cast<int64_t>(connections_.size())});
      }

      auto conn = std::make_unique<TrackedConnection>();
      conn->fd = client_fd;
      auto& ref = *conn;
      conn->thread =
          std::thread(&Server::HandleConnection, this, client_fd, std::ref(ref.finished));
      connections_.push_back(std::move(conn));
    }
  }

  Shutdown();
}

void Server::HandleConnection(socket_t client_fd, std::atomic<bool>& finished) {
  const auto client_id = next_client_id_.fetch_add(1);
  resp::ConnectionState state{
      .client_id = client_id,
      .client_name = {},
      .protocol_version = 2,
  };
  const auto& registry = resp::GlobalRegistry();
  // NOLINTNEXTLINE(misc-const-correctness)
  resp::RequestPipeline pipeline(registry, state);

  ABYSS_LOG_DEBUG(ConnLog(), "client connected", {"client_id", static_cast<uint64_t>(client_id)});

  std::vector<uint8_t> read_buf(4096);
  std::vector<uint8_t> pending;
  std::vector<uint8_t> output;

  for (;;) {
    auto n = recv(client_fd, reinterpret_cast<char*>(read_buf.data()),
                  static_cast<int>(read_buf.size()), 0);
    if (n <= 0) break;

    pending.insert(pending.end(), read_buf.begin(), read_buf.begin() + n);
    output.clear();

    auto result = pipeline.Process(pending, output);

    if (result.bytes_consumed > 0) {
      pending.erase(pending.begin(),
                    pending.begin() + static_cast<ptrdiff_t>(result.bytes_consumed));
    }

    if (!output.empty()) {
      size_t sent = 0;
      while (sent < output.size()) {
        auto w = send(client_fd, reinterpret_cast<const char*>(output.data() + sent),
                      static_cast<int>(output.size() - sent), 0);
        if (w <= 0) break;
        sent += static_cast<size_t>(w);
      }
      if (sent < output.size()) break;
    }

    if (result.close_requested) break;
  }

  CloseSocket(client_fd);
  ABYSS_LOG_DEBUG(ConnLog(), "client disconnected",
                  {"client_id", static_cast<uint64_t>(client_id)});
  finished.store(true, std::memory_order_release);
}

void Server::CleanFinishedConnections() {
  std::lock_guard lock(connections_mutex_);
  auto it = connections_.begin();
  while (it != connections_.end()) {
    if ((*it)->finished.load(std::memory_order_acquire)) {
      (*it)->thread.join();
      it = connections_.erase(it);
    } else {
      ++it;
    }
  }
}

void Server::Shutdown() {
  if (shutting_down_.exchange(true, std::memory_order_acq_rel)) return;

  ABYSS_LOG_INFO(ServerLog(), "shutdown starting");

  if (listen_fd_ != kInvalidSocket) {
    CloseSocket(listen_fd_);
    listen_fd_ = kInvalidSocket;
  }

  size_t draining = 0;
  {
    std::lock_guard lock(connections_mutex_);
    for (auto& conn : connections_) {
      if (!conn->finished.load(std::memory_order_acquire)) {
        ::shutdown(conn->fd, kShutdownRdwr);
        ++draining;
      }
    }
  }
  if (draining > 0) {
    ABYSS_LOG_INFO(ServerLog(), "draining in-flight connections",
                   {"count", static_cast<int64_t>(draining)});
  }

  {
    std::lock_guard lock(connections_mutex_);
    for (auto& conn : connections_) {
      if (conn->thread.joinable()) {
        conn->thread.join();
      }
    }
    connections_.clear();
  }

  if (cold_pool_) cold_pool_->Stop();
  if (hot_pool_) hot_pool_->Stop();
  if (hot_eviction_worker_) hot_eviction_worker_->Stop();

#ifdef _WIN32
  WSACleanup();
#endif

  ready_.store(false, std::memory_order_release);
  ABYSS_LOG_INFO(ServerLog(), "shutdown complete");
}

}  // namespace abyss::server
