#include "server.h"

#ifdef _WIN32
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#define CLOSE_SOCKET(s) closesocket(s)
#define SHUTDOWN_RDWR SD_BOTH
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#define CLOSE_SOCKET(s) close(s)
#define SHUTDOWN_RDWR SHUT_RDWR
#endif

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <utility>

#include "abyss/resp/request_pipeline.h"
#include "abyss/version.h"

namespace abyss::server {

namespace {
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
    std::cerr << "WSAStartup failed\n";
    return false;
  }
#endif

  std::error_code ec;
  std::filesystem::create_directories(config_.queue.wal_path, ec);
  if (ec) {
    std::cerr << "failed to create WAL directory: " << ec.message() << "\n";
    return false;
  }

  hot_store_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = config_.hot.max_memory_bytes,
  });

  auto fsync_policy = queue::FsyncPolicyFromString(config_.queue.fsync_policy);
  if (!fsync_policy.has_value()) {
    std::cerr << "invalid fsync_policy: " << fsync_policy.error().message() << "\n";
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
    std::cerr << "queue opened but still recovering; refusing to start\n";
    return false;
  }
  if (!queue_result.has_value()) {
    std::cerr << "failed to open WAL queue: " << queue_result.error().message() << "\n";
    return false;
  }
  queue_ = std::move(*queue_result);

  consumer_rpc_ = std::make_unique<core::ConsumerRpc>();

#ifdef ABYSS_HAVE_ROCKSDB
  std::filesystem::create_directories(config_.cold.data_path, ec);
  if (ec) {
    std::cerr << "failed to create cold store directory: " << ec.message() << "\n";
    return false;
  }

  auto cold_result = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
      .data_path = config_.cold.data_path,
      .write_buffer_size_bytes = config_.cold.write_buffer_size_bytes,
  });
  if (!cold_result.has_value()) {
    std::cerr << "failed to open cold store: " << cold_result.error().message() << "\n";
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
                  .jitter_fraction = config_.cold_consumer.deadline_jitter_ratio,
                  .buffer_high_water_bytes = config_.cold_consumer.buffer_high_water_bytes,
                  .max_flush_batch_size = config_.cold_consumer.max_flush_batch_size,
              },
      },
      eviction_policy);

  engine_ = std::make_unique<engine::TieringEngine>(*queue_, *hot_store_, *cold_store_, *cold_pool_,
                                                    *consumer_rpc_, hot_store_->shard_count());

  hot_consumer_ = std::make_unique<consumer::HotConsumer>(*queue_, *hot_store_, 0,
                                                          config_.hot.default_eviction);

  hot_consumer_->Start();
  cold_pool_->Start();
#endif

  ready_.store(true, std::memory_order_release);
  return true;
}

bool Server::SetupListener() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ == kInvalidSocket) {
    std::cerr << "socket: " << GetSocketError() << "\n";
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
    std::cerr << "bind: " << GetSocketError() << "\n";
    CLOSE_SOCKET(listen_fd_);
    listen_fd_ = kInvalidSocket;
    return false;
  }

  if (listen(listen_fd_, 128) < 0) {
    std::cerr << "listen: " << GetSocketError() << "\n";
    CLOSE_SOCKET(listen_fd_);
    listen_fd_ = kInvalidSocket;
    return false;
  }

  return true;
}

void Server::Run(const std::atomic<bool>& stop) {
  if (!SetupListener()) return;

  std::cerr << "abyss v" << kVersion << " listening on " << config_.resp.bind << ":"
            << config_.resp.port << "\n";

  while (!stop.load(std::memory_order_acquire)) {
    pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};

#ifdef _WIN32
    int ret = WSAPoll(&pfd, 1, 100);
#else
    int ret = poll(&pfd, 1, 100);
#endif

    if (ret < 0) {
      if (IsSocketInterrupted()) continue;
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
        CLOSE_SOCKET(client_fd);
        continue;
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
  resp::ConnectionState state{
      .client_id = next_client_id_.fetch_add(1),
      .client_name = {},
      .protocol_version = 2,
  };
  const auto& registry = resp::GlobalRegistry();
  // NOLINTNEXTLINE(misc-const-correctness)
  resp::RequestPipeline pipeline(registry, state);

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

  CLOSE_SOCKET(client_fd);
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

  if (listen_fd_ != kInvalidSocket) {
    CLOSE_SOCKET(listen_fd_);
    listen_fd_ = kInvalidSocket;
  }

  {
    std::lock_guard lock(connections_mutex_);
    for (auto& conn : connections_) {
      if (!conn->finished.load(std::memory_order_acquire)) {
        ::shutdown(conn->fd, SHUTDOWN_RDWR);
      }
    }
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
  if (hot_consumer_) hot_consumer_->Stop();

#ifdef _WIN32
  WSACleanup();
#endif

  ready_.store(false, std::memory_order_release);
}

}  // namespace abyss::server
