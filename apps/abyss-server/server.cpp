#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <utility>

#include "abyss/resp/request_pipeline.h"
#include "abyss/version.h"

namespace abyss::server {

Server::Server(config::Config config) : config_(std::move(config)) {}

Server::~Server() { Shutdown(); }

bool Server::Initialize() {
  std::error_code ec;
  std::filesystem::create_directories(config_.queue.wal_path, ec);
  if (ec) {
    std::cerr << "failed to create WAL directory: " << ec.message() << "\n";
    return false;
  }

  queue_ = std::make_unique<queue::WalQueue>(queue::WalConfig{
      .wal_path = config_.queue.wal_path,
      .segment_size_bytes = config_.queue.segment_size_bytes,
  });

  hot_store_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = config_.hot.max_memory_bytes,
  });

  compaction_buffer_ = std::make_unique<consumer::CompactionBuffer>();
  consumer_rpc_ = std::make_unique<core::ConsumerRpc>();

#ifdef ABYSS_HAVE_ROCKSDB
  std::filesystem::create_directories(config_.cold.data_path, ec);
  if (ec) {
    std::cerr << "failed to create cold store directory: " << ec.message()
              << "\n";
    return false;
  }

  auto cold_result =
      cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
          .data_path = config_.cold.data_path,
          .write_buffer_size_bytes = config_.cold.write_buffer_size_bytes,
      });
  if (!cold_result.has_value()) {
    std::cerr << "failed to open cold store: "
              << cold_result.error().message() << "\n";
    return false;
  }
  cold_store_ = std::move(*cold_result);

  engine_ = std::make_unique<engine::TieringEngine>(
      *queue_, *hot_store_, *cold_store_, *compaction_buffer_, *consumer_rpc_,
      hot_store_->shard_count());

  hot_consumer_ = std::make_unique<consumer::HotConsumer>(
      *queue_, *hot_store_, 0, config_.hot.default_eviction);
  cold_consumer_ = std::make_unique<consumer::ColdConsumer>(
      *queue_, *cold_store_, *compaction_buffer_, 0);

  hot_consumer_->Start();
  cold_consumer_->Start();
#endif

  ready_.store(true, std::memory_order_release);
  return true;
}

bool Server::SetupListener() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    std::cerr << "socket: " << std::strerror(errno) << "\n";
    return false;
  }

  int on = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  if (config_.resp.bind == "0.0.0.0") {
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  } else {
    inet_pton(AF_INET, config_.resp.bind.c_str(), &addr.sin_addr);
  }
  addr.sin_port = htons(config_.resp.port);

  if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "bind: " << std::strerror(errno) << "\n";
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  if (listen(listen_fd_, 128) < 0) {
    std::cerr << "listen: " << std::strerror(errno) << "\n";
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  return true;
}

void Server::Run(const std::atomic<bool>& stop) {
  if (!SetupListener()) return;

  std::cerr << "abyss v" << kVersion << " listening on " << config_.resp.bind
            << ":" << config_.resp.port << "\n";

  while (!stop.load(std::memory_order_acquire)) {
    pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
    int ret = poll(&pfd, 1, 100);
    if (ret < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (ret == 0) continue;

    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(
        listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
      if (errno == EINTR) continue;
      break;
    }

    int on = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#ifdef __APPLE__
    setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif

    CleanFinishedConnections();

    {
      std::lock_guard lock(connections_mutex_);
      if (connections_.size() >= config_.resp.max_connections) {
        close(client_fd);
        continue;
      }

      auto conn = std::make_unique<TrackedConnection>();
      conn->fd = client_fd;
      auto& ref = *conn;
      conn->thread = std::thread(&Server::HandleConnection, this, client_fd,
                                 std::ref(ref.finished));
      connections_.push_back(std::move(conn));
    }
  }

  Shutdown();
}

void Server::HandleConnection(int client_fd, std::atomic<bool>& finished) {
  resp::ConnectionState state{
      .client_id = next_client_id_.fetch_add(1),
      .client_name = {},
      .protocol_version = 2,
  };
  const auto& registry = resp::GlobalRegistry();
  resp::RequestPipeline pipeline(registry, state);

  std::vector<uint8_t> read_buf(4096);
  std::vector<uint8_t> pending;
  std::vector<uint8_t> output;

  for (;;) {
    auto n = recv(client_fd, read_buf.data(), read_buf.size(), 0);
    if (n <= 0) break;

    pending.insert(pending.end(), read_buf.begin(),
                   read_buf.begin() + n);
    output.clear();

    auto result = pipeline.Process(pending, output);

    if (result.bytes_consumed > 0) {
      pending.erase(
          pending.begin(),
          pending.begin() + static_cast<ptrdiff_t>(result.bytes_consumed));
    }

    if (!output.empty()) {
      size_t sent = 0;
      while (sent < output.size()) {
        auto w =
            send(client_fd, output.data() + sent, output.size() - sent, 0);
        if (w <= 0) break;
        sent += static_cast<size_t>(w);
      }
      if (sent < output.size()) break;
    }

    if (result.close_requested) break;
  }

  close(client_fd);
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

  if (listen_fd_ >= 0) {
    close(listen_fd_);
    listen_fd_ = -1;
  }

  {
    std::lock_guard lock(connections_mutex_);
    for (auto& conn : connections_) {
      if (!conn->finished.load(std::memory_order_acquire)) {
        ::shutdown(conn->fd, SHUT_RDWR);
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

  if (cold_consumer_) cold_consumer_->Stop();
  if (hot_consumer_) hot_consumer_->Stop();

  ready_.store(false, std::memory_order_release);
}

}  // namespace abyss::server
