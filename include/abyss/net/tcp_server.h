#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/resp/command_registry.h"

namespace abyss::net {

class Reactor;

struct TcpServerConfig {
  std::string bind = "0.0.0.0";
  uint16_t port = 6379;
  uint32_t max_connections = 1024;
  uint32_t accept_queue = 128;
  // 0 = auto: min(hardware_concurrency, 16).
  uint32_t io_threads = 0;
  ConnectionConfig connection;
  std::chrono::seconds shutdown_grace{30};
  std::chrono::milliseconds reaper_tick{1000};
};

// Start/RequestStop/Stop/Join are main-thread; not concurrent with each other.
class TcpServer {
 public:
  TcpServer(TcpServerConfig config, const resp::CommandRegistry& registry,
            core::CommandDispatcher& dispatcher,
            core::SteadyClockFn clock = core::DefaultSteadyClock);
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;
  TcpServer(TcpServer&&) = delete;
  TcpServer& operator=(TcpServer&&) = delete;

  core::Result<void> Start();
  void RequestStop();
  void Join();
  void Stop();

  uint16_t BoundPort() const noexcept { return bound_port_; }
  bool IsRunning() const noexcept { return running_.load(std::memory_order_acquire); }
  size_t ActiveConnections() const noexcept {
    return active_count_.load(std::memory_order_relaxed);
  }
  const TcpServerConfig& Config() const noexcept { return config_; }

 private:
  void AcceptAll();

  TcpServerConfig config_;
  const resp::CommandRegistry& registry_;
  core::CommandDispatcher& dispatcher_;
  core::SteadyClockFn clock_;
  NetMetrics metrics_;

  Fd listen_fd_;
  uint16_t bound_port_ = 0;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::atomic<size_t> active_count_{0};
  std::atomic<uint64_t> next_client_id_{1};
  std::atomic<uint32_t> rr_index_{0};

  std::vector<std::unique_ptr<Reactor>> reactors_;

  friend class Reactor;
};

}  // namespace abyss::net
