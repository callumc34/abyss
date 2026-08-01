#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <unordered_map>
#include <vector>

#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/net/connection.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"

namespace abyss::net {

class TcpServer;

struct HandoffEntry {
  Fd fd;
  uint32_t remote_ipv4 = 0;
  uint16_t remote_port = 0;
  uint64_t client_id = 0;
};

// Reactor 0 also owns the listener fd; otherwise reactors are symmetric.
class Reactor {
 public:
  Reactor(uint32_t id, TcpServer& server, std::unique_ptr<Poller> poller, bool is_acceptor);
  ~Reactor();

  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;
  Reactor(Reactor&&) = delete;
  Reactor& operator=(Reactor&&) = delete;

  core::Result<void> AdoptListener(Socket listen_fd);

  void Start();
  void RequestStop();
  void Join();

  bool IsRunning() const noexcept { return running_.load(std::memory_order_acquire); }
  uint32_t Id() const noexcept { return id_; }
  Poller& GetPoller() noexcept { return *poller_; }

  // Thread-safe.
  void Handoff(HandoffEntry entry);

  size_t ConnectionCount() const noexcept { return connections_.size(); }

  // Max read-buffer high-water over this reactor's live connections. The value
  // is computed on the reactor thread (which exclusively owns connections_) and
  // published through an atomic, so any thread may read it.
  size_t MaxReadBufferHighWaterBytes() const noexcept {
    return static_cast<size_t>(read_buffer_high_water_.load(std::memory_order_acquire));
  }

 private:
  void Run();
  void DrainHandoff();
  void ProcessEvents(std::span<const Event> events);
  void RunReaper(core::SteadyTime now);
  void SweepClosed();
  void ForceCloseAll();
  void ObserveReadBufferHighWater(size_t bytes) noexcept;
  void RepublishReadBufferHighWater() noexcept;

  uint32_t id_;
  TcpServer& server_;
  std::unique_ptr<Poller> poller_;
  bool is_acceptor_;
  Socket listen_fd_ = kInvalidSocket;
  // Pointer identity distinguishes accept-side events from connections.
  char listener_marker_ = 0;
  bool listener_armed_ = false;

  std::atomic<bool> running_{false};
  // Set by Start() once a joinable thread is owned. Gates only the destructor
  // join; external observers keep reading running_ via IsRunning().
  std::atomic<bool> started_{false};
  std::thread thread_;
  std::atomic<uint64_t> read_buffer_high_water_{0};

  std::unordered_map<uint64_t, std::unique_ptr<Connection>> connections_;
  std::vector<uint64_t> to_close_;

  mutable std::mutex handoff_mu_;
  std::vector<HandoffEntry> handoff_queue_ ABYSS_GUARDED_BY(handoff_mu_);
};

}  // namespace abyss::net
