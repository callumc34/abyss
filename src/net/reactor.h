#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <unordered_map>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/timer_wheel.h"
#include "abyss/core/types.h"
#include "abyss/net/connection.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "abyss/resp/orphans.h"
#include "inbox.h"

namespace abyss::net {

class TcpServer;

struct HandoffEntry {
  Fd fd;
  uint32_t remote_ipv4 = 0;
  uint16_t remote_port = 0;
  uint64_t client_id = 0;
};

// Reactor 0 also owns the listener fd; otherwise reactors are symmetric.
// Its connections, their engine events, their deadlines and what they
// leave owed a post stay on it: none migrates to another reactor.
class Reactor {
 public:
  // Deadlines are kept to the millisecond, the poller's resolution, on
  // a wheel of 8192 one-millisecond buckets: a little over 8 s a round.
  static constexpr std::chrono::milliseconds kTimerTick{1};
  static constexpr size_t kTimerBuckets = 8192;

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
  // Where the engine posts for this reactor's connections.
  core::CompletionSink& Sink() noexcept { return inbox_; }

  // Thread-safe.
  void Handoff(HandoffEntry entry);

  size_t ConnectionCount() const noexcept { return connections_.size(); }

  // Max read-buffer high-water over this reactor's live connections. The value
  // is computed on the reactor thread (which exclusively owns connections_) and
  // published through an atomic, so any thread may read it.
  size_t MaxReadBufferHighWaterBytes() const noexcept {
    return static_cast<size_t>(read_buffer_high_water_.load(std::memory_order_acquire));
  }

  // Published by the reactor each pass, for any thread. Busy is
  // unknown until it has stopped dispatching.
  static constexpr size_t kBusyUnknown = std::numeric_limits<size_t>::max();
  size_t BusyConnections() const noexcept { return busy_.load(std::memory_order_acquire); }
  size_t OrphanCount() const noexcept { return orphan_count_.load(std::memory_order_acquire); }
  bool ForceClosed() const noexcept { return force_closed_.load(std::memory_order_acquire); }

  Inbox& InboxForTesting() noexcept { return inbox_; }

 private:
  void Run();
  void DrainInbox();
  void DrainHandoff();
  void ProcessEvents(std::span<const Event> events);
  void RunTimers(core::SteadyTime now);
  void RunReaper(core::SteadyTime now);
  void StopDispatch();
  void SweepClosed();
  void ForceCloseAll();
  void Publish(bool stopping);
  std::chrono::milliseconds PollTimeout(std::chrono::milliseconds reaper_tick) const;
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
  bool dispatch_stopped_ = false;

  std::atomic<bool> running_{false};
  // Set by Start() once a joinable thread is owned. Gates only the destructor
  // join; external observers keep reading running_ via IsRunning().
  std::atomic<bool> started_{false};
  std::thread thread_;
  std::atomic<uint64_t> read_buffer_high_water_{0};
  std::atomic<size_t> busy_{kBusyUnknown};
  std::atomic<size_t> orphan_count_{0};
  std::atomic<bool> force_closed_{false};

  // Declared in the order they must outlive one another: pipelines
  // hold the wheel's nodes and name the inbox in their slots.
  core::TimerWheel timers_;
  Inbox inbox_;
  resp::Orphans orphans_;
  std::unordered_map<uint64_t, std::unique_ptr<Connection>> connections_;
  std::vector<uint64_t> to_close_;
  std::vector<core::CompletionEvent> events_;
  std::vector<uint64_t> expired_;
  std::vector<Connection*> touched_;

  mutable std::mutex handoff_mu_;
  std::vector<HandoffEntry> handoff_queue_ ABYSS_GUARDED_BY(handoff_mu_);
};

}  // namespace abyss::net
