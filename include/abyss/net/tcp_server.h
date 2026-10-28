#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/request_pipeline.h"

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
  // Bounds Shutdown from closing the connections to the reactors' stop.
  std::chrono::milliseconds shutdown_timeout{60000};
  std::chrono::milliseconds reaper_tick{1000};
};

// What Shutdown stops between closing the connections and stopping the
// reactors, which go on taking its events meanwhile.
struct EngineStop {
  // Fails every waiter and joins every thread that can post; leaves its
  // objects alive, since the reactors' cleanup calls into them.
  std::function<void()> stop;
  // Cold reads still running, for a timeout's report.
  std::function<size_t()> running_jobs;
};

// The exit status when Shutdown's timeout passes.
inline constexpr int kShutdownTimeoutExitCode = 70;

// Start, the stop steps, Stop, Shutdown and Join are main-thread; not
// concurrent with each other.
class TcpServer {
 public:
  TcpServer(TcpServerConfig config, const resp::CommandRegistry& registry,
            resp::PipelineDependencies deps, core::SteadyClockFn clock = core::DefaultSteadyClock);
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;
  TcpServer(TcpServer&&) = delete;
  TcpServer& operator=(TcpServer&&) = delete;

  core::Result<void> Start();

  // The stop, in order. Accepting ends, and every connection stops
  // reading and dispatching.
  void StopAccepting();
  // Until no connection has a command in flight or a reply unwritten,
  // or `grace` passes. True if they all finished.
  bool Quiesce(std::chrono::milliseconds grace);
  // Closes every connection. What the engine still owes them stays
  // with the reactors' orphans.
  void ForceClose();
  // Once the engine is done: each reactor takes the last of its events,
  // checks it has no orphans, and stops. Joins them.
  void Join();

  // All of it, for a server with no engine to stop on the way: orphans
  // are waited for, up to the grace, before the reactors stop.
  void Stop();
  // The server's: StopAccepting, Quiesce for the grace, ForceClose, then
  // the engine's stop and Join. Past shutdown_timeout from ForceClose,
  // logs what is stuck and exits the process with no destructors run,
  // since references the engine holds are still live.
  void Shutdown(const EngineStop& engine);
  // Every step at once, with no grace: the reactors' destructor uses it.
  void RequestStop();

  uint16_t BoundPort() const noexcept { return bound_port_; }
  bool IsRunning() const noexcept { return running_.load(std::memory_order_acquire); }
  size_t ActiveConnections() const noexcept {
    return active_count_.load(std::memory_order_relaxed);
  }
  // Fleet-wide max read-buffer high-water across live connections: the fold of
  // each reactor's own published max. Reads only atomics, so it is safe
  // alongside running reactors; like the other accessors it must not race
  // Start()/Join(), which are the only mutators of the reactor set.
  size_t MaxReadBufferHighWaterBytes() const noexcept;
  // Across the reactors, as each last published it.
  size_t Orphans() const noexcept;
  const TcpServerConfig& Config() const noexcept { return config_; }

  // Runs on the poster's thread as each reactor's inbox takes a post,
  // before the events land.
  void SetInboxPostHookForTesting(const std::function<void()>& hook);

 private:
  void AcceptAll();
  void JoinReactors();
  // Once joined: the listener and the reactors go.
  void Release();
  void WakeAll();
  void NotifyProgress();
  bool AllQuiet() const;
  bool AllForced() const;

  TcpServerConfig config_;
  const resp::CommandRegistry& registry_;
  resp::PipelineDependencies deps_;
  core::SteadyClockFn clock_;
  NetMetrics metrics_;

  Fd listen_fd_;
  uint16_t bound_port_ = 0;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> force_close_{false};
  std::atomic<bool> engine_done_{false};
  std::atomic<bool> running_{false};
  std::atomic<size_t> active_count_{0};
  std::atomic<uint64_t> next_client_id_{1};
  std::atomic<uint32_t> rr_index_{0};

  // Reactors report their progress through the stop here.
  std::mutex progress_mu_;
  std::condition_variable progress_cv_;

  std::vector<std::unique_ptr<Reactor>> reactors_;

  friend class Reactor;
};

}  // namespace abyss::net
