#include "abyss/net/tcp_server.h"

#include <algorithm>
#include <cstdlib>
#include <thread>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/net.h"
#include "reactor.h"

ABYSS_LOG_COMPONENT("abyss.net")

namespace abyss::net {

namespace {

uint32_t ResolveIoThreads(uint32_t configured) {
  if (configured > 0) return configured;
  const auto hw = std::thread::hardware_concurrency();
  return std::clamp<uint32_t>(hw == 0 ? 1U : hw, 1U, 16U);
}

// Bounds the stop: past its timeout it reports and ends the process,
// destructors unrun, since what is stuck still refers to what they
// would free.
class ShutdownWatchdog {
 public:
  ShutdownWatchdog(std::chrono::milliseconds timeout, std::function<void()> expired)
      : thread_([this, timeout, expired = std::move(expired)] {
          std::unique_lock lock(mu_);
          if (cv_.wait_for(lock, timeout, [this] { return done_; })) return;
          expired();
        }) {}
  ~ShutdownWatchdog() {
    {
      const std::scoped_lock lock(mu_);
      done_ = true;
    }
    cv_.notify_all();
    thread_.join();
  }

  ShutdownWatchdog(const ShutdownWatchdog&) = delete;
  ShutdownWatchdog& operator=(const ShutdownWatchdog&) = delete;
  ShutdownWatchdog(ShutdownWatchdog&&) = delete;
  ShutdownWatchdog& operator=(ShutdownWatchdog&&) = delete;

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool done_ = false;
  std::thread thread_;
};

}  // namespace

TcpServer::TcpServer(TcpServerConfig config, const resp::CommandRegistry& registry,
                     resp::PipelineDependencies deps, core::SteadyClockFn clock)
    : config_(std::move(config)),
      registry_(registry),
      deps_(deps),
      clock_(std::move(clock)),
      metrics_(NetMetrics::Register()) {}

TcpServer::~TcpServer() { Stop(); }

core::Result<void> TcpServer::Start() {
  if (running_.load(std::memory_order_acquire)) {
    return std::unexpected(
        core::Error{core::ErrorCode::kAlreadyExists, "TcpServer::Start called twice"});
  }

  platform::net::IgnoreSigPipe();

  const auto io_threads = ResolveIoThreads(config_.io_threads);
  config_.io_threads = io_threads;

  auto listen_result = CreateListenSocket(ListenOptions{
      .bind_addr = config_.bind,
      .port = config_.port,
      .backlog = static_cast<int>(config_.accept_queue),
      .reuse_addr = true,
  });
  if (!listen_result) {
    ABYSS_LOG_ERROR("listener bind failed", {"bind", std::string_view{config_.bind}},
                    {"port", static_cast<int64_t>(config_.port)},
                    {"err", std::string_view{listen_result.error().message()}});
    return std::unexpected(listen_result.error());
  }
  listen_fd_ = std::move(listen_result->fd);
  bound_port_ = listen_result->bound_port;

  reactors_.reserve(io_threads);
  for (uint32_t i = 0; i < io_threads; ++i) {
    auto poller_result = CreatePoller();
    if (!poller_result) {
      reactors_.clear();
      listen_fd_.Reset();
      return std::unexpected(poller_result.error());
    }
    const bool is_acceptor = (i == 0);
    reactors_.emplace_back(
        std::make_unique<Reactor>(i, *this, std::move(*poller_result), is_acceptor));
  }

  if (auto r = reactors_[0]->AdoptListener(listen_fd_.Get()); !r) {
    reactors_.clear();
    listen_fd_.Reset();
    return std::unexpected(r.error());
  }

  for (auto& reactor : reactors_) {
    reactor->Start();
  }

  running_.store(true, std::memory_order_release);
  ABYSS_LOG_INFO("tcp server listening", {"bind", std::string_view{config_.bind}},
                 {"port", static_cast<int64_t>(bound_port_)},
                 {"io_threads", static_cast<int64_t>(io_threads)},
                 {"max_connections", static_cast<int64_t>(config_.max_connections)});
  return {};
}

void TcpServer::StopAccepting() {
  if (stop_requested_.exchange(true, std::memory_order_acq_rel)) return;
  ABYSS_LOG_INFO("tcp server stopping",
                 {"active", static_cast<int64_t>(active_count_.load(std::memory_order_relaxed))},
                 {"grace_seconds", static_cast<int64_t>(config_.shutdown_grace.count())});
  WakeAll();
}

bool TcpServer::Quiesce(std::chrono::milliseconds grace) {
  std::unique_lock lock(progress_mu_);
  const bool quiet = progress_cv_.wait_for(lock, grace, [this] { return AllQuiet(); });
  if (!quiet) {
    ABYSS_LOG_WARN("shutdown grace passed with commands in flight; closing their connections",
                   {"grace_ms", static_cast<int64_t>(grace.count())});
  }
  return quiet;
}

void TcpServer::ForceClose() {
  force_close_.store(true, std::memory_order_release);
  WakeAll();
  std::unique_lock lock(progress_mu_);
  progress_cv_.wait(lock, [this] { return AllForced(); });
}

void TcpServer::Join() {
  JoinReactors();
  Release();
}

void TcpServer::JoinReactors() {
  engine_done_.store(true, std::memory_order_release);
  WakeAll();
  for (auto& reactor : reactors_) {
    reactor->Join();
  }
}

void TcpServer::Release() {
  listen_fd_.Reset();
  if (reactors_.empty()) return;
  reactors_.clear();
  running_.store(false, std::memory_order_release);
  ABYSS_LOG_INFO("tcp server stopped");
}

void TcpServer::Stop() {
  if (reactors_.empty()) {
    Release();
    return;
  }
  StopAccepting();
  (void)Quiesce(config_.shutdown_grace);
  ForceClose();
  std::unique_lock lock(progress_mu_);
  (void)progress_cv_.wait_for(lock, config_.shutdown_grace, [this] { return Orphans() == 0; });
  lock.unlock();
  Join();
}

void TcpServer::Shutdown(const EngineStop& engine) {
  StopAccepting();
  (void)Quiesce(config_.shutdown_grace);
  {
    const ShutdownWatchdog watchdog(config_.shutdown_timeout, [this, &engine] {
      const size_t stuck = engine.running_jobs ? engine.running_jobs() : 0;
      ABYSS_LOG_CRITICAL("shutdown timed out; exiting without cleanup",
                         {"timeout_ms", static_cast<int64_t>(config_.shutdown_timeout.count())},
                         {"stuck_jobs", static_cast<uint64_t>(stuck)},
                         {"orphans", static_cast<uint64_t>(Orphans())});
      log::Flush();
      std::_Exit(kShutdownTimeoutExitCode);
    });
    ForceClose();
    // The reactors go on taking what the engine posts as it stops.
    if (engine.stop) engine.stop();
    JoinReactors();
  }
  Release();
}

void TcpServer::RequestStop() {
  stop_requested_.store(true, std::memory_order_release);
  force_close_.store(true, std::memory_order_release);
  engine_done_.store(true, std::memory_order_release);
  WakeAll();
}

size_t TcpServer::MaxReadBufferHighWaterBytes() const noexcept {
  size_t max_bytes = 0;
  for (const auto& reactor : reactors_) {
    max_bytes = std::max(max_bytes, reactor->MaxReadBufferHighWaterBytes());
  }
  return max_bytes;
}

size_t TcpServer::Orphans() const noexcept {
  size_t orphans = 0;
  for (const auto& reactor : reactors_) orphans += reactor->OrphanCount();
  return orphans;
}

void TcpServer::SetInboxPostHookForTesting(const std::function<void()>& hook) {
  for (auto& reactor : reactors_) reactor->InboxForTesting().SetPostHookForTesting(hook);
}

void TcpServer::WakeAll() {
  for (auto& reactor : reactors_) {
    reactor->RequestStop();
  }
}

void TcpServer::NotifyProgress() {
  {
    const std::scoped_lock lock(progress_mu_);
  }
  progress_cv_.notify_all();
}

bool TcpServer::AllQuiet() const {
  return std::ranges::all_of(reactors_, [](const auto& reactor) {
    return !reactor->IsRunning() || reactor->BusyConnections() == 0;
  });
}

bool TcpServer::AllForced() const {
  return std::ranges::all_of(reactors_, [](const auto& reactor) {
    return !reactor->IsRunning() || reactor->ForceClosed();
  });
}

void TcpServer::AcceptAll() {
  while (true) {
    auto accepted = AcceptNonBlocking(listen_fd_.Get());
    if (!accepted) {
      if (accepted.error().code() == core::ErrorCode::kUnavailable) return;
      ABYSS_LOG_WARN("accept failed", {"err", std::string_view{accepted.error().message()}});
      return;
    }

    SetTcpNoDelay(accepted->fd.Get());
    platform::net::SetNoSigPipePerSocket(accepted->fd.Get());

    const auto prior = active_count_.fetch_add(1, std::memory_order_acq_rel);
    if (prior >= config_.max_connections) {
      active_count_.fetch_sub(1, std::memory_order_release);
      metrics_.Rejected(metrics::RejectReason::kMaxConnections).Increment();
      ABYSS_LOG_DEBUG("accept rejected: max_connections",
                      {"limit", static_cast<int64_t>(config_.max_connections)});
      continue;
    }

    const auto client_id = next_client_id_.fetch_add(1, std::memory_order_relaxed);
    const auto target = rr_index_.fetch_add(1, std::memory_order_relaxed) % config_.io_threads;

    HandoffEntry entry{
        .fd = std::move(accepted->fd),
        .remote_ipv4 = accepted->remote_ipv4,
        .remote_port = accepted->remote_port,
        .client_id = client_id,
    };
    reactors_[target]->Handoff(std::move(entry));
  }
}

}  // namespace abyss::net
