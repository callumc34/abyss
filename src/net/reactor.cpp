#include "reactor.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/net/tcp_server.h"

namespace abyss::net {

namespace {

const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.net.reactor");
  return l;
}

}  // namespace

Reactor::Reactor(uint32_t id, TcpServer& server, std::unique_ptr<Poller> poller, bool is_acceptor)
    : id_(id), server_(server), poller_(std::move(poller)), is_acceptor_(is_acceptor) {}

Reactor::~Reactor() {
  if (running_.load(std::memory_order_acquire)) {
    server_.RequestStop();
    if (thread_.joinable()) thread_.join();
  }
}

core::Result<void> Reactor::AdoptListener(int listen_fd) {
  if (!is_acceptor_) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument,
                                       "Reactor::AdoptListener called on a non-acceptor reactor"});
  }
  if (auto r = poller_->Add(listen_fd, EventKind::kReadable, &listener_marker_); !r) {
    return std::unexpected(r.error());
  }
  listen_fd_ = listen_fd;
  listener_armed_ = true;
  return {};
}

void Reactor::Start() { thread_ = std::thread(&Reactor::Run, this); }

void Reactor::RequestStop() {
  if (auto r = poller_->Wake(); !r) {
    ABYSS_LOG_WARN(Log(), "wake during stop failed", {"reactor", static_cast<int64_t>(id_)},
                   {"err", std::string_view{r.error().message()}});
  }
}

void Reactor::Join() {
  if (thread_.joinable()) thread_.join();
}

void Reactor::Handoff(HandoffEntry entry) {
  {
    const std::scoped_lock lk(handoff_mu_);
    handoff_queue_.push_back(std::move(entry));
  }
  if (auto r = poller_->Wake(); !r) {
    ABYSS_LOG_WARN(Log(), "wake during handoff failed", {"reactor", static_cast<int64_t>(id_)},
                   {"err", std::string_view{r.error().message()}});
  }
}

void Reactor::Run() {
  running_.store(true, std::memory_order_release);
  ABYSS_LOG_DEBUG(Log(), "reactor started", {"reactor", static_cast<int64_t>(id_)},
                  {"acceptor", is_acceptor_});

  bool grace_set = false;
  core::SteadyTime grace_deadline{};
  const auto reaper_tick = server_.Config().reaper_tick;
  const auto shutdown_grace = server_.Config().shutdown_grace;

  while (true) {
    const bool stopping = server_.stop_requested_.load(std::memory_order_acquire);

    if (stopping && is_acceptor_ && listener_armed_) {
      if (auto r = poller_->Remove(listen_fd_); !r) {
        ABYSS_LOG_WARN(Log(), "listener disarm failed", {"reactor", static_cast<int64_t>(id_)},
                       {"err", std::string_view{r.error().message()}});
      }
      listener_armed_ = false;
    }

    if (stopping && !grace_set) {
      grace_deadline = std::chrono::steady_clock::now() + shutdown_grace;
      grace_set = true;
    }

    auto timeout = reaper_tick;
    if (grace_set) {
      const auto now = std::chrono::steady_clock::now();
      const auto remaining =
          std::chrono::duration_cast<std::chrono::milliseconds>(grace_deadline - now);
      timeout = std::clamp(remaining, std::chrono::milliseconds{0}, reaper_tick);
    }

    auto wait_result = poller_->Wait(timeout);
    if (!wait_result) {
      ABYSS_LOG_ERROR(Log(), "poll wait failed", {"reactor", static_cast<int64_t>(id_)},
                      {"err", std::string_view{wait_result.error().message()}});
      break;
    }

    DrainHandoff();
    ProcessEvents(*wait_result);

    const auto now = std::chrono::steady_clock::now();
    RunReaper(now);

    if (grace_set && now >= grace_deadline) {
      ForceCloseAll();
    }

    SweepClosed();

    if (grace_set && connections_.empty()) {
      break;
    }
  }

  ForceCloseAll();
  SweepClosed();

  running_.store(false, std::memory_order_release);
  ABYSS_LOG_DEBUG(Log(), "reactor stopped", {"reactor", static_cast<int64_t>(id_)});
}

void Reactor::DrainHandoff() {
  std::vector<HandoffEntry> drained;
  {
    const std::scoped_lock lk(handoff_mu_);
    drained.swap(handoff_queue_);
  }

  for (auto& entry : drained) {
    auto conn = std::make_unique<Connection>(
        std::move(entry.fd), entry.remote_ipv4, entry.remote_port, entry.client_id, *poller_,
        server_.registry_, server_.dispatcher_, server_.config_.connection, server_.metrics_,
        server_.clock_);

    if (auto r = conn->Arm(); !r) {
      ABYSS_LOG_WARN(Log(), "arm failed; dropping connection", {"client_id", entry.client_id},
                     {"err", std::string_view{r.error().message()}});
      server_.active_count_.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }

    connections_.emplace(entry.client_id, std::move(conn));
  }
}

void Reactor::ProcessEvents(std::span<const Event> events) {
  for (const auto& ev : events) {
    if (ev.user_data == &listener_marker_) {
      server_.AcceptAll();
      continue;
    }
    auto* conn = static_cast<Connection*>(ev.user_data);
    if (conn == nullptr || conn->IsClosed()) continue;

    if (Has(ev.kinds, EventKind::kReadable)) {
      conn->OnReadable();
    }
    if (!conn->IsClosed() && Has(ev.kinds, EventKind::kWritable)) {
      conn->OnWritable();
    }
  }
}

void Reactor::RunReaper(core::SteadyTime now) {
  for (auto& [id, conn] : connections_) {
    if (conn->IsClosed()) continue;
    if (conn->IsIdle(now)) {
      conn->Close(metrics::CloseReason::kIdle);
    }
  }
}

void Reactor::ForceCloseAll() {
  for (auto& [id, conn] : connections_) {
    if (!conn->IsClosed()) {
      conn->Close(metrics::CloseReason::kServerShutdown);
    }
  }
}

void Reactor::SweepClosed() {
  to_close_.clear();
  for (const auto& [id, conn] : connections_) {
    if (conn->IsClosed()) to_close_.push_back(id);
  }
  for (const uint64_t id : to_close_) {
    connections_.erase(id);
    server_.active_count_.fetch_sub(1, std::memory_order_relaxed);
  }
  to_close_.clear();
}

}  // namespace abyss::net
