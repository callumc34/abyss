#include "reactor.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/net/tcp_server.h"

ABYSS_LOG_COMPONENT("abyss.net.reactor")

namespace abyss::net {

Reactor::Reactor(uint32_t id, TcpServer& server, std::unique_ptr<Poller> poller, bool is_acceptor)
    : id_(id),
      server_(server),
      poller_(std::move(poller)),
      is_acceptor_(is_acceptor),
      timers_(kTimerTick, kTimerBuckets),
      inbox_(*poller_),
      orphans_(server.metrics_.orphans) {}

// running_ is set by the worker, so gating on it leaves a window between
// Start() and the first line of Run() where a joinable thread would go
// unjoined and ~thread would call std::terminate. started_ closes it.
Reactor::~Reactor() {
  if (!started_.load(std::memory_order_acquire) || !thread_.joinable()) return;
  server_.RequestStop();
  RequestStop();
  thread_.join();
}

core::Result<void> Reactor::AdoptListener(Socket listen_fd) {
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

void Reactor::Start() {
  thread_ = std::thread(&Reactor::Run, this);
  started_.store(true, std::memory_order_release);
}

void Reactor::RequestStop() {
  if (auto r = poller_->Wake(); !r) {
    ABYSS_LOG_WARN("wake during stop failed", {"reactor", static_cast<int64_t>(id_)},
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
    ABYSS_LOG_WARN("wake during handoff failed", {"reactor", static_cast<int64_t>(id_)},
                   {"err", std::string_view{r.error().message()}});
  }
}

// Runs until the engine is done, so every event it posts lands here,
// and what the reactor's connections left owed a post is cleaned up on
// this thread.
void Reactor::Run() {
  running_.store(true, std::memory_order_release);
  ABYSS_LOG_DEBUG("reactor started", {"reactor", static_cast<int64_t>(id_)},
                  {"acceptor", is_acceptor_});

  const auto reaper_tick = server_.Config().reaper_tick;
  while (true) {
    const bool stopping = server_.stop_requested_.load(std::memory_order_acquire);
    if (stopping) StopDispatch();

    auto wait_result = poller_->Wait(PollTimeout(reaper_tick));
    if (!wait_result) {
      ABYSS_LOG_ERROR("poll wait failed", {"reactor", static_cast<int64_t>(id_)},
                      {"err", std::string_view{wait_result.error().message()}});
      break;
    }

    DrainInbox();
    DrainHandoff();
    ProcessEvents(*wait_result);

    const auto now = core::SteadyClock::now();
    RunTimers(now);
    RunReaper(now);

    if (server_.force_close_.load(std::memory_order_acquire) &&
        !force_closed_.load(std::memory_order_relaxed)) {
      ForceCloseAll();
      SweepClosed();
      force_closed_.store(true, std::memory_order_release);
    }
    SweepClosed();
    Publish(stopping);

    if (server_.engine_done_.load(std::memory_order_acquire)) break;
  }

  // Nothing posts once the engine is done: one more take lands the
  // last of it, and whatever its connections left owed is settled.
  DrainInbox();
  ForceCloseAll();
  SweepClosed();
  inbox_.Close();
  ABYSS_DCHECK(orphans_.empty(), "a reactor stopped with orphans still owed a post");
  force_closed_.store(true, std::memory_order_release);
  Publish(true);

  running_.store(false, std::memory_order_release);
  ABYSS_LOG_DEBUG("reactor stopped", {"reactor", static_cast<int64_t>(id_)});
}

std::chrono::milliseconds Reactor::PollTimeout(std::chrono::milliseconds reaper_tick) const {
  const auto due = timers_.NextDue();
  if (!due.has_value()) return reaper_tick;
  const auto until = *due - core::SteadyClock::now();
  if (until <= std::chrono::steady_clock::duration::zero()) return std::chrono::milliseconds{0};
  // Rounded up: a wait that ends early only spins.
  return std::min(std::chrono::ceil<std::chrono::milliseconds>(until), reaper_tick);
}

void Reactor::DrainInbox() {
  inbox_.Take(events_);
  for (const core::CompletionEvent& event : events_) {
    const auto it = connections_.find(event.client_id);
    Connection* const conn = it != connections_.end() ? it->second.get() : nullptr;
    if (!orphans_.Route(event, conn != nullptr ? conn->Pipeline() : nullptr)) {
      server_.metrics_.late_completions.Increment();
      continue;
    }
    if (conn != nullptr && (touched_.empty() || touched_.back() != conn)) touched_.push_back(conn);
  }
  events_.clear();
  for (Connection* const conn : touched_) conn->Pump();
  touched_.clear();
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
        server_.registry_, server_.deps_, server_.config_.connection, server_.metrics_,
        server_.clock_, resp::PipelineHost{.sink = &inbox_, .timers = &timers_});

    if (auto r = conn->Arm(); !r) {
      ABYSS_LOG_WARN("arm failed; dropping connection", {"client_id", entry.client_id},
                     {"err", std::string_view{r.error().message()}});
      server_.active_count_.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }
    if (dispatch_stopped_) conn->StopDispatch();

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
      // A connection's high-water only grows, and only while reading, so
      // folding it here keeps the published max exact at O(1) per event.
      ObserveReadBufferHighWater(conn->ReadBufferHighWater());
    }
    if (!conn->IsClosed() && Has(ev.kinds, EventKind::kWritable)) {
      conn->OnWritable();
    }
  }
}

void Reactor::RunTimers(core::SteadyTime now) {
  timers_.Expire(now, expired_);
  for (const uint64_t client_id : expired_) {
    const auto it = connections_.find(client_id);
    if (it == connections_.end() || it->second->Pipeline() == nullptr) {
      orphans_.Expire(client_id, now);
      continue;
    }
    if (auto orphan = it->second->OnTimer(now)) orphans_.Adopt(std::move(orphan));
  }
  expired_.clear();
}

void Reactor::RunReaper(core::SteadyTime now) {
  for (auto& [id, conn] : connections_) {
    if (conn->IsClosed()) continue;
    if (conn->IsIdle(now)) {
      conn->Close(metrics::CloseReason::kIdle);
    }
  }
}

void Reactor::StopDispatch() {
  if (dispatch_stopped_) return;
  dispatch_stopped_ = true;
  if (is_acceptor_ && listener_armed_) {
    if (auto r = poller_->Remove(listen_fd_); !r) {
      ABYSS_LOG_WARN("listener disarm failed", {"reactor", static_cast<int64_t>(id_)},
                     {"err", std::string_view{r.error().message()}});
    }
    listener_armed_ = false;
  }
  for (auto& [id, conn] : connections_) conn->StopDispatch();
}

void Reactor::ForceCloseAll() {
  for (auto& [id, conn] : connections_) {
    if (!conn->IsClosed()) {
      conn->Close(metrics::CloseReason::kServerShutdown);
    }
  }
}

// A closed connection goes at once, its socket and buffers with it; a
// pipeline the engine still owes a post stays with the orphans.
void Reactor::SweepClosed() {
  to_close_.clear();
  for (const auto& [id, conn] : connections_) {
    if (conn->IsClosed()) to_close_.push_back(id);
  }
  if (to_close_.empty()) return;
  for (const uint64_t id : to_close_) {
    const auto it = connections_.find(id);
    if (auto pipeline = it->second->ReleasePipeline()) orphans_.Adopt(std::move(pipeline));
    connections_.erase(it);
    server_.active_count_.fetch_sub(1, std::memory_order_relaxed);
  }
  to_close_.clear();
  // Departure is the only way the live max can fall, so it is the only place
  // the incrementally-folded value needs a full recompute.
  RepublishReadBufferHighWater();
}

void Reactor::Publish(bool stopping) {
  orphan_count_.store(orphans_.size(), std::memory_order_release);
  if (!stopping) return;
  size_t busy = 0;
  for (const auto& [id, conn] : connections_) {
    if (conn->Busy()) ++busy;
  }
  busy_.store(busy, std::memory_order_release);
  server_.NotifyProgress();
}

void Reactor::ObserveReadBufferHighWater(size_t bytes) noexcept {
  const auto value = static_cast<uint64_t>(bytes);
  if (value > read_buffer_high_water_.load(std::memory_order_relaxed)) {
    read_buffer_high_water_.store(value, std::memory_order_release);
  }
}

void Reactor::RepublishReadBufferHighWater() noexcept {
  uint64_t max_bytes = 0;
  for (const auto& [id, conn] : connections_) {
    max_bytes = std::max(max_bytes, static_cast<uint64_t>(conn->ReadBufferHighWater()));
  }
  read_buffer_high_water_.store(max_bytes, std::memory_order_release);
}

}  // namespace abyss::net
