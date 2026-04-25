#include "abyss/net/tcp_server.h"

#include <algorithm>
#include <thread>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "reactor.h"

namespace abyss::net {

namespace {

const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.net");
  return l;
}

uint32_t ResolveIoThreads(uint32_t configured) {
  if (configured > 0) return configured;
  const auto hw = std::thread::hardware_concurrency();
  return std::clamp<uint32_t>(hw == 0 ? 1U : hw, 1U, 16U);
}

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

  IgnoreSigPipeProcessWide();

  const auto io_threads = ResolveIoThreads(config_.io_threads);
  config_.io_threads = io_threads;

  auto listen_result = CreateListenSocket(ListenOptions{
      .bind_addr = config_.bind,
      .port = config_.port,
      .backlog = static_cast<int>(config_.accept_queue),
      .reuse_addr = true,
  });
  if (!listen_result) {
    ABYSS_LOG_ERROR(Log(), "listener bind failed", {"bind", std::string_view{config_.bind}},
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
  ABYSS_LOG_INFO(Log(), "tcp server listening", {"bind", std::string_view{config_.bind}},
                 {"port", static_cast<int64_t>(bound_port_)},
                 {"io_threads", static_cast<int64_t>(io_threads)},
                 {"max_connections", static_cast<int64_t>(config_.max_connections)});
  return {};
}

void TcpServer::RequestStop() {
  if (stop_requested_.exchange(true, std::memory_order_acq_rel)) return;
  ABYSS_LOG_INFO(Log(), "tcp server stopping",
                 {"active", static_cast<int64_t>(active_count_.load(std::memory_order_relaxed))},
                 {"grace_seconds", static_cast<int64_t>(config_.shutdown_grace.count())});
  for (auto& reactor : reactors_) {
    reactor->RequestStop();
  }
}

void TcpServer::Join() {
  for (auto& reactor : reactors_) {
    reactor->Join();
  }
  listen_fd_.Reset();
  reactors_.clear();
  running_.store(false, std::memory_order_release);
  ABYSS_LOG_INFO(Log(), "tcp server stopped");
}

void TcpServer::Stop() {
  RequestStop();
  Join();
}

void TcpServer::AcceptAll() {
  while (true) {
    auto accepted = AcceptNonBlocking(listen_fd_.Get());
    if (!accepted) {
      if (accepted.error().code() == core::ErrorCode::kUnavailable) return;
      ABYSS_LOG_WARN(Log(), "accept failed", {"err", std::string_view{accepted.error().message()}});
      return;
    }

    SetTcpNoDelay(accepted->fd.Get());
    SetNoSigPipe(accepted->fd.Get());

    const auto prior = active_count_.fetch_add(1, std::memory_order_acq_rel);
    if (prior >= config_.max_connections) {
      active_count_.fetch_sub(1, std::memory_order_release);
      metrics_.Rejected(metrics::RejectReason::kMaxConnections).Increment();
      ABYSS_LOG_DEBUG(Log(), "accept rejected: max_connections",
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
