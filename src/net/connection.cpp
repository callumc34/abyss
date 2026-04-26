#include "abyss/net/connection.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/platform/net.h"

namespace abyss::net {

namespace {

namespace pnet = abyss::platform::net;

const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.net.conn");
  return l;
}

constexpr size_t kRecvChunkBytes = 8192;
constexpr size_t kWriteCompactThresholdBytes = 65536;

}  // namespace

metrics::CounterHandle NetMetrics::Closed(metrics::CloseReason r) const noexcept {
  switch (r) {
    case metrics::CloseReason::kClient:
      return connections_closed_client;
    case metrics::CloseReason::kIdle:
      return connections_closed_idle;
    case metrics::CloseReason::kOversize:
      return connections_closed_oversize;
    case metrics::CloseReason::kBackpressure:
      return connections_closed_backpressure;
    case metrics::CloseReason::kServerShutdown:
      return connections_closed_server_shutdown;
  }
  return {};
}

metrics::CounterHandle NetMetrics::Rejected(metrics::RejectReason r) const noexcept {
  switch (r) {
    case metrics::RejectReason::kMaxConnections:
      return connections_rejected_max_connections;
    case metrics::RejectReason::kBindFamily:
      return connections_rejected_bind_family;
  }
  return {};
}

NetMetrics NetMetrics::Register() {
  auto& reg = metrics::Registry::Instance();
  return NetMetrics{
      .connections_active = reg.Gauge(metrics::names::kNetConnectionsActive),
      .connections_accepted = reg.Counter(metrics::names::kNetConnectionsAcceptedTotal),
      .bytes_in = reg.Counter(metrics::names::kNetBytesInTotal),
      .bytes_out = reg.Counter(metrics::names::kNetBytesOutTotal),
      .read_buffer_high_water = reg.Gauge(metrics::names::kNetReadBufferHighWaterBytes),
      .backpressure_active = reg.Gauge(metrics::names::kNetBackpressureActive),
      .backpressure_entered = reg.Counter(metrics::names::kNetBackpressureEnteredTotal),
      .backpressure_exited = reg.Counter(metrics::names::kNetBackpressureExitedTotal),
      .connections_closed_client =
          reg.Counter(metrics::names::kNetConnectionsClosedTotal, metrics::CloseReason::kClient),
      .connections_closed_idle =
          reg.Counter(metrics::names::kNetConnectionsClosedTotal, metrics::CloseReason::kIdle),
      .connections_closed_oversize =
          reg.Counter(metrics::names::kNetConnectionsClosedTotal, metrics::CloseReason::kOversize),
      .connections_closed_backpressure = reg.Counter(metrics::names::kNetConnectionsClosedTotal,
                                                     metrics::CloseReason::kBackpressure),
      .connections_closed_server_shutdown = reg.Counter(metrics::names::kNetConnectionsClosedTotal,
                                                        metrics::CloseReason::kServerShutdown),
      .connections_rejected_max_connections = reg.Counter(
          metrics::names::kNetConnectionsRejectedTotal, metrics::RejectReason::kMaxConnections),
      .connections_rejected_bind_family = reg.Counter(metrics::names::kNetConnectionsRejectedTotal,
                                                      metrics::RejectReason::kBindFamily),
  };
}

Connection::Connection(Fd fd, uint32_t remote_ipv4, uint16_t remote_port, uint64_t client_id,
                       Poller& poller, const resp::CommandRegistry& registry,
                       resp::PipelineDependencies deps, ConnectionConfig config,
                       NetMetrics& net_metrics, core::SteadyClockFn clock)
    : fd_(std::move(fd)),
      remote_ipv4_(remote_ipv4),
      remote_port_(remote_port),
      client_id_(client_id),
      poller_(poller),
      config_(config),
      metrics_(net_metrics),
      clock_(std::move(clock)),
      pipeline_(std::make_unique<resp::RequestPipeline>(
          registry, resp::ConnectionState{.client_id = client_id, .protocol_version = 2}, deps)),
      last_activity_(clock_()) {
  metrics_.connections_active.Increment();
  metrics_.connections_accepted.Increment();
}

Connection::~Connection() {
  if (!closed_) Close(metrics::CloseReason::kServerShutdown);
}

std::optional<metrics::CloseReason> Connection::CloseReasonValue() const noexcept {
  return close_reason_;
}

bool Connection::IsIdle(core::SteadyTime now) const noexcept {
  return (now - last_activity_) > config_.idle_timeout;
}

core::Result<void> Connection::Arm() {
  armed_interest_ = DesiredInterest();
  return poller_.Add(fd_.Get(), armed_interest_, this);
}

EventKind Connection::DesiredInterest() const noexcept {
  EventKind k = EventKind::kNone;
  if (!reading_paused_) k |= EventKind::kReadable;
  if (HasPendingWrites()) k |= EventKind::kWritable;
  return k;
}

bool Connection::SyncPollerInterest() {
  const EventKind want = DesiredInterest();
  if (want == armed_interest_) return true;
  if (auto r = poller_.Modify(fd_.Get(), want, this); !r) {
    ABYSS_LOG_WARN(Log(), "poller modify failed", {"client_id", client_id_},
                   {"err", std::string_view{r.error().message()}});
    Close(metrics::CloseReason::kClient);
    return false;
  }
  armed_interest_ = want;
  return true;
}

void Connection::Close(metrics::CloseReason reason) {
  if (closed_) return;
  closed_ = true;
  close_reason_ = reason;
  metrics_.Closed(reason).Increment();
  metrics_.connections_active.Decrement();
  if (reading_paused_) {
    metrics_.backpressure_active.Decrement();
    reading_paused_ = false;
  }

  // De-register before close: a reused fd number must not deliver to us.
  if (fd_.Valid()) {
    if (auto r = poller_.Remove(fd_.Get()); !r) {
      ABYSS_LOG_DEBUG(Log(), "poller remove on close failed", {"client_id", client_id_},
                      {"err", std::string_view{r.error().message()}});
    }
    pnet::ShutdownBoth(fd_.Get());
  }
  fd_.Reset();

  ABYSS_LOG_DEBUG(Log(), "client closed", {"client_id", client_id_},
                  {"reason", metrics::ToStringView(reason)},
                  {"bytes_pending", static_cast<uint64_t>(WriteBufferBytes())});
}

void Connection::OnReadable() {
  if (closed_) return;

  while (true) {
    const size_t old_size = read_buf_.size();
    if (old_size + kRecvChunkBytes > config_.max_read_buffer_bytes &&
        old_size >= config_.max_read_buffer_bytes) {
      ABYSS_LOG_WARN(Log(), "read buffer would exceed max", {"client_id", client_id_},
                     {"limit", static_cast<uint64_t>(config_.max_read_buffer_bytes)});
      Close(metrics::CloseReason::kOversize);
      return;
    }
    const size_t want = std::min(kRecvChunkBytes, config_.max_read_buffer_bytes - old_size);
    read_buf_.resize(old_size + want);

    const auto n = pnet::Recv(fd_.Get(), read_buf_.data() + old_size, want, 0);
    if (n > 0) {
      read_buf_.resize(old_size + static_cast<size_t>(n));
      metrics_.bytes_in.Increment(static_cast<double>(n));
      TouchActivity();
      RecordReadBufferHighWater();
      if (read_buf_.size() >= config_.max_read_buffer_bytes) break;
      continue;
    }
    read_buf_.resize(old_size);
    if (n == 0) {
      Close(metrics::CloseReason::kClient);
      return;
    }
    const int err = pnet::LastError();
    if (pnet::IsInterrupted(err)) continue;
    if (pnet::IsWouldBlock(err)) break;
    ABYSS_LOG_DEBUG(Log(), "recv error", {"client_id", client_id_},
                    {"err", std::string_view{pnet::ErrorString(err)}});
    Close(metrics::CloseReason::kClient);
    return;
  }

  DispatchPipelineOutput();
  if (closed_) return;

  TryDrainWrite();
  if (closed_) return;

  if (EnforceWriteHardLimit()) return;
  MaybePauseReading();
  if (closed_) return;

  if (close_after_drain_ && !HasPendingWrites()) {
    Close(metrics::CloseReason::kClient);
    return;
  }

  SyncPollerInterest();
}

void Connection::OnWritable() {
  if (closed_) return;

  TryDrainWrite();
  if (closed_) return;

  MaybeResumeReading();
  if (closed_) return;

  if (close_after_drain_ && !HasPendingWrites()) {
    Close(metrics::CloseReason::kClient);
    return;
  }

  SyncPollerInterest();
}

void Connection::DispatchPipelineOutput() {
  if (read_buf_.empty()) return;
  const auto result = pipeline_->Process(read_buf_, write_buf_);
  if (result.bytes_consumed > 0) {
    read_buf_.erase(read_buf_.begin(),
                    read_buf_.begin() + static_cast<ptrdiff_t>(result.bytes_consumed));
  }
  if (result.close_requested) {
    close_after_drain_ = true;
  }
}

void Connection::TryDrainWrite() {
  while (write_pos_ < write_buf_.size()) {
    const size_t remaining = write_buf_.size() - write_pos_;
    const auto n = pnet::Send(fd_.Get(), write_buf_.data() + write_pos_, remaining,
                              pnet::SendFlagsNoSigPipe());
    if (n > 0) {
      write_pos_ += static_cast<size_t>(n);
      metrics_.bytes_out.Increment(static_cast<double>(n));
      continue;
    }
    if (n < 0) {
      const int err = pnet::LastError();
      if (pnet::IsInterrupted(err)) continue;
      if (pnet::IsWouldBlock(err)) return;
      if (pnet::IsBrokenPipe(err) || pnet::IsConnReset(err)) {
        Close(metrics::CloseReason::kClient);
        return;
      }
      ABYSS_LOG_DEBUG(Log(), "send error", {"client_id", client_id_},
                      {"err", std::string_view{pnet::ErrorString(err)}});
      Close(metrics::CloseReason::kClient);
      return;
    }
    return;
  }

  if (write_pos_ == write_buf_.size()) {
    write_buf_.clear();
    write_pos_ = 0;
  } else if (write_pos_ > kWriteCompactThresholdBytes) {
    write_buf_.erase(write_buf_.begin(), write_buf_.begin() + static_cast<ptrdiff_t>(write_pos_));
    write_pos_ = 0;
  }
}

bool Connection::EnforceWriteHardLimit() {
  if (WriteBufferBytes() > config_.write_hard_limit_bytes) {
    ABYSS_LOG_WARN(Log(), "write buffer exceeded hard limit", {"client_id", client_id_},
                   {"bytes", static_cast<uint64_t>(WriteBufferBytes())},
                   {"limit", static_cast<uint64_t>(config_.write_hard_limit_bytes)});
    Close(metrics::CloseReason::kBackpressure);
    return true;
  }
  return false;
}

void Connection::MaybePauseReading() {
  if (reading_paused_) return;
  if (WriteBufferBytes() < config_.write_backpressure_bytes) return;

  reading_paused_ = true;
  if (!SyncPollerInterest()) return;
  metrics_.backpressure_active.Increment();
  metrics_.backpressure_entered.Increment();
  ABYSS_LOG_DEBUG(Log(), "backpressure paused", {"client_id", client_id_},
                  {"write_bytes", static_cast<uint64_t>(WriteBufferBytes())});
}

void Connection::MaybeResumeReading() {
  if (!reading_paused_) return;
  if (WriteBufferBytes() >= config_.write_resume_bytes) return;

  reading_paused_ = false;
  if (!SyncPollerInterest()) return;
  metrics_.backpressure_active.Decrement();
  metrics_.backpressure_exited.Increment();
  ABYSS_LOG_DEBUG(Log(), "backpressure resumed", {"client_id", client_id_},
                  {"write_bytes", static_cast<uint64_t>(WriteBufferBytes())});
}

void Connection::RecordReadBufferHighWater() {
  if (read_buf_.size() > read_buf_high_water_) {
    read_buf_high_water_ = read_buf_.size();
    metrics_.read_buffer_high_water.Set(static_cast<double>(read_buf_high_water_));
  }
}

void Connection::TouchActivity() { last_activity_ = clock_(); }

}  // namespace abyss::net
