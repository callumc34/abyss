#include "abyss/net/connection.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/platform/net.h"

ABYSS_LOG_COMPONENT("abyss.net.conn")

namespace abyss::net {

namespace {

namespace pnet = abyss::platform::net;

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
      .backpressure_active = reg.Gauge(metrics::names::kNetBackpressureActive),
      .backpressure_entered = reg.Counter(metrics::names::kNetBackpressureEnteredTotal),
      .backpressure_exited = reg.Counter(metrics::names::kNetBackpressureExitedTotal),
      .late_completions = reg.Counter(metrics::names::kNetLateCompletionsTotal),
      .orphans = reg.Gauge(metrics::names::kNetOrphans),
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
                       NetMetrics& net_metrics, core::SteadyClockFn clock, resp::PipelineHost host)
    : fd_(std::move(fd)),
      remote_ipv4_(remote_ipv4),
      remote_port_(remote_port),
      client_id_(client_id),
      poller_(poller),
      config_(config),
      metrics_(net_metrics),
      clock_(std::move(clock)),
      pipeline_(std::make_unique<resp::RequestPipeline>(
          registry, resp::ConnectionState{.client_id = client_id, .protocol_version = 2}, deps,
          host,
          resp::PipelineLimits{.max_inflight = config.max_inflight_commands,
                               .reply_bytes = config.write_backpressure_bytes})),
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
  // A command waiting on the engine is not idleness.
  if (pipeline_ != nullptr && pipeline_->InFlight() > 0) return false;
  return (now - last_activity_) > config_.idle_timeout;
}

bool Connection::Busy() const noexcept {
  if (closed_) return false;
  return HasPendingWrites() || (pipeline_ != nullptr && pipeline_->InFlight() > 0);
}

core::Result<void> Connection::Arm() {
  armed_interest_ = DesiredInterest();
  return poller_.Add(fd_.Get(), armed_interest_, this);
}

bool Connection::ReadAllowed() const noexcept {
  return !reading_paused_ && !feed_blocked_ && !dispatch_stopped_ && !pending_close_.has_value();
}

EventKind Connection::DesiredInterest() const noexcept {
  EventKind k = EventKind::kNone;
  if (ReadAllowed()) k |= EventKind::kReadable;
  if (HasPendingWrites()) k |= EventKind::kWritable;
  return k;
}

bool Connection::SyncPollerInterest() {
  const EventKind want = DesiredInterest();
  if (want == armed_interest_) return true;
  if (auto r = poller_.Modify(fd_.Get(), want, this); !r) {
    ABYSS_LOG_WARN("poller modify failed", {"client_id", client_id_},
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
  // Keyed on backpressure_counted_, never on reading_paused_: a pause whose
  // poller update failed sets the pause flag but never reaches the increment.
  if (backpressure_counted_) {
    metrics_.backpressure_active.Decrement();
    backpressure_counted_ = false;
  }
  reading_paused_ = false;

  // De-register before close: a reused fd number must not deliver to us.
  if (fd_.Valid()) {
    if (auto r = poller_.Remove(fd_.Get()); !r) {
      ABYSS_LOG_DEBUG("poller remove on close failed", {"client_id", client_id_},
                      {"err", std::string_view{r.error().message()}});
    }
    pnet::ShutdownBoth(fd_.Get());
  }
  fd_.Reset();
  // Never waits for the engine: what it still owes posts lands as cleanup.
  if (pipeline_ != nullptr) pipeline_->Close();

  ABYSS_LOG_DEBUG("client closed", {"client_id", client_id_},
                  {"reason", metrics::ToStringView(reason)},
                  {"bytes_pending", static_cast<uint64_t>(WriteBufferBytes())});
  // Undispatched input and unwritten replies go with the socket.
  std::vector<uint8_t>().swap(read_buf_);
  std::vector<uint8_t>().swap(write_buf_);
  write_pos_ = 0;
}

std::unique_ptr<resp::RequestPipeline> Connection::ReleasePipeline() {
  ABYSS_DCHECK(closed_, "a pipeline released from an open connection");
  return std::move(pipeline_);
}

void Connection::OnReadable() {
  if (closed_) return;
  socket_drained_ = false;
  Pump();
}

void Connection::OnWritable() {
  if (closed_) return;
  Pump();
}

std::unique_ptr<resp::OrphanTask> Connection::OnTimer(core::SteadyTime now) {
  if (pipeline_ == nullptr) return nullptr;
  auto orphan = pipeline_->Expire(now);
  Pump();
  return orphan;
}

void Connection::StopDispatch() {
  if (closed_) return;
  dispatch_stopped_ = true;
  pipeline_->StopDispatch();
  SyncPollerInterest();
}

// Each pass dispatches what is buffered, writes what is ready, then
// reads more if it may. Edge-triggered backends redeliver nothing for
// bytes already in the kernel, so whenever reading is allowed again the
// socket is read without waiting for an event; and what is buffered is
// dispatched before reading is re-armed.
void Connection::Pump() {
  bool read_to_cap = false;
  for (;;) {
    if (closed_) return;
    const auto fed = Feed();
    if (closed_) return;
    if (read_to_cap && fed.consumed == 0 && !fed.blocked && !pending_close_.has_value()) {
      // A single command fills the entire read buffer yet cannot be framed:
      // it can never be consumed, so close now rather than wait for an edge
      // that will never arrive.
      ABYSS_LOG_WARN("read buffer at cap with unconsumable command", {"client_id", client_id_},
                     {"limit", static_cast<uint64_t>(config_.max_read_buffer_bytes)});
      Close(metrics::CloseReason::kOversize);
      return;
    }
    pipeline_->Drain(write_buf_);
    TryDrainWrite();
    if (closed_) return;
    if (EnforceWriteHardLimit()) return;
    UpdateBackpressure();
    if (closed_) return;
    const bool allowed = ReadAllowed();
    // Blocked or paused, it read nothing for a while: try the socket.
    if (allowed && !read_allowed_) socket_drained_ = false;
    read_allowed_ = allowed;
    if (!allowed || socket_drained_) break;
    read_to_cap = ReadSocket();
  }
  if (pending_close_.has_value() && pipeline_->InFlight() == 0 && !HasPendingWrites()) {
    Close(*pending_close_);
    return;
  }
  SyncPollerInterest();
}

resp::RequestPipeline::FeedResult Connection::Feed() {
  if (read_buf_.empty()) {
    feed_blocked_ = false;
    return {};
  }
  const auto fed = pipeline_->Feed(read_buf_, WriteBufferBytes());
  if (fed.consumed > 0) {
    read_buf_.erase(read_buf_.begin(), read_buf_.begin() + static_cast<ptrdiff_t>(fed.consumed));
  }
  feed_blocked_ = fed.blocked;
  // Both QUIT and an unframable protocol error close the client connection
  // once every reply before it, and its own, is written.
  if (pipeline_->close_request() != resp::RequestPipeline::CloseRequest::kNone &&
      !pending_close_.has_value()) {
    pending_close_ = metrics::CloseReason::kClient;
  }
  return fed;
}

bool Connection::ReadSocket() {
  while (true) {
    const size_t old_size = read_buf_.size();
    if (old_size >= config_.max_read_buffer_bytes) return true;
    const size_t want = std::min(kRecvChunkBytes, config_.max_read_buffer_bytes - old_size);
    read_buf_.resize(old_size + want);

    const auto n = pnet::Recv(fd_.Get(), read_buf_.data() + old_size, want, 0);
    if (n > 0) {
      read_buf_.resize(old_size + static_cast<size_t>(n));
      metrics_.bytes_in.Increment(static_cast<double>(n));
      TouchActivity();
      RecordReadBufferHighWater();
      continue;
    }
    read_buf_.resize(old_size);
    if (n == 0) {
      Close(metrics::CloseReason::kClient);
      return false;
    }
    const int err = pnet::LastError();
    if (pnet::IsInterrupted(err)) continue;
    if (pnet::IsWouldBlock(err)) {
      socket_drained_ = true;
      return false;
    }
    ABYSS_LOG_DEBUG("recv error", {"client_id", client_id_},
                    {"err", std::string_view{pnet::ErrorString(err)}});
    Close(metrics::CloseReason::kClient);
    return false;
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
      ABYSS_LOG_DEBUG("send error", {"client_id", client_id_},
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
    ABYSS_LOG_WARN("write buffer exceeded hard limit", {"client_id", client_id_},
                   {"bytes", static_cast<uint64_t>(WriteBufferBytes())},
                   {"limit", static_cast<uint64_t>(config_.write_hard_limit_bytes)});
    Close(metrics::CloseReason::kBackpressure);
    return true;
  }
  return false;
}

// Pauses at the slot cap or the reply-byte threshold, counting replies
// held for commands not yet written; resumes below write_resume_bytes
// with fewer than half the cap in use.
void Connection::UpdateBackpressure() {
  const size_t held = WriteBufferBytes() + pipeline_->ReplyBytes();
  const size_t slots = pipeline_->Slots();
  if (!reading_paused_) {
    if (held < config_.write_backpressure_bytes && slots < config_.max_inflight_commands) return;
    // reading_paused_ must flip first because SyncPollerInterest derives the
    // interest mask from it; the gauge moves only once the transition is
    // committed. A failed Modify closes with backpressure_counted_ still false,
    // so Close() cannot decrement an increment that never happened.
    reading_paused_ = true;
    if (!SyncPollerInterest()) return;
    backpressure_counted_ = true;
    metrics_.backpressure_active.Increment();
    metrics_.backpressure_entered.Increment();
    ABYSS_LOG_DEBUG("backpressure paused", {"client_id", client_id_},
                    {"write_bytes", static_cast<uint64_t>(held)},
                    {"slots", static_cast<uint64_t>(slots)});
    return;
  }
  if (held >= config_.write_resume_bytes || slots >= (config_.max_inflight_commands + 1) / 2) {
    return;
  }
  reading_paused_ = false;
  // A failed Modify here leaves backpressure_counted_ set, so the Close() it
  // triggers drops the gauge exactly once instead of leaking the increment.
  if (!SyncPollerInterest()) return;
  backpressure_counted_ = false;
  metrics_.backpressure_active.Decrement();
  metrics_.backpressure_exited.Increment();
  ABYSS_LOG_DEBUG("backpressure resumed", {"client_id", client_id_},
                  {"write_bytes", static_cast<uint64_t>(held)});
}

// Deliberately does not touch the process-wide gauge: every connection Setting
// it makes the value last-writer-wins. The fleet max is folded from these
// per-connection peaks by the server-scoped snapshotter.
void Connection::RecordReadBufferHighWater() {
  read_buf_high_water_ = std::max(read_buf_high_water_, read_buf_.size());
}

void Connection::TouchActivity() { last_activity_ = clock_(); }

}  // namespace abyss::net
