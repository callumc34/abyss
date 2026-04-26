#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/request_pipeline.h"

namespace abyss::net {

// Default-constructed handles are no-ops, so tests can pass `NetMetrics{}`.
struct NetMetrics {
  metrics::GaugeHandle connections_active;
  metrics::CounterHandle connections_accepted;
  metrics::CounterHandle bytes_in;
  metrics::CounterHandle bytes_out;
  metrics::GaugeHandle read_buffer_high_water;
  metrics::GaugeHandle backpressure_active;
  metrics::CounterHandle backpressure_entered;
  metrics::CounterHandle backpressure_exited;

  metrics::CounterHandle Closed(metrics::CloseReason r) const noexcept;
  metrics::CounterHandle Rejected(metrics::RejectReason r) const noexcept;

  metrics::CounterHandle connections_closed_client;
  metrics::CounterHandle connections_closed_idle;
  metrics::CounterHandle connections_closed_oversize;
  metrics::CounterHandle connections_closed_backpressure;
  metrics::CounterHandle connections_closed_server_shutdown;
  metrics::CounterHandle connections_rejected_max_connections;
  metrics::CounterHandle connections_rejected_bind_family;

  static NetMetrics Register();
};

struct ConnectionConfig {
  size_t max_read_buffer_bytes = 67108864;
  size_t write_backpressure_bytes = 4194304;
  size_t write_resume_bytes = 1048576;
  size_t write_hard_limit_bytes = 16777216;
  std::chrono::seconds idle_timeout{300};
};

// All non-const methods run on the owning reactor thread.
class Connection {
 public:
  Connection(Fd fd, uint32_t remote_ipv4, uint16_t remote_port, uint64_t client_id, Poller& poller,
             const resp::CommandRegistry& registry, resp::PipelineDependencies deps,
             ConnectionConfig config, NetMetrics& net_metrics,
             core::SteadyClockFn clock = core::DefaultSteadyClock);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
  Connection(Connection&&) = delete;
  Connection& operator=(Connection&&) = delete;

  Socket RawFd() const noexcept { return fd_.Get(); }
  uint64_t ClientId() const noexcept { return client_id_; }
  uint32_t RemoteIpv4() const noexcept { return remote_ipv4_; }
  uint16_t RemotePort() const noexcept { return remote_port_; }

  bool IsClosed() const noexcept { return closed_; }
  std::optional<metrics::CloseReason> CloseReasonValue() const noexcept;

  core::SteadyTime LastActivity() const noexcept { return last_activity_; }
  bool IsIdle(core::SteadyTime now) const noexcept;

  bool ReadingPaused() const noexcept { return reading_paused_; }
  size_t WriteBufferBytes() const noexcept { return write_buf_.size() - write_pos_; }
  size_t ReadBufferHighWater() const noexcept { return read_buf_high_water_; }
  bool HasPendingWrites() const noexcept { return WriteBufferBytes() > 0; }

  core::Result<void> Arm();

  void OnReadable();
  void OnWritable();

  // Idempotent; first reason wins.
  void Close(metrics::CloseReason reason);

 private:
  void TryDrainWrite();
  void MaybePauseReading();
  void MaybeResumeReading();
  bool EnforceWriteHardLimit();
  void DispatchPipelineOutput();
  void RecordReadBufferHighWater();
  void TouchActivity();

  // Lazy poller interest: kReadable iff not paused; kWritable iff write
  // buffer has pending bytes. Avoids spurious POLLOUT storms on level-
  // triggered backends (WSAPoll) for idle connections.
  EventKind DesiredInterest() const noexcept;
  bool SyncPollerInterest();

  Fd fd_;
  uint32_t remote_ipv4_;
  uint16_t remote_port_;
  uint64_t client_id_;
  Poller& poller_;
  ConnectionConfig config_;
  NetMetrics& metrics_;
  core::SteadyClockFn clock_;

  std::unique_ptr<resp::RequestPipeline> pipeline_;

  std::vector<uint8_t> read_buf_;
  std::vector<uint8_t> write_buf_;
  size_t write_pos_ = 0;
  size_t read_buf_high_water_ = 0;

  EventKind armed_interest_ = EventKind::kNone;

  core::SteadyTime last_activity_;
  bool reading_paused_ = false;
  bool close_after_drain_ = false;
  bool closed_ = false;
  std::optional<metrics::CloseReason> close_reason_;
};

}  // namespace abyss::net
