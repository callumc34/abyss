#pragma once

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace abyss::metrics {

// Closed whitelist of permitted label keys.
enum class LabelKey : uint8_t { kTier, kShard, kCmd, kReason, kStatus, kOp };

constexpr std::string_view ToStringView(LabelKey k) noexcept {
  switch (k) {
    case LabelKey::kTier:
      return "tier";
    case LabelKey::kShard:
      return "shard";
    case LabelKey::kCmd:
      return "cmd";
    case LabelKey::kReason:
      return "reason";
    case LabelKey::kStatus:
      return "status";
    case LabelKey::kOp:
      return "op";
  }
  return {};
}

enum class Tier : uint8_t { kHot, kBuffer, kCold };

constexpr std::string_view ToStringView(Tier t) noexcept {
  switch (t) {
    case Tier::kHot:
      return "hot";
    case Tier::kBuffer:
      return "buffer";
    case Tier::kCold:
      return "cold";
  }
  return {};
}

enum class FlushStatus : uint8_t { kSuccess, kFailure };

constexpr std::string_view ToStringView(FlushStatus s) noexcept {
  switch (s) {
    case FlushStatus::kSuccess:
      return "success";
    case FlushStatus::kFailure:
      return "failure";
  }
  return {};
}

enum class FlushReason : uint8_t { kQuiet, kDeadline, kPressure };

constexpr std::string_view ToStringView(FlushReason r) noexcept {
  switch (r) {
    case FlushReason::kQuiet:
      return "quiet";
    case FlushReason::kDeadline:
      return "deadline";
    case FlushReason::kPressure:
      return "pressure";
  }
  return {};
}

enum class CloseReason : uint8_t {
  kClient,
  kIdle,
  kOversize,
  kBackpressure,
  kServerShutdown,
};

constexpr std::string_view ToStringView(CloseReason r) noexcept {
  switch (r) {
    case CloseReason::kClient:
      return "client";
    case CloseReason::kIdle:
      return "idle";
    case CloseReason::kOversize:
      return "oversize";
    case CloseReason::kBackpressure:
      return "backpressure";
    case CloseReason::kServerShutdown:
      return "server_shutdown";
  }
  return {};
}

enum class RejectReason : uint8_t {
  kMaxConnections,
  kBindFamily,
};

constexpr std::string_view ToStringView(RejectReason r) noexcept {
  switch (r) {
    case RejectReason::kMaxConnections:
      return "max_connections";
    case RejectReason::kBindFamily:
      return "bind_family";
  }
  return {};
}

// Command-name label value. Values are expected to be views into the command
// registry; never client-supplied strings.
struct CmdLabel {
  std::string_view value;
};

// Shard-id label value. Formatted to string once at registration.
struct ShardLabel {
  uint32_t id;
};

// Maps a label-value type to its label key. Specialisations are the closed
// set of valid label-value types; any other type fails to compile against the
// descriptor templates below.
template <class T>
struct LabelKeyOf;

template <>
struct LabelKeyOf<Tier> {
  static constexpr LabelKey value = LabelKey::kTier;
};
template <>
struct LabelKeyOf<FlushStatus> {
  static constexpr LabelKey value = LabelKey::kStatus;
};
template <>
struct LabelKeyOf<FlushReason> {
  static constexpr LabelKey value = LabelKey::kReason;
};
template <>
struct LabelKeyOf<CloseReason> {
  static constexpr LabelKey value = LabelKey::kReason;
};
template <>
struct LabelKeyOf<RejectReason> {
  static constexpr LabelKey value = LabelKey::kReason;
};
template <>
struct LabelKeyOf<CmdLabel> {
  static constexpr LabelKey value = LabelKey::kCmd;
};
template <>
struct LabelKeyOf<ShardLabel> {
  static constexpr LabelKey value = LabelKey::kShard;
};

// Produces the string value written into a Prometheus series for a typed
// label value. Registration-time only; never called on the hot path.
inline std::string ToLabelString(Tier t) { return std::string(ToStringView(t)); }
inline std::string ToLabelString(FlushStatus s) { return std::string(ToStringView(s)); }
inline std::string ToLabelString(FlushReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(CloseReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(RejectReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(CmdLabel c) { return std::string(c.value); }
inline std::string ToLabelString(ShardLabel s) { return std::to_string(s.id); }

template <class... Ts>
struct CounterDesc {
  std::string_view name;
  std::string_view help;

  static constexpr size_t Arity() noexcept { return sizeof...(Ts); }
};

template <class... Ts>
struct GaugeDesc {
  std::string_view name;
  std::string_view help;

  static constexpr size_t Arity() noexcept { return sizeof...(Ts); }
};

template <class... Ts>
struct HistogramDesc {
  std::string_view name;
  std::string_view help;
  std::span<const double> buckets;

  static constexpr size_t Arity() noexcept { return sizeof...(Ts); }
};

namespace buckets {

// Six decadal buckets from 100us to 10s. Covers the whole latency spectrum
// we care about end-to-end without over-binning hot ranges.
inline constexpr std::array<double, 6> kLatencySeconds{0.0001, 0.001, 0.01, 0.1, 1.0, 10.0};

// Power-of-ten buckets for batch sizes.
inline constexpr std::array<double, 5> kBatchSize{1, 10, 100, 1000, 10000};

}  // namespace buckets

namespace names {

inline constexpr HistogramDesc<CmdLabel> kHotOpDurationSeconds{
    .name = "abyss_hot_op_duration_seconds",
    .help = "Hot store operation latency per command.",
    .buckets = buckets::kLatencySeconds,
};

inline constexpr HistogramDesc<CmdLabel> kColdOpDurationSeconds{
    .name = "abyss_cold_op_duration_seconds",
    .help = "Cold store operation latency per command.",
    .buckets = buckets::kLatencySeconds,
};

inline constexpr HistogramDesc<CmdLabel> kBufferOpDurationSeconds{
    .name = "abyss_buffer_op_duration_seconds",
    .help = "Compaction buffer read latency per command.",
    .buckets = buckets::kLatencySeconds,
};

inline constexpr HistogramDesc<CmdLabel> kRespRequestDurationSeconds{
    .name = "abyss_resp_request_duration_seconds",
    .help = "End-to-end RESP request latency per command.",
    .buckets = buckets::kLatencySeconds,
};

inline constexpr HistogramDesc<> kQueueAppendDurationSeconds{
    .name = "abyss_queue_append_duration_seconds",
    .help = "Queue append latency including group-commit fsync.",
    .buckets = buckets::kLatencySeconds,
};

inline constexpr HistogramDesc<> kColdFlushBatchSize{
    .name = "abyss_cold_flush_batch_size",
    .help = "Number of keys per cold flush batch.",
    .buckets = buckets::kBatchSize,
};

inline constexpr GaugeDesc<> kHotConsumerLagEntries{
    .name = "abyss_hot_consumer_lag_entries",
    .help = "Entries between hot consumer position and queue head.",
};

inline constexpr GaugeDesc<> kColdConsumerLagEntries{
    .name = "abyss_cold_consumer_lag_entries",
    .help = "Entries between cold consumer position and queue head.",
};

inline constexpr GaugeDesc<> kColdBufferOldestEntryAgeSeconds{
    .name = "abyss_cold_buffer_oldest_entry_age_seconds",
    .help = "Age of the oldest un-flushed compaction-buffer entry.",
};

inline constexpr GaugeDesc<> kHotConsumerSeq{
    .name = "abyss_hot_consumer_seq",
    .help = "Hot consumer current sequence position.",
};

inline constexpr GaugeDesc<> kColdConsumerSeq{
    .name = "abyss_cold_consumer_seq",
    .help = "Cold consumer current sequence position.",
};

inline constexpr GaugeDesc<> kHotMemoryBytes{
    .name = "abyss_hot_memory_bytes",
    .help = "Hot store memory usage in bytes.",
};

inline constexpr GaugeDesc<> kHotKeys{
    .name = "abyss_hot_keys",
    .help = "Number of keys in the hot store.",
};

inline constexpr GaugeDesc<> kColdDiskBytes{
    .name = "abyss_cold_disk_bytes",
    .help = "Cold store disk usage in bytes.",
};

inline constexpr GaugeDesc<> kColdKeys{
    .name = "abyss_cold_keys",
    .help = "Number of keys in the cold store.",
};

inline constexpr GaugeDesc<> kQueueDepth{
    .name = "abyss_queue_depth",
    .help = "Number of entries in the queue.",
};

inline constexpr GaugeDesc<> kQueueDiskBytes{
    .name = "abyss_queue_disk_bytes",
    .help = "Queue WAL disk usage in bytes.",
};

inline constexpr GaugeDesc<> kColdBufferEntries{
    .name = "abyss_cold_buffer_entries",
    .help = "Number of keys in the compaction buffer.",
};

inline constexpr GaugeDesc<> kColdBufferBytes{
    .name = "abyss_cold_buffer_bytes",
    .help = "Estimated memory usage of the compaction buffer.",
};

inline constexpr CounterDesc<Tier> kHitsTotal{
    .name = "abyss_hits_total",
    .help = "Read hits by tier.",
};

inline constexpr CounterDesc<> kMissesTotal{
    .name = "abyss_misses_total",
    .help = "Read misses across all tiers.",
};

inline constexpr CounterDesc<> kQueueAppendedTotal{
    .name = "abyss_queue_appended_total",
    .help = "Total entries appended to the queue.",
};

inline constexpr CounterDesc<FlushStatus> kColdFlushTotal{
    .name = "abyss_cold_flush_total",
    .help = "Cold consumer flush operations by outcome.",
};

inline constexpr CounterDesc<FlushReason> kColdFlushReasonTotal{
    .name = "abyss_cold_flush_reason_total",
    .help = "Cold consumer flush operations by trigger reason.",
};

// Tier domain for this metric is limited to {kHot, kCold}; kBuffer is invalid.
inline constexpr CounterDesc<Tier> kTtlExpiredTotal{
    .name = "abyss_ttl_expired_total",
    .help = "TTL expirations by tier.",
};

inline constexpr CounterDesc<> kEvictedTotal{
    .name = "abyss_evicted_total",
    .help = "Keys evicted from the hot store.",
};

inline constexpr CounterDesc<> kPromotionsTotal{
    .name = "abyss_promotions_total",
    .help = "Cold hits promoted back to the hot store.",
};

inline constexpr GaugeDesc<> kNetConnectionsActive{
    .name = "abyss_net_connections_active",
    .help = "Currently open TCP connections.",
};

inline constexpr CounterDesc<> kNetConnectionsAcceptedTotal{
    .name = "abyss_net_connections_accepted_total",
    .help = "TCP connections accepted since startup.",
};

inline constexpr CounterDesc<CloseReason> kNetConnectionsClosedTotal{
    .name = "abyss_net_connections_closed_total",
    .help = "TCP connections closed by server-observed reason.",
};

inline constexpr CounterDesc<RejectReason> kNetConnectionsRejectedTotal{
    .name = "abyss_net_connections_rejected_total",
    .help = "TCP accept attempts rejected before becoming a connection.",
};

inline constexpr CounterDesc<> kNetBytesInTotal{
    .name = "abyss_net_bytes_in_total",
    .help = "Bytes received from clients.",
};

inline constexpr CounterDesc<> kNetBytesOutTotal{
    .name = "abyss_net_bytes_out_total",
    .help = "Bytes sent to clients.",
};

inline constexpr GaugeDesc<> kNetReadBufferHighWaterBytes{
    .name = "abyss_net_read_buffer_high_water_bytes",
    .help = "Largest read-buffer size observed across active connections.",
};

inline constexpr GaugeDesc<> kNetBackpressureActive{
    .name = "abyss_net_backpressure_active",
    .help = "Connections currently paused for write back-pressure.",
};

inline constexpr CounterDesc<> kNetBackpressureEnteredTotal{
    .name = "abyss_net_backpressure_entered_total",
    .help = "Times a connection entered the back-pressure paused state.",
};

inline constexpr CounterDesc<> kNetBackpressureExitedTotal{
    .name = "abyss_net_backpressure_exited_total",
    .help = "Times a connection exited the back-pressure paused state.",
};

}  // namespace names

}  // namespace abyss::metrics
