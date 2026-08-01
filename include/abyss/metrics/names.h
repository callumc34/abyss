#pragma once

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace abyss::metrics {

// Closed whitelist of permitted label keys.
enum class LabelKey : uint8_t { kTier, kShard, kCmd, kReason, kStatus, kOp, kProto, kSubject };

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
    case LabelKey::kProto:
      return "proto";
    case LabelKey::kSubject:
      return "subject";
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

enum class FlushReason : uint8_t { kQuiet, kDeadline, kPressure, kDrain };

enum class RequestStatus : uint8_t { kOk, kError, kLoading, kUnknown, kArity, kNoProto };

constexpr std::string_view ToStringView(RequestStatus s) noexcept {
  switch (s) {
    case RequestStatus::kOk:
      return "ok";
    case RequestStatus::kError:
      return "error";
    case RequestStatus::kLoading:
      return "loading";
    case RequestStatus::kUnknown:
      return "unknown";
    case RequestStatus::kArity:
      return "arity";
    case RequestStatus::kNoProto:
      return "noproto";
  }
  return {};
}

// Bounded to {2, 3}; RESP3 is rejected in Phase 1 but still observed.
struct ProtoLabel {
  uint8_t version;
};

constexpr std::string_view ToStringView(FlushReason r) noexcept {
  switch (r) {
    case FlushReason::kQuiet:
      return "quiet";
    case FlushReason::kDeadline:
      return "deadline";
    case FlushReason::kPressure:
      return "pressure";
    case FlushReason::kDrain:
      return "drain";
  }
  return {};
}

// Why the cold consumer's drain/flush loop is backing off rather than making
// progress. kIdle = nothing to drain or flush; kPoisoned = a terminal apply
// failure pinned the batch; kBackpressure = the cold store is unwritable /
// the durable gate is not yet satisfied.
enum class BackoffReason : uint8_t { kIdle, kPoisoned, kBackpressure };

constexpr std::string_view ToStringView(BackoffReason r) noexcept {
  switch (r) {
    case BackoffReason::kIdle:
      return "idle";
    case BackoffReason::kPoisoned:
      return "poisoned";
    case BackoffReason::kBackpressure:
      return "backpressure";
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

// TTL-scanner subject: which type of record was sampled.
enum class TtlSubject : uint8_t { kString, kCollection };

constexpr std::string_view ToStringView(TtlSubject s) noexcept {
  switch (s) {
    case TtlSubject::kString:
      return "string";
    case TtlSubject::kCollection:
      return "collection";
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
struct LabelKeyOf<BackoffReason> {
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
template <>
struct LabelKeyOf<RequestStatus> {
  static constexpr LabelKey value = LabelKey::kStatus;
};
template <>
struct LabelKeyOf<ProtoLabel> {
  static constexpr LabelKey value = LabelKey::kProto;
};
template <>
struct LabelKeyOf<TtlSubject> {
  static constexpr LabelKey value = LabelKey::kSubject;
};

// Produces the string value written into a Prometheus series for a typed
// label value. Registration-time only; never called on the hot path.
inline std::string ToLabelString(Tier t) { return std::string(ToStringView(t)); }
inline std::string ToLabelString(FlushStatus s) { return std::string(ToStringView(s)); }
inline std::string ToLabelString(FlushReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(BackoffReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(CloseReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(RejectReason r) { return std::string(ToStringView(r)); }
inline std::string ToLabelString(CmdLabel c) { return std::string(c.value); }
inline std::string ToLabelString(ShardLabel s) { return std::to_string(s.id); }
inline std::string ToLabelString(RequestStatus s) { return std::string(ToStringView(s)); }
inline std::string ToLabelString(ProtoLabel p) { return std::to_string(p.version); }
inline std::string ToLabelString(TtlSubject s) { return std::string(ToStringView(s)); }

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

// Sub-100us resolution is load-bearing: the hot-read (<100us), hot-write (<50us) and
// buffer-read (<50us) targets are unresolvable by histogram_quantile without boundaries
// below them. Upper decades stay coarse so cold (5ms) and queue-append remain resolvable
// without per-histogram bucket proliferation.
inline constexpr std::array<double, 14> kLatencySeconds{0.00001, 0.000025, 0.00005, 0.0001, 0.00025,
                                                        0.0005,  0.001,    0.0025,  0.005,  0.01,
                                                        0.05,    0.1,      1.0,     10.0};

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

inline constexpr CounterDesc<CmdLabel, RequestStatus> kRespRequestsTotal{
    .name = "abyss_resp_requests_total",
    .help = "RESP requests by command and outcome.",
};

inline constexpr CounterDesc<> kRespParseErrorsTotal{
    .name = "abyss_resp_parse_errors_total",
    .help = "RESP parse errors.",
};

inline constexpr CounterDesc<ProtoLabel> kRespProtocolVersionTotal{
    .name = "abyss_resp_protocol_version_total",
    .help = "HELLO handshakes by negotiated protocol version.",
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

inline constexpr GaugeDesc<> kFsDurableDirSupported{
    .name = "abyss_fs_durable_dir_supported",
    .help =
        "1 if the data volume can make directory entries durable (fsync), else 0. A 0 on a "
        "durability-required deployment is a refuse-to-start condition.",
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

inline constexpr GaugeDesc<> kHotMaxMemoryBytes{
    .name = "abyss_hot_max_memory_bytes",
    .help = "Configured hot store memory budget in bytes; 0 means unlimited.",
};

inline constexpr GaugeDesc<> kHotAccessBufferDepth{
    .name = "abyss_hot_access_buffer_depth",
    .help = "Total depth of the per-shard deferred read-access refresh buffers.",
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

inline constexpr CounterDesc<> kQueueReaperFailuresTotal{
    .name = "abyss_queue_reaper_failures_total",
    .help = "Segment removals the reaper could not complete; retention reclamation is stalled.",
};

inline constexpr GaugeDesc<> kQueueOldestEligibleUnreapedAgeSeconds{
    .name = "abyss_queue_oldest_eligible_unreaped_age_seconds",
    .help = "Age of the oldest reap-eligible segment still on disk; rises when reaping stalls.",
};

inline constexpr GaugeDesc<> kColdBufferEntries{
    .name = "abyss_cold_buffer_entries",
    .help = "Number of keys in the compaction buffer.",
};

inline constexpr GaugeDesc<> kColdBufferBytes{
    .name = "abyss_cold_buffer_bytes",
    .help = "Estimated memory usage of the compaction buffer.",
};

inline constexpr GaugeDesc<> kColdFlushHeapDepth{
    .name = "abyss_cold_flush_heap_depth",
    .help = "Live entries in the compaction buffer's flush heap; surfaces heap growth.",
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

inline constexpr CounterDesc<> kWalDecodeCorruptionTotal{
    .name = "abyss_wal_decode_corruption_total",
    .help =
        "CRC-valid WAL entries that failed structural decode during recovery. Genuine corruption "
        "of durably-acked data; recovery fail-stops (never truncates) so acked data is not "
        "silently discarded.",
};

inline constexpr CounterDesc<FlushStatus> kColdFlushTotal{
    .name = "abyss_cold_flush_total",
    .help = "Cold consumer flush operations by outcome.",
};

inline constexpr CounterDesc<FlushReason> kColdFlushReasonTotal{
    .name = "abyss_cold_flush_reason_total",
    .help = "Cold consumer flush operations by trigger reason.",
};

inline constexpr CounterDesc<FlushStatus> kColdCheckpointTotal{
    .name = "abyss_cold_checkpoint_total",
    .help = "Cold-store durable checkpoints (FlushWAL sync=true) by outcome.",
};

inline constexpr HistogramDesc<> kColdCheckpointDurationSeconds{
    .name = "abyss_cold_checkpoint_duration_seconds",
    .help = "Cold-store checkpoint (durable WAL fsync) latency.",
    .buckets = buckets::kLatencySeconds,
};

inline constexpr GaugeDesc<> kColdCheckpointIntervalSeconds{
    .name = "abyss_cold_checkpoint_interval_seconds",
    .help =
        "Observed wall interval between cold-store checkpoints; surfaces the bounded "
        "checkpoint cadence so its fsync cost is not a hidden knob.",
};

inline constexpr CounterDesc<BackoffReason> kColdConsumerBackoffTotal{
    .name = "abyss_cold_consumer_backoff_total",
    .help = "Cold consumer loop backoff events by reason (idle, poisoned, backpressure).",
};

inline constexpr CounterDesc<> kColdUnsupportedOpTotal{
    .name = "abyss_cold_unsupported_op_total",
    .help =
        "WAL writes whose command has no parser in this build. Skipped, not quarantined: no tier "
        "materialised them, so hot and cold do not diverge. A rising value means the command "
        "surface advertises more than the storage layer implements.",
};

inline constexpr CounterDesc<> kColdParsePoisonTotal{
    .name = "abyss_cold_parse_poison_total",
    .help =
        "Structurally-undecodable WAL ops the cold consumer could not materialise. Each pins WAL "
        "retention below the poison seq for the shard until operator intervention (fail-closed).",
};

// Tier domain for this metric is limited to {kHot, kCold}; kBuffer is invalid.
inline constexpr CounterDesc<Tier> kTtlExpiredTotal{
    .name = "abyss_ttl_expired_total",
    .help = "TTL expirations by tier.",
};

inline constexpr CounterDesc<> kEvictedTotal{
    .name = "abyss_evicted_total",
    .help = "Keys evicted from the hot store by eviction deadline (tier transition).",
};

inline constexpr CounterDesc<> kHotMemoryEvictedTotal{
    .name = "abyss_hot_memory_evicted_total",
    .help = "Keys evicted from the hot store under memory pressure (LRU tier transition).",
};

inline constexpr CounterDesc<> kHotAccessBufferDroppedTotal{
    .name = "abyss_hot_access_buffer_dropped_total",
    .help = "Deferred read-access refreshes dropped past the access-buffer high-water cap.",
};

inline constexpr CounterDesc<> kHotTombstonesReclaimedTotal{
    .name = "abyss_hot_tombstones_reclaimed_total",
    .help = "Delete tombstones reclaimed from the hot store after the cold consumer caught up.",
};

inline constexpr CounterDesc<> kPromotionsTotal{
    .name = "abyss_promotions_total",
    .help = "Cold hits promoted back to the hot store.",
};

// A cold collection scan (SMEMBERS/ZRANGE/HGETALL/HKEYS/HVALS) exceeded the
// configured cold_scan_deadline and was failed closed with a deadline error
// rather than returning a silently truncated result. A rising rate means a
// legitimately large collection is being capped — operators tune
// cold_scan_deadline. See docs/operations/failure-modes.md (decision 4 /
// invariant 5).
inline constexpr CounterDesc<> kColdScanDeadlineExceededTotal{
    .name = "abyss_cold_scan_deadline_exceeded_total",
    .help = "Cold collection scans aborted because they exceeded the cold-scan deadline.",
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

// --- Cold TTL active expiry ------------------------------------------------

inline constexpr CounterDesc<TtlSubject> kColdTtlSamplesTotal{
    .name = "abyss_cold_ttl_samples_total",
    .help = "Random samples drawn by the cold TTL scanner, by subject type.",
};

inline constexpr CounterDesc<TtlSubject> kColdTtlWithTtlTotal{
    .name = "abyss_cold_ttl_with_ttl_total",
    .help = "Sampled records that carried a TTL flag, by subject type.",
};

inline constexpr CounterDesc<TtlSubject> kColdTtlExpiredTotal{
    .name = "abyss_cold_ttl_expired_total",
    .help = "Sampled records that were past their TTL, by subject type.",
};

inline constexpr CounterDesc<TtlSubject> kColdTtlDeletedTotal{
    .name = "abyss_cold_ttl_deleted_total",
    .help = "Records deleted by the cold TTL scanner, by subject type.",
};

inline constexpr CounterDesc<TtlSubject> kColdTtlConflictsTotal{
    .name = "abyss_cold_ttl_conflicts_total",
    .help = "Cold TTL expiry attempts aborted by a concurrent writer (CAS).",
};

inline constexpr GaugeDesc<> kColdTtlIntervalMs{
    .name = "abyss_cold_ttl_interval_ms",
    .help = "Current sleep interval between cold TTL scanner ticks.",
};

inline constexpr GaugeDesc<> kColdTtlSampleSize{
    .name = "abyss_cold_ttl_sample_size",
    .help = "Current sample size for each cold TTL scanner tick.",
};

inline constexpr GaugeDesc<> kColdTtlRateMultiplier{
    .name = "abyss_cold_ttl_rate_multiplier",
    .help = "Adaptive rate multiplier driving the cold TTL scanner cadence.",
};

inline constexpr GaugeDesc<> kColdTtlCpuFraction{
    .name = "abyss_cold_ttl_cpu_fraction",
    .help = "EWMA of the cold TTL scanner thread CPU as a fraction of wall time.",
};

inline constexpr GaugeDesc<> kColdTtlDiskPressureFraction{
    .name = "abyss_cold_ttl_disk_pressure_fraction",
    .help = "Fraction of the cold-store filesystem in use, observed by the TTL scanner.",
};

inline constexpr GaugeDesc<> kColdTtlDiskPressureActive{
    .name = "abyss_cold_ttl_disk_pressure_active",
    .help = "Whether the cold TTL scanner is in disk-pressure mode (1) or not (0).",
};

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

// Encoded as the underlying value of engine::RecoverySnapshot::Phase:
// 0=queue_open, 1=resolver_replay, 2=cold_hot_replay, 3=complete.
inline constexpr GaugeDesc<> kRecoveryPhase{
    .name = "abyss_recovery_phase",
    .help =
        "Current recovery phase (0=queue_open, 1=resolver_replay, "
        "2=cold_hot_replay, 3=complete).",
};

// Per-consumer counters live under separate metric names rather than a Tier
// label because the resolver is not a tier and the closed Tier enum should
// not be widened just for recovery observability.
inline constexpr GaugeDesc<> kRecoveryResolverEntriesReplayed{
    .name = "abyss_recovery_resolver_entries_replayed",
    .help = "Live entries scanned by the resolver during recovery, all shards.",
};

inline constexpr GaugeDesc<> kRecoveryResolverEntriesTarget{
    .name = "abyss_recovery_resolver_entries_target",
    .help = "Total entries the resolver must scan during recovery, all shards.",
};

// Conditional-write durability pressure: the resolver could not confirm a
// self-emitted Resolved's WAL fsync within its budget before fulfilling the
// client (steady state) or before advancing the recovery ack past a re-decided
// dangling. A non-zero rate means conditional acks are stalling/erroring on
// durable-layer latency, not silently losing writes (the Resolved stays in the
// WAL and applies on catch-up). Surfaces XDUR-2 / HOTC-5 (no silent degradation).
inline constexpr CounterDesc<> kResolverDurableWaitTimeoutsTotal{
    .name = "abyss_resolver_durable_wait_timeouts_total",
    .help =
        "Resolver self-emitted Resolved durability waits that timed out before "
        "the conditional ack (steady-state) or the recovery ack barrier.",
};

inline constexpr GaugeDesc<> kRecoveryColdEntriesReplayed{
    .name = "abyss_recovery_cold_entries_replayed",
    .help = "Live entries drained by the cold consumer during recovery, all shards.",
};

inline constexpr GaugeDesc<> kRecoveryColdEntriesTarget{
    .name = "abyss_recovery_cold_entries_target",
    .help = "Total entries the cold consumer must drain during recovery, all shards.",
};

inline constexpr GaugeDesc<> kRecoveryHotEntriesReplayed{
    .name = "abyss_recovery_hot_entries_replayed",
    .help = "Live entries settled by the hot consumer during recovery, all shards.",
};

inline constexpr GaugeDesc<> kRecoveryHotEntriesTarget{
    .name = "abyss_recovery_hot_entries_target",
    .help = "Total entries the hot consumer must settle during recovery, all shards.",
};

inline constexpr GaugeDesc<> kRecoveryDurationSeconds{
    .name = "abyss_recovery_duration_seconds",
    .help = "Wall-clock elapsed time for the current recovery run.",
};

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Encoded as the underlying value of server::Server::LifecycleState:
// 0=initializing, 1=recovering, 2=serving, 3=draining, 4=stopped. The
// client-visible LOADING gate is asserted in every state except serving, so
// this is the single source of truth for "is the data plane open".
inline constexpr GaugeDesc<> kServerLifecycleState{
    .name = "abyss_server_lifecycle_state",
    .help =
        "Current server lifecycle state (0=initializing, 1=recovering, "
        "2=serving, 3=draining, 4=stopped).",
};

// A graceful shutdown drain that hit its deadline before the cold buffer
// emptied: the remaining slice is left in the WAL for replay (correctness
// preserved). A non-zero count means the shutdown_grace budget was too small
// for the buffered work — surfaced rather than silently truncated.
inline constexpr CounterDesc<> kColdDrainTruncatedTotal{
    .name = "abyss_cold_drain_truncated_total",
    .help =
        "Cold consumer graceful drains that hit the shutdown deadline before the "
        "buffer emptied; the remaining slice replays from the WAL on next start.",
};

}  // namespace names

}  // namespace abyss::metrics
