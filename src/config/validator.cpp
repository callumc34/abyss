#include "validator.h"

#include <algorithm>
#include <bit>
#include <chrono>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"

namespace abyss::config::internal {

namespace {

// WAL framing constants the validator needs to floor
// segment_size_bytes, kept local so the config library does not depend
// on the queue library's private headers.
//   kSegmentHeaderSize: the 4 KiB header block, so frames start
//     page-aligned (kLogSegmentHeaderBytes).
//   kMaxEntryEnvelope: a generous bound on a frame's overhead (commit
//     word, CRC, frame header, command framing, padding). A segment
//     holds its header plus one max-size frame.
//   kFrameAlign: frames are 8-byte aligned, and so is a segment.
//   kMaxFrameSpace: frame offsets within a segment are 32-bit.
constexpr size_t kSegmentHeaderSize = 4096;
constexpr size_t kMaxEntryEnvelope = 1024;
constexpr size_t kFrameAlign = 8;
constexpr uint64_t kMaxFrameSpace = 0xFFFFFFFFULL;
constexpr uint32_t kMinRingEntries = uint32_t{1} << 12;
constexpr uint32_t kMaxRingEntries = uint32_t{1} << 24;
// Redis proto-max-bulk-len: the largest single value we ever accept.
constexpr size_t kMaxAcceptableValueSize = size_t{512} * 1024 * 1024;
// Upper bound on the cold checkpoint cadence. The cold commit cannot pass data the
// last checkpoint did not make durable, so this bounds how far the commit — and
// therefore WAL retention release — can trail the applied frontier.
constexpr std::chrono::milliseconds kMaxCheckpointMinInterval{60000};
// Below the floor the offset fsync dominates the disk; above the ceiling a
// crash replays, and retention trails, by more than a minute of commits.
constexpr std::chrono::milliseconds kMinOffsetFsyncInterval{10};
constexpr std::chrono::milliseconds kMaxOffsetFsyncInterval{60000};
// Below the floor the window admits too little to batch a flush; above
// the ceiling a power loss under process_crash loses more than operators
// size for.
constexpr uint64_t kMinDurabilityWindowBytes = uint64_t{1} << 20;
constexpr uint64_t kMaxDurabilityWindowBytes = uint64_t{4} << 30;
constexpr std::chrono::milliseconds kMinDurabilityWindow{10};
constexpr std::chrono::milliseconds kMaxDurabilityWindow{60000};

// Snapshot gauges scraped less often than this stop being an alerting signal:
// Prometheus would sample a value already stale by more than a scrape interval.
constexpr std::chrono::milliseconds kMaxMetricsSnapshotInterval{60000};

core::Error InvalidArg(std::string path, std::string_view message) {
  std::string msg = std::move(path);
  msg += ": ";
  msg += message;
  return {core::ErrorCode::kInvalidArgument, std::move(msg)};
}

bool OneOf(std::string_view value, std::initializer_list<std::string_view> allowed) {
  return std::ranges::find(allowed, value) != allowed.end();
}

core::Result<void> RequireNonEmpty(std::string path, std::string_view value) {
  if (value.empty()) return std::unexpected(InvalidArg(std::move(path), "must not be empty"));
  return {};
}

core::Result<void> RequirePositive(std::string path, size_t value) {
  if (value == 0) return std::unexpected(InvalidArg(std::move(path), "must be > 0"));
  return {};
}

core::Result<void> RequirePositive(std::string path, uint32_t value) {
  if (value == 0) return std::unexpected(InvalidArg(std::move(path), "must be > 0"));
  return {};
}

core::Result<void> RequirePositive(std::string path, std::chrono::seconds value) {
  if (value.count() <= 0)
    return std::unexpected(InvalidArg(std::move(path), "must be > 0 seconds"));
  return {};
}

core::Result<void> ValidateHot(const HotConfig& hot) {
  if (auto r = RequireNonEmpty("hot.backend", hot.backend); !r) return r;
  if (auto r = RequirePositive("hot.max_memory_bytes", hot.max_memory_bytes); !r) return r;
  if (auto r = RequirePositive("hot.shard_count", hot.shard_count); !r) return r;
  if (hot.shard_count > core::kRpcMaxShardCount) {
    return std::unexpected(
        InvalidArg("hot.shard_count", "must be <= " + std::to_string(core::kRpcMaxShardCount) +
                                          " (RpcId packing reserves bit 63 for the flush tag; "
                                          "ADP-011 inv 6 / ENGINE-4)"));
  }
  if (hot.eviction_tick.count() <= 0) {
    return std::unexpected(InvalidArg("hot.eviction_tick_ms", "must be > 0 milliseconds"));
  }

  std::vector<core::EvictionRule> rules;
  rules.reserve(hot.eviction_overrides.size());
  for (const auto& o : hot.eviction_overrides) {
    rules.push_back({.prefix = o.prefix, .eviction = o.eviction});
  }
  return core::EvictionPolicy::Validate(hot.default_eviction, rules, "hot");
}

core::Result<void> ValidateColdTtlScanner(const cold::TtlScanner::Config& s) {
  if (s.high_threshold < 0.0 || s.high_threshold > 1.0) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.high_threshold", "must be in [0.0, 1.0]"));
  }
  if (s.low_threshold < 0.0 || s.low_threshold > 1.0) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.low_threshold", "must be in [0.0, 1.0]"));
  }
  if (s.high_threshold <= s.low_threshold) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.high_threshold",
                                      "must be strictly greater than low_threshold"));
  }
  if (s.disk_pressure_threshold < 0.0 || s.disk_pressure_threshold > 1.0) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.disk_pressure_threshold", "must be in [0.0, 1.0]"));
  }
  if (s.disk_pressure_release_threshold < 0.0 || s.disk_pressure_release_threshold > 1.0) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.disk_pressure_release_threshold", "must be in [0.0, 1.0]"));
  }
  if (s.disk_pressure_release_threshold > s.disk_pressure_threshold) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.disk_pressure_release_threshold",
                                      "must be <= disk_pressure_threshold"));
  }
  if (s.base_sample_size == 0 || s.min_sample_size == 0) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.base_sample_size", "must be > 0"));
  }
  if (s.min_sample_size > s.base_sample_size || s.base_sample_size > s.max_sample_size) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.base_sample_size",
                   "must satisfy min_sample_size <= base_sample_size <= max_sample_size"));
  }
  if (s.min_interval.count() <= 0 || s.base_interval.count() <= 0 || s.max_interval.count() <= 0) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.base_interval_ms", "must be > 0"));
  }
  if (s.min_interval > s.base_interval || s.base_interval > s.max_interval) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.base_interval_ms",
                   "must satisfy min_interval_ms <= base_interval_ms <= max_interval_ms"));
  }
  if (s.rate_increase_factor <= 1.0) {
    return std::unexpected(InvalidArg("cold.ttl_scanner.rate_increase_factor", "must be > 1.0"));
  }
  if (s.rate_decrease_factor <= 0.0 || s.rate_decrease_factor >= 1.0) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.rate_decrease_factor", "must be in (0.0, 1.0)"));
  }
  if (s.max_cpu_fraction <= 0.0 || s.max_cpu_fraction > 1.0) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.max_cpu_fraction", "must be in (0.0, 1.0]"));
  }
  if (s.cpu_ewma_window.count() <= 0) {
    return std::unexpected(
        InvalidArg("cold.ttl_scanner.cpu_ewma_window_seconds", "must be > 0 seconds"));
  }
  return {};
}

core::Result<void> ValidateCold(const ColdConfig& cold) {
  if (auto r = RequireNonEmpty("cold.backend", cold.backend); !r) return r;
  if (auto r = RequireNonEmpty("cold.data_path", cold.data_path); !r) return r;
  if (auto r = RequirePositive("cold.write_buffer_size_bytes", cold.write_buffer_size_bytes); !r)
    return r;
  if (auto r = ValidateColdTtlScanner(cold.ttl_scanner); !r) return r;
  return {};
}

core::Result<void> ValidateQueue(const QueueConfig& q) {
  if (auto r = RequireNonEmpty("queue.backend", q.backend); !r) return r;
  if (auto r = RequireNonEmpty("queue.wal_path", q.wal_path); !r) return r;
  if (auto r = RequirePositive("queue.segment_size_bytes", q.segment_size_bytes); !r) return r;

  // A segment below the header size underflows the capacity subtractions and
  // wedges the shard in an infinite rotation loop (QUEUE-4). Floor it above the
  // header plus a minimal entry envelope.
  const size_t segment_floor = kSegmentHeaderSize + kMaxEntryEnvelope;
  if (q.segment_size_bytes < segment_floor) {
    return std::unexpected(InvalidArg(
        "queue.segment_size_bytes",
        "must be >= " + std::to_string(segment_floor) + " (segment header + minimum entry)"));
  }
  if (q.segment_size_bytes % kFrameAlign != 0 ||
      q.segment_size_bytes - kSegmentHeaderSize > kMaxFrameSpace) {
    return std::unexpected(InvalidArg("queue.segment_size_bytes",
                                      "must be a multiple of 8 below 4 GiB plus its 4 KiB header"));
  }

  // max_value_size_bytes is the single-value ceiling, decoupled from the
  // segment size (G11 / Decision 2). It must be positive, within Redis's
  // proto-max-bulk-len, and small enough that one max-size entry plus its
  // envelope fits a fixed-size segment (no jumbo segments — the reaper,
  // sealed-segment accounting, recovery base_seq math, and disk gauges all rely
  // on uniform segment size).
  if (auto r = RequirePositive("queue.max_value_size_bytes", q.max_value_size_bytes); !r) return r;
  if (q.max_value_size_bytes > kMaxAcceptableValueSize) {
    return std::unexpected(InvalidArg(
        "queue.max_value_size_bytes",
        "must be <= " + std::to_string(kMaxAcceptableValueSize) + " (Redis proto-max-bulk-len)"));
  }
  if (q.segment_size_bytes < kSegmentHeaderSize + q.max_value_size_bytes + kMaxEntryEnvelope) {
    return std::unexpected(InvalidArg("queue.segment_size_bytes",
                                      "must be >= queue.max_value_size_bytes + " +
                                          std::to_string(kSegmentHeaderSize + kMaxEntryEnvelope) +
                                          " so a max-size value fits one fixed-size segment"));
  }

  if (q.log_count == 0 || !std::has_single_bit(q.log_count)) {
    return std::unexpected(InvalidArg("queue.log_count", "must be a power of two"));
  }
  if (q.ring_entries < kMinRingEntries || q.ring_entries > kMaxRingEntries ||
      !std::has_single_bit(q.ring_entries)) {
    return std::unexpected(InvalidArg(
        "queue.ring_entries", "must be a power of two in [" + std::to_string(kMinRingEntries) +
                                  ", " + std::to_string(kMaxRingEntries) + "]"));
  }

  if (q.min_retention.count() < 0)
    return std::unexpected(InvalidArg("queue.min_retention_seconds", "must be >= 0 seconds"));

  if (q.offset_fsync_interval < kMinOffsetFsyncInterval ||
      q.offset_fsync_interval > kMaxOffsetFsyncInterval) {
    return std::unexpected(
        InvalidArg("queue.offset_fsync_interval_ms",
                   "must be in [" + std::to_string(kMinOffsetFsyncInterval.count()) + ", " +
                       std::to_string(kMaxOffsetFsyncInterval.count()) + "] milliseconds"));
  }

  if (q.durability != core::Durability::kProcessCrash &&
      q.durability != core::Durability::kPowerLoss) {
    return std::unexpected(
        InvalidArg("queue.durability", "must be one of: process_crash, power_loss"));
  }
  if (q.durability_window_bytes < kMinDurabilityWindowBytes ||
      q.durability_window_bytes > kMaxDurabilityWindowBytes) {
    return std::unexpected(InvalidArg("queue.durability_window_bytes",
                                      "must be in [" + std::to_string(kMinDurabilityWindowBytes) +
                                          ", " + std::to_string(kMaxDurabilityWindowBytes) + "]"));
  }
  if (q.durability_window < kMinDurabilityWindow || q.durability_window > kMaxDurabilityWindow) {
    return std::unexpected(
        InvalidArg("queue.durability_window_ms",
                   "must be in [" + std::to_string(kMinDurabilityWindow.count()) + ", " +
                       std::to_string(kMaxDurabilityWindow.count()) + "] milliseconds"));
  }
  return {};
}

core::Result<void> ValidateHotConsumer(const HotConsumerConfig& c) {
  if (auto r = RequirePositive("hot_consumer.read_batch_size", c.read_batch_size); !r) return r;
  if (c.read_timeout.count() <= 0) {
    return std::unexpected(InvalidArg("hot_consumer.read_timeout_ms", "must be > 0 milliseconds"));
  }
  return {};
}

core::Result<void> ValidateColdConsumer(const ColdConsumerConfig& c) {
  if (auto r = RequirePositive("cold_consumer.quiet_threshold_seconds", c.quiet_threshold); !r)
    return r;
  if (auto r = RequirePositive("cold_consumer.safety_margin_seconds", c.safety_margin); !r)
    return r;
  if (c.jitter_fraction < 0.0 || c.jitter_fraction > 1.0) {
    return std::unexpected(InvalidArg("cold_consumer.jitter_fraction", "must be in [0.0, 1.0]"));
  }
  if (auto r = RequirePositive("cold_consumer.buffer_high_water_bytes", c.buffer_high_water_bytes);
      !r)
    return r;
  if (c.buffer_low_water_bytes > 0 && c.buffer_low_water_bytes > c.buffer_high_water_bytes) {
    return std::unexpected(
        InvalidArg("cold_consumer.buffer_low_water_bytes", "must be <= buffer_high_water_bytes"));
  }
  if (auto r = RequirePositive("cold_consumer.max_flush_batch_size", c.max_flush_batch_size); !r)
    return r;
  if (auto r = RequirePositive("cold_consumer.queue_read_max_count", c.queue_read_max_count); !r)
    return r;
  if (c.queue_read_timeout.count() <= 0) {
    return std::unexpected(
        InvalidArg("cold_consumer.queue_read_timeout_ms", "must be > 0 milliseconds"));
  }
  if (c.retry_initial_backoff.count() < 0) {
    return std::unexpected(
        InvalidArg("cold_consumer.retry_initial_backoff_ms", "must be >= 0 milliseconds"));
  }
  if (c.retry_max_backoff.count() < 0) {
    return std::unexpected(
        InvalidArg("cold_consumer.retry_max_backoff_ms", "must be >= 0 milliseconds"));
  }
  if (c.retry_initial_backoff > c.retry_max_backoff) {
    return std::unexpected(
        InvalidArg("cold_consumer.retry_initial_backoff_ms", "must be <= retry_max_backoff_ms"));
  }
  // The checkpoint cadence is the cold durability frontier: a zero cadence
  // fsyncs per batch, and a zero interval is not a cadence at all. Both bounds
  // must be positive so the commit can only trail durable data by a bounded amount.
  if (auto r = RequirePositive("cold_consumer.checkpoint_max_flushes", c.checkpoint_max_flushes);
      !r) {
    return r;
  }
  if (c.checkpoint_min_interval.count() <= 0) {
    return std::unexpected(
        InvalidArg("cold_consumer.checkpoint_min_interval_ms", "must be > 0 milliseconds"));
  }
  if (c.checkpoint_min_interval > kMaxCheckpointMinInterval) {
    return std::unexpected(
        InvalidArg("cold_consumer.checkpoint_min_interval_ms",
                   "must be <= " + std::to_string(kMaxCheckpointMinInterval.count()) +
                       " milliseconds (bounds the cold durability lag)"));
  }
  if (c.loop_initial_backoff.count() <= 0) {
    return std::unexpected(
        InvalidArg("cold_consumer.loop_initial_backoff_ms", "must be > 0 milliseconds"));
  }
  if (c.loop_max_backoff.count() <= 0) {
    return std::unexpected(
        InvalidArg("cold_consumer.loop_max_backoff_ms", "must be > 0 milliseconds"));
  }
  if (c.loop_initial_backoff > c.loop_max_backoff) {
    return std::unexpected(
        InvalidArg("cold_consumer.loop_initial_backoff_ms", "must be <= loop_max_backoff_ms"));
  }
  if (c.drain_grace.count() <= 0) {
    return std::unexpected(InvalidArg("cold_consumer.drain_grace_seconds", "must be > 0 seconds"));
  }
  return {};
}

core::Result<void> ValidateConsumerRpc(const ConsumerRpcConfig& c) {
  if (auto r = RequirePositive("consumer_rpc.registry_shard_count", c.registry_shard_count); !r)
    return r;
  if (c.default_timeout.count() <= 0) {
    return std::unexpected(
        InvalidArg("consumer_rpc.default_timeout_ms", "must be > 0 milliseconds"));
  }
  return {};
}

core::Result<void> ValidateEngine(const EngineConfig& e) {
  if (e.write_timeout.count() <= 0) {
    return std::unexpected(InvalidArg("engine.write_timeout_ms", "must be > 0 milliseconds"));
  }
  if (e.min_rpc_wait_fraction <= 0.0 || e.min_rpc_wait_fraction >= 1.0) {
    return std::unexpected(InvalidArg("engine.min_rpc_wait_fraction", "must be in (0.0, 1.0)"));
  }
  if (e.buffer_consistency_wait_timeout.count() <= 0) {
    return std::unexpected(
        InvalidArg("engine.buffer_consistency_wait_timeout_ms", "must be > 0 milliseconds"));
  }
  return {};
}

core::Result<void> ValidateRecovery(const RecoveryConfig& r) {
  if (auto res = RequirePositive("recovery.replay_parallelism", r.replay_parallelism); !res)
    return res;
  if (auto res = RequirePositive("recovery.hot_replay_batch_size", r.hot_replay_batch_size); !res)
    return res;
  if (auto res = RequirePositive("recovery.cold_replay_batch_size", r.cold_replay_batch_size); !res)
    return res;
  return {};
}

core::Result<void> ValidateNet(const NetConfig& n) {
  if (auto res = RequireNonEmpty("net.bind", n.bind); !res) return res;
  // port == 0 requests a kernel-assigned ephemeral port, reported on stdout.
  if (auto res = RequirePositive("net.max_connections", n.max_connections); !res) return res;
  if (auto res = RequirePositive("net.idle_timeout_seconds", n.idle_timeout); !res) return res;
  if (auto res = RequirePositive("net.accept_queue", n.accept_queue); !res) return res;
  // io_threads == 0 means "auto" — TcpServer resolves it at startup.
  if (auto res = RequirePositive("net.max_read_buffer_bytes", n.max_read_buffer_bytes); !res) {
    return res;
  }
  if (auto res = RequirePositive("net.write_backpressure_bytes", n.write_backpressure_bytes);
      !res) {
    return res;
  }
  if (auto res = RequirePositive("net.write_resume_bytes", n.write_resume_bytes); !res) return res;
  if (auto res = RequirePositive("net.write_hard_limit_bytes", n.write_hard_limit_bytes); !res) {
    return res;
  }
  // The pause/resume/hard-limit ordering is the back-pressure invariant. A
  // misconfigured ordering silently disables back-pressure, so reject loudly.
  if (n.write_resume_bytes >= n.write_backpressure_bytes) {
    return std::unexpected(
        InvalidArg("net.write_resume_bytes", "must be < write_backpressure_bytes"));
  }
  if (n.write_backpressure_bytes >= n.write_hard_limit_bytes) {
    return std::unexpected(
        InvalidArg("net.write_backpressure_bytes", "must be < write_hard_limit_bytes"));
  }
  if (n.shutdown_grace.count() <= 0) {
    return std::unexpected(InvalidArg("net.shutdown_grace_seconds", "must be > 0 seconds"));
  }
  if (n.reaper_tick.count() <= 0) {
    return std::unexpected(InvalidArg("net.reaper_tick_ms", "must be > 0 milliseconds"));
  }
  return {};
}

core::Result<void> ValidateMetrics(const MetricsConfig& m) {
  if (auto r = RequireNonEmpty("metrics.bind", m.bind); !r) return r;
  // port == 0 requests an OS-assigned ephemeral port; the listener reports
  // the bound port via the readiness pipe.
  if (m.snapshot_interval <= std::chrono::milliseconds::zero()) {
    return std::unexpected(InvalidArg("metrics.snapshot_interval_ms", "must be > 0"));
  }
  if (m.snapshot_interval > kMaxMetricsSnapshotInterval) {
    return std::unexpected(
        InvalidArg("metrics.snapshot_interval_ms",
                   "must be <= " + std::to_string(kMaxMetricsSnapshotInterval.count()) + " ms"));
  }
  return {};
}

core::Result<void> ValidateLog(const LogConfig& l) {
  if (!OneOf(l.format, {"json", "text"})) {
    return std::unexpected(InvalidArg("log.format", "must be one of: json, text"));
  }
  if (!OneOf(l.sink, {"stdout", "stderr"})) {
    return std::unexpected(InvalidArg("log.sink", "must be one of: stdout, stderr"));
  }
  std::unordered_set<std::string> seen;
  for (size_t i = 0; i < l.component_levels.size(); ++i) {
    const auto& entry = l.component_levels[i];
    std::string base = "log.component_levels[";
    base += std::to_string(i);
    base += ']';
    if (auto r = RequireNonEmpty(base + ".component", entry.component); !r) return r;
    if (!seen.insert(entry.component).second) {
      return std::unexpected(InvalidArg(base + ".component", "duplicate component"));
    }
  }
  return {};
}

core::Result<void> ValidateAdmin(const AdminConfig& a) {
  if (auto r = RequireNonEmpty("admin.bind", a.bind); !r) return r;
  // port == 0 requests an OS-assigned ephemeral port.
  return {};
}

// WAL retention must cover the longest hot-tier residency.
core::Result<void> ValidateRetentionVsEviction(const Config& c) {
  std::vector<core::EvictionRule> rules;
  rules.reserve(c.hot.eviction_overrides.size());
  for (const auto& o : c.hot.eviction_overrides) {
    rules.push_back({.prefix = o.prefix, .eviction = o.eviction});
  }
  const auto needed = core::EvictionPolicy::MaxConfiguredTtl(c.hot.default_eviction, rules);
  if (c.queue.min_retention < needed) {
    std::string msg = "must be >= ";
    msg += std::to_string(needed.count());
    msg += " (max of hot.default_eviction_seconds and hot.eviction_overrides[*].eviction_seconds)";
    return std::unexpected(InvalidArg("queue.min_retention_seconds", msg));
  }
  return {};
}

// Cold pauses absorption while it waits for a batch to become power
// durable, which freezes the frontier buffer-consistency reads wait on.
// Half leaves room for the apply and the next drain.
core::Result<void> ValidateColdReadVsConsistencyWait(const Config& c) {
  if (c.cold_consumer.queue_read_timeout * 2 > c.engine.buffer_consistency_wait_timeout) {
    return std::unexpected(
        InvalidArg("cold_consumer.queue_read_timeout_ms",
                   "must be at most half of engine.buffer_consistency_wait_timeout_ms (" +
                       std::to_string(c.engine.buffer_consistency_wait_timeout.count()) + " ms)"));
  }
  return {};
}

// Every log carries at least one shard's stream.
core::Result<void> ValidateLogCountVsShards(const Config& c) {
  if (c.queue.log_count > c.hot.shard_count) {
    return std::unexpected(
        InvalidArg("queue.log_count",
                   "must be <= hot.shard_count (" + std::to_string(c.hot.shard_count) + ")"));
  }
  return {};
}

// NOLINTNEXTLINE(misc-unused-parameters)
core::Result<void> ValidatePortCollisions(const Config& c) {
  const std::vector<std::pair<uint16_t, std::string_view>> ports = {
      {c.net.port, "net.port"},
      {c.metrics.port, "metrics.port"},
      {c.admin.port, "admin.port"},
  };
  for (size_t i = 0; i < ports.size(); ++i) {
    if (ports[i].first == 0) continue;  // ephemeral; resolved at bind time
    for (size_t j = i + 1; j < ports.size(); ++j) {
      if (ports[i].first == ports[j].first) {
        std::string msg = "collides with ";
        msg += ports[j].second;
        msg += " (both bound to the same port)";
        return std::unexpected(InvalidArg(std::string(ports[i].second), msg));
      }
    }
  }
  return {};
}

}  // namespace

core::Result<void> Validate(const Config& config) {
  if (!OneOf(config.profile, {"embedded", "external", "hybrid"})) {
    return std::unexpected(InvalidArg("profile", "must be one of: embedded, external, hybrid"));
  }
  if (auto r = ValidateHot(config.hot); !r) return r;
  if (auto r = ValidateCold(config.cold); !r) return r;
  if (auto r = ValidateQueue(config.queue); !r) return r;
  if (auto r = ValidateHotConsumer(config.hot_consumer); !r) return r;
  if (auto r = ValidateColdConsumer(config.cold_consumer); !r) return r;
  if (auto r = ValidateConsumerRpc(config.consumer_rpc); !r) return r;
  if (auto r = ValidateEngine(config.engine); !r) return r;
  if (auto r = ValidateRecovery(config.recovery); !r) return r;
  if (auto r = ValidateNet(config.net); !r) return r;
  if (auto r = ValidateMetrics(config.metrics); !r) return r;
  if (auto r = ValidateAdmin(config.admin); !r) return r;
  if (auto r = ValidateLog(config.log); !r) return r;
  if (auto r = ValidatePortCollisions(config); !r) return r;
  if (auto r = ValidateRetentionVsEviction(config); !r) return r;
  if (auto r = ValidateColdReadVsConsistencyWait(config); !r) return r;
  if (auto r = ValidateLogCountVsShards(config); !r) return r;
  return {};
}

}  // namespace abyss::config::internal
