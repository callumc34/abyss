#include "validator.h"

#include <algorithm>
#include <array>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace abyss::config::internal {

namespace {

core::Error InvalidArg(std::string path, std::string_view message) {
  std::string msg = std::move(path);
  msg += ": ";
  msg += message;
  return {core::ErrorCode::kInvalidArgument, std::move(msg)};
}

bool OneOf(std::string_view value, std::initializer_list<std::string_view> allowed) {
  return std::ranges::find(allowed, value) != allowed.end();
}

core::Result<void> RequirePort(std::string path, uint16_t port) {
  if (port == 0) return std::unexpected(InvalidArg(std::move(path), "port must be non-zero"));
  return {};
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
  if (auto r = RequirePositive("hot.default_eviction_seconds", hot.default_eviction); !r) return r;

  std::unordered_set<std::string> seen;
  for (size_t i = 0; i < hot.eviction_overrides.size(); ++i) {
    const auto& o = hot.eviction_overrides[i];
    std::string base = "hot.eviction_overrides[";
    base += std::to_string(i);
    base += ']';
    if (auto r = RequireNonEmpty(base + ".prefix", o.prefix); !r) return r;
    if (auto r = RequirePositive(base + ".eviction_seconds", o.eviction); !r) return r;
    if (!seen.insert(o.prefix).second) {
      return std::unexpected(InvalidArg(base + ".prefix", "duplicate prefix"));
    }
  }
  return {};
}

core::Result<void> ValidateCold(const ColdConfig& cold) {
  if (auto r = RequireNonEmpty("cold.backend", cold.backend); !r) return r;
  if (auto r = RequireNonEmpty("cold.data_path", cold.data_path); !r) return r;
  if (auto r = RequirePositive("cold.write_buffer_size_bytes", cold.write_buffer_size_bytes); !r)
    return r;
  return {};
}

core::Result<void> ValidateQueue(const QueueConfig& q) {
  if (auto r = RequireNonEmpty("queue.backend", q.backend); !r) return r;
  if (auto r = RequireNonEmpty("queue.wal_path", q.wal_path); !r) return r;
  if (auto r = RequirePositive("queue.segment_size_bytes", q.segment_size_bytes); !r) return r;
  if (q.min_retention.count() < 0)
    return std::unexpected(InvalidArg("queue.min_retention_seconds", "must be >= 0 seconds"));

  if (!OneOf(q.fsync_policy, {"fsync_per_write", "group_commit", "fsync_none"})) {
    return std::unexpected(InvalidArg("queue.wal_fsync_policy",
                                      "must be one of: fsync_per_write, group_commit, fsync_none"));
  }

  if (q.fsync_policy == "group_commit") {
    if (auto r = RequirePositive("queue.group_commit_interval_us", q.group_commit_interval_us); !r)
      return r;
    if (auto r = RequirePositive("queue.group_commit_max_bytes", q.group_commit_max_bytes); !r)
      return r;
  }
  return {};
}

core::Result<void> ValidateColdConsumer(const ColdConsumerConfig& c) {
  if (auto r = RequirePositive("cold_consumer.quiet_threshold_seconds", c.quiet_threshold); !r)
    return r;
  if (auto r = RequirePositive("cold_consumer.safety_margin_seconds", c.safety_margin); !r)
    return r;
  if (c.deadline_jitter_ratio < 0.0 || c.deadline_jitter_ratio > 1.0) {
    return std::unexpected(
        InvalidArg("cold_consumer.deadline_jitter_ratio", "must be in [0.0, 1.0]"));
  }
  if (auto r = RequirePositive("cold_consumer.buffer_high_water_bytes", c.buffer_high_water_bytes);
      !r)
    return r;
  if (auto r = RequirePositive("cold_consumer.max_flush_batch_size", c.max_flush_batch_size); !r)
    return r;
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

core::Result<void> ValidateResp(const RespConfig& r) {
  if (auto res = RequireNonEmpty("resp.bind", r.bind); !res) return res;
  if (auto res = RequirePort("resp.port", r.port); !res) return res;
  if (auto res = RequirePositive("resp.max_connections", r.max_connections); !res) return res;
  if (auto res = RequirePositive("resp.idle_timeout_seconds", r.idle_timeout); !res) return res;
  return {};
}

core::Result<void> ValidateMetrics(const MetricsConfig& m) {
  if (auto r = RequireNonEmpty("metrics.bind", m.bind); !r) return r;
  if (auto r = RequirePort("metrics.port", m.port); !r) return r;
  return {};
}

core::Result<void> ValidateAdmin(const AdminConfig& a) {
  if (auto r = RequireNonEmpty("admin.bind", a.bind); !r) return r;
  if (auto r = RequirePort("admin.port", a.port); !r) return r;
  return {};
}

core::Result<void> ValidatePortCollisions(const Config& c) {
  const std::array<std::pair<uint16_t, std::string_view>, 3> ports = {{
      {c.resp.port, "resp.port"},
      {c.metrics.port, "metrics.port"},
      {c.admin.port, "admin.port"},
  }};
  for (const auto* lhs = ports.begin(); lhs != ports.end(); ++lhs) {
    for (const auto* rhs = std::next(lhs); rhs != ports.end(); ++rhs) {
      if (lhs->first == rhs->first) {
        std::string msg = "collides with ";
        msg += rhs->second;
        msg += " (both bound to the same port)";
        return std::unexpected(InvalidArg(std::string(lhs->second), msg));
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
  if (auto r = ValidateColdConsumer(config.cold_consumer); !r) return r;
  if (auto r = ValidateRecovery(config.recovery); !r) return r;
  if (auto r = ValidateResp(config.resp); !r) return r;
  if (auto r = ValidateMetrics(config.metrics); !r) return r;
  if (auto r = ValidateAdmin(config.admin); !r) return r;
  if (auto r = ValidatePortCollisions(config); !r) return r;
  return {};
}

}  // namespace abyss::config::internal
