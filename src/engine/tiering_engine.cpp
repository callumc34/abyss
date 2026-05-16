#include "abyss/engine/tiering_engine.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.engine")

namespace abyss::engine {

TieringEngine::TieringEngine(core::Queue& queue, core::HotStore& hot_store,
                             core::ColdStore& cold_store,
                             consumer::CompactionBufferRouter& buffer_router,
                             core::ConsumerRpc& rpc, TieringEngineConfig config)
    : queue_(queue),
      hot_store_(hot_store),
      cold_store_(cold_store),
      buffer_router_(buffer_router),
      rpc_(rpc),
      config_(config) {}

namespace {

bool IsHashRead(const core::ops::ReadOp& op) {
  return std::holds_alternative<core::ops::HashGet>(op) ||
         std::holds_alternative<core::ops::HashGetAll>(op) ||
         std::holds_alternative<core::ops::HashMultiGet>(op) ||
         std::holds_alternative<core::ops::HashFieldExists>(op) ||
         std::holds_alternative<core::ops::HashKeys>(op) ||
         std::holds_alternative<core::ops::HashVals>(op) ||
         std::holds_alternative<core::ops::HashLen>(op);
}

core::RespValue WrongType() {
  return core::RespValue::Error(core::ErrorPrefix::kWrongType,
                                "Operation against a key holding the wrong kind of value");
}

// Empty / dead-key response for each hash read op shape.
core::RespValue TombstoneResponse(const core::ops::ReadOp& op) {
  return std::visit(
      [](const auto& o) -> core::RespValue {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::HashGet>) {
          return core::RespValue::Null();
        } else if constexpr (std::is_same_v<T, core::ops::HashFieldExists>) {
          return core::RespValue::Integer(0);
        } else if constexpr (std::is_same_v<T, core::ops::HashLen>) {
          return core::RespValue::Integer(0);
        } else if constexpr (std::is_same_v<T, core::ops::HashMultiGet>) {
          std::vector<core::RespValue> nulls(o.fields.size(), core::RespValue::Null());
          return core::RespValue::Array(std::move(nulls));
        } else {
          // HashGetAll, HashKeys, HashVals.
          return core::RespValue::Array({});
        }
      },
      op);
}

// Decodes cold's HGETALL array into a {field → value} map. Cold emits a
// flat array of alternating bulk strings; the merge needs random access.
std::unordered_map<std::string, std::string> DecodeColdHashMap(const core::RespValue& cold_hash) {
  std::unordered_map<std::string, std::string> map;
  if (!cold_hash.IsArray()) return map;
  const auto& arr = cold_hash.AsArray();
  map.reserve(arr.size() / 2);
  for (size_t i = 0; i + 1 < arr.size(); i += 2) {
    if (arr[i].IsBulkString() && arr[i + 1].IsBulkString()) {
      map.emplace(arr[i].AsString(), arr[i + 1].AsString());
    }
  }
  return map;
}

}  // namespace

core::Result<core::RespValue> TieringEngine::DispatchRead(std::string_view name,
                                                          const core::RespCommand& cmd) {
  auto op = core::ops::ParseReadOp(name, cmd);
  if (!op.has_value()) {
    return std::unexpected(op.error());
  }

  auto hot_result = hot_store_.Exec(*op);
  if (hot_result.has_value()) {
    return hot_result;
  }
  if (hot_result.error().code() != core::ErrorCode::kNotFound) {
    return hot_result;
  }

  // Hash reads require buffer-overlay merge: the buffer holds a delta over
  // cold, so neither tier alone has the full state when hot misses.
  if (IsHashRead(*op)) {
    return DispatchHashRead(*op);
  }

  auto key = core::ops::PrimaryKey(*op);
  if (!key.empty()) {
    auto buffer_result = buffer_router_.Read(key);
    if (buffer_result.has_value()) {
      return buffer_result;
    }
  }

  auto cold_result = cold_store_.Exec(*op);
  if (cold_result.has_value() && !cold_result->IsNull() && !key.empty()) {
    PromoteThroughQueue(key);
  }
  return cold_result;
}

core::Result<core::RespValue> TieringEngine::DispatchHashRead(const core::ops::ReadOp& op) {
  const auto key = core::ops::PrimaryKey(op);
  const auto overlay = buffer_router_.HashOverlayFor(key);

  switch (overlay.kind) {
    case consumer::HashOverlay::Kind::kTombstone:
      return TombstoneResponse(op);
    case consumer::HashOverlay::Kind::kWrongType:
      return WrongType();
    case consumer::HashOverlay::Kind::kNotPresent:
      // No buffer state; cold answers canonically.
      return cold_store_.Exec(op);
    case consumer::HashOverlay::Kind::kHash:
      break;
  }

  // Per-field reads: ask the overlay first, fall back to cold per-unknown.
  if (const auto* get = std::get_if<core::ops::HashGet>(&op)) {
    const auto field = std::string(get->field);
    if (overlay.removed_fields.contains(field)) return core::RespValue::Null();
    auto it = overlay.fields.find(field);
    if (it != overlay.fields.end()) return core::RespValue::BulkString(it->second);
    return cold_store_.Exec(op);
  }
  if (const auto* hex = std::get_if<core::ops::HashFieldExists>(&op)) {
    const auto field = std::string(hex->field);
    if (overlay.removed_fields.contains(field)) return core::RespValue::Integer(0);
    if (overlay.fields.contains(field)) return core::RespValue::Integer(1);
    return cold_store_.Exec(op);
  }
  if (const auto* hmget = std::get_if<core::ops::HashMultiGet>(&op)) {
    std::vector<core::RespValue> out;
    out.reserve(hmget->fields.size());
    for (auto field : hmget->fields) {
      const auto fs = std::string(field);
      if (overlay.removed_fields.contains(fs)) {
        out.push_back(core::RespValue::Null());
        continue;
      }
      auto it = overlay.fields.find(fs);
      if (it != overlay.fields.end()) {
        out.push_back(core::RespValue::BulkString(it->second));
        continue;
      }
      auto cold_one = cold_store_.Exec(core::ops::ReadOp{core::ops::HashGet{
          .key = hmget->key,
          .field = field,
      }});
      if (!cold_one.has_value()) return std::unexpected(cold_one.error());
      out.push_back(std::move(*cold_one));
    }
    return core::RespValue::Array(std::move(out));
  }

  // Full-collection reads: take cold's HGETALL and apply the overlay.
  auto cold_all = cold_store_.Exec(core::ops::ReadOp{core::ops::HashGetAll{.key = key}});
  if (!cold_all.has_value()) return std::unexpected(cold_all.error());
  auto merged = DecodeColdHashMap(*cold_all);
  for (const auto& removed : overlay.removed_fields) {
    merged.erase(removed);
  }
  for (const auto& [field, value] : overlay.fields) {
    merged[field] = value;
  }

  if (std::holds_alternative<core::ops::HashLen>(op)) {
    return core::RespValue::Integer(static_cast<int64_t>(merged.size()));
  }
  std::vector<core::RespValue> out;
  if (std::holds_alternative<core::ops::HashGetAll>(op)) {
    out.reserve(merged.size() * 2);
    for (const auto& [field, value] : merged) {
      out.push_back(core::RespValue::BulkString(field));
      out.push_back(core::RespValue::BulkString(value));
    }
  } else if (std::holds_alternative<core::ops::HashKeys>(op)) {
    out.reserve(merged.size());
    for (const auto& [field, _] : merged) {
      out.push_back(core::RespValue::BulkString(field));
    }
  } else if (std::holds_alternative<core::ops::HashVals>(op)) {
    out.reserve(merged.size());
    for (const auto& [_, value] : merged) {
      out.push_back(core::RespValue::BulkString(value));
    }
  }
  return core::RespValue::Array(std::move(out));
}

void TieringEngine::PromoteThroughQueue(std::string_view key) {
  auto promotion = cold_store_.GetPromotionCommand(key);
  if (!promotion.has_value() || !promotion->has_value()) return;

  core::QueueEntry entry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = std::move(**promotion)},
  };
  const core::ShardId shard = core::ComputeShard(key, config_.shard_count);
  // Best-effort: client already has the cold value; never block the read path.
  auto appended = queue_.Append(shard, std::move(entry));
  if (!appended.has_value()) {
    promotion_append_failures_.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_WARN("promotion append failed", {"shard", static_cast<int64_t>(shard)},
                   {"key_hash", log::KeyHash(key)},
                   {"err", std::string_view{appended.error().message()}});
  }
}

TieringEngineMetrics TieringEngine::Snapshot() const {
  return TieringEngineMetrics{
      .promotion_append_failures = promotion_append_failures_.load(std::memory_order_relaxed),
  };
}

namespace {

core::ShardId ShardForCmd(const core::RespCommand& cmd, uint32_t shard_count) {
  if (cmd.args.size() <= 1) return 0;
  return core::ComputeShard(cmd.args[1], shard_count);
}

}  // namespace

core::Result<core::RespValue> TieringEngine::DispatchWrite(std::string_view /*name*/,
                                                           core::RespCommand cmd) {
  const core::ShardId shard = ShardForCmd(cmd, config_.shard_count);

  core::QueueEntry entry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = std::move(cmd)},
  };

  auto pending = queue_.BeginAppend(shard, std::move(entry));
  if (!pending.has_value()) {
    return std::unexpected(pending.error());
  }
  const core::SequenceId seq = pending->seq();
  const core::RpcId rpc_id = core::MakeRpcId(shard, seq);
  auto rpc_future = rpc_.Register(rpc_id);
  queue::DurabilityFuture durable_future = std::move(pending->durable());
  pending->Publish();

  const auto durable_deadline = std::chrono::steady_clock::now() + config_.write_timeout;

  // fsync first: a durable-layer failure takes precedence over consumer error.
  if (durable_future.wait_until(durable_deadline) == std::future_status::timeout) {
    rpc_.Cancel(rpc_id);
    ABYSS_LOG_WARN("write durable wait timeout", {"shard", static_cast<int64_t>(shard)},
                   {"seq", static_cast<uint64_t>(seq)},
                   {"timeout_ms", static_cast<int64_t>(config_.write_timeout.count())});
    return core::RespValue::Error(
        core::ErrorPrefix::kErr,
        "write durable wait exceeded server timeout; write will apply on consumer catch-up");
  }
  auto durable = durable_future.get();
  if (!durable.has_value()) {
    rpc_.Cancel(rpc_id);
    ABYSS_LOG_ERROR("write durable failed", {"shard", static_cast<int64_t>(shard)},
                    {"seq", static_cast<uint64_t>(seq)},
                    {"err", std::string_view{durable.error().message()}});
    return std::unexpected(durable.error());
  }

  const auto now = std::chrono::steady_clock::now();
  const auto min_rpc_budget = std::chrono::milliseconds{static_cast<int64_t>(
      static_cast<double>(config_.write_timeout.count()) * config_.min_rpc_wait_fraction)};
  const auto rpc_deadline = std::max(durable_deadline, now + min_rpc_budget);

  if (rpc_future.wait_until(rpc_deadline) == std::future_status::timeout) {
    rpc_.Cancel(rpc_id);
    ABYSS_LOG_WARN("write durable but consumer apply timeout",
                   {"shard", static_cast<int64_t>(shard)}, {"seq", static_cast<uint64_t>(seq)});
    return core::RespValue::Error(
        core::ErrorPrefix::kErr,
        "write durable in queue but consumer did not apply within timeout");
  }
  return rpc_future.get();
}

core::Result<core::RespValue> TieringEngine::DispatchConditional(std::string_view /*name*/,
                                                                 core::RespCommand cmd,
                                                                 core::PredicateFlags flags) {
  const core::ShardId shard = ShardForCmd(cmd, config_.shard_count);

  core::QueueEntry entry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Conditional{.cmd = std::move(cmd), .flags = flags},
  };

  auto pending = queue_.BeginAppend(shard, std::move(entry));
  if (!pending.has_value()) {
    return std::unexpected(pending.error());
  }
  const core::SequenceId seq = pending->seq();
  const core::RpcId rpc_id = core::MakeRpcId(shard, seq);
  auto rpc_future = rpc_.Register(rpc_id);
  queue::DurabilityFuture durable_future = std::move(pending->durable());
  pending->Publish();

  const auto durable_deadline = std::chrono::steady_clock::now() + config_.write_timeout;
  if (durable_future.wait_until(durable_deadline) == std::future_status::timeout) {
    rpc_.Cancel(rpc_id);
    return core::RespValue::Error(
        core::ErrorPrefix::kErr,
        "conditional durable wait exceeded server timeout; will resolve on resolver catch-up");
  }
  auto durable = durable_future.get();
  if (!durable.has_value()) {
    rpc_.Cancel(rpc_id);
    return std::unexpected(durable.error());
  }

  const auto now = std::chrono::steady_clock::now();
  const auto min_rpc_budget = std::chrono::milliseconds{static_cast<int64_t>(
      static_cast<double>(config_.write_timeout.count()) * config_.min_rpc_wait_fraction)};
  const auto rpc_deadline = std::max(durable_deadline, now + min_rpc_budget);

  if (rpc_future.wait_until(rpc_deadline) == std::future_status::timeout) {
    rpc_.Cancel(rpc_id);
    return core::RespValue::Error(
        core::ErrorPrefix::kErr,
        "conditional durable in queue but resolver did not decide within timeout");
  }
  return rpc_future.get();
}

}  // namespace abyss::engine
