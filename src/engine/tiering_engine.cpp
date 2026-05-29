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
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.engine")

namespace abyss::engine {

TieringEngine::TieringEngine(core::Queue& queue, core::HotStore& hot_store,
                             core::ColdStore& cold_store,
                             consumer::CompactionBufferRouter& buffer_router,
                             const consumer::HotConsumerProgress& hot_progress,
                             core::ConsumerRpc& rpc, TieringEngineConfig config)
    : queue_(queue),
      hot_store_(hot_store),
      cold_store_(cold_store),
      buffer_router_(buffer_router),
      hot_progress_(hot_progress),
      rpc_(rpc),
      config_(config) {
  auto& reg = metrics::Registry::Instance();
  hits_hot_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kHot);
  hits_buffer_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kBuffer);
  hits_cold_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kCold);
  misses_ = reg.Counter(metrics::names::kMissesTotal);
  promotions_ = reg.Counter(metrics::names::kPromotionsTotal);
}

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
  return DispatchSingleKeyRead(*op);
}

core::Result<core::RespValue> TieringEngine::DispatchSingleKeyRead(const core::ops::ReadOp& op) {
  auto hot_result = hot_store_.Exec(op);
  if (hot_result.has_value()) {
    hits_hot_.Increment();
    return hot_result;
  }
  if (hot_result.error().code() != core::ErrorCode::kNotFound) {
    return hot_result;
  }

  // Hot missed, so the key is unknown to hot (a recent delete would be a
  // tombstone, handled above). A whole-key string read is genuinely absent and
  // cold is equally current, so it serves now; a collection read can race a
  // buffered partial mutation (SREM/HDEL), so it gates on cold. See ADP-006.
  auto key = core::ops::PrimaryKey(op);
  const bool needs_buffer_consistency = !std::holds_alternative<core::ops::StringGet>(op);
  if (needs_buffer_consistency && !WaitForBufferConsistency(key)) {
    return core::RespValue::Error(
        core::ErrorPrefix::kErr, "read timed out waiting for compaction buffer to catch up to hot");
  }

  if (IsHashRead(op)) {
    return DispatchHashRead(op);
  }

  if (!key.empty()) {
    auto buffer_result = buffer_router_.Read(key);
    if (buffer_result.has_value()) {
      hits_buffer_.Increment();
      return buffer_result;
    }
  }

  auto cold_result = cold_store_.Exec(op);
  if (cold_result.has_value() && !cold_result->IsNull()) {
    hits_cold_.Increment();
    if (!key.empty()) {
      PromoteThroughQueue(key);
    }
  } else if (cold_result.has_value()) {
    misses_.Increment();
  }
  return cold_result;
}

bool TieringEngine::WaitForBufferConsistency(std::string_view key) {
  if (key.empty()) return true;
  const auto shard = core::ComputeShard(key, config_.shard_count);
  const auto target_seq = hot_progress_.HighestSettledSeq(shard);
  if (target_seq == 0) return true;
  if (buffer_router_.WaitForDrainedSeq(shard, target_seq,
                                       config_.buffer_consistency_wait_timeout)) {
    return true;
  }
  read_buffer_wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
  return false;
}

core::Result<core::RespValue> TieringEngine::DispatchHashRead(const core::ops::ReadOp& op) {
  // Caller (DispatchSingleKeyRead) has already waited for buffer consistency.
  const auto key = core::ops::PrimaryKey(op);
  const auto overlay = buffer_router_.HashOverlayFor(key);

  // Tracks whether cold answered any portion of the read. Cold-hit semantics
  // win over buffer-hit when both contributed (ADP-006 §Metrics).
  bool touched_cold = false;

  auto record_outcome = [&](const core::Result<core::RespValue>& result) {
    if (!result.has_value()) return;
    if (touched_cold) {
      hits_cold_.Increment();
    } else {
      hits_buffer_.Increment();
    }
  };

  switch (overlay.kind) {
    case consumer::HashOverlay::Kind::kTombstone: {
      auto r = TombstoneResponse(op);
      hits_buffer_.Increment();
      return r;
    }
    case consumer::HashOverlay::Kind::kWrongType:
      return WrongType();
    case consumer::HashOverlay::Kind::kNotPresent: {
      auto cold = cold_store_.Exec(op);
      if (cold.has_value()) {
        if (cold->IsNull()) {
          misses_.Increment();
        } else {
          hits_cold_.Increment();
        }
      }
      return cold;
    }
    case consumer::HashOverlay::Kind::kHash:
      break;
  }

  // Per-field reads: ask the overlay first, fall back to cold per-unknown.
  if (const auto* get = std::get_if<core::ops::HashGet>(&op)) {
    const auto field = std::string(get->field);
    if (overlay.removed_fields.contains(field)) {
      hits_buffer_.Increment();
      return core::RespValue::Null();
    }
    auto it = overlay.fields.find(field);
    if (it != overlay.fields.end()) {
      hits_buffer_.Increment();
      return core::RespValue::BulkString(it->second);
    }
    touched_cold = true;
    auto r = cold_store_.Exec(op);
    record_outcome(r);
    return r;
  }
  if (const auto* hex = std::get_if<core::ops::HashFieldExists>(&op)) {
    const auto field = std::string(hex->field);
    if (overlay.removed_fields.contains(field)) {
      hits_buffer_.Increment();
      return core::RespValue::Integer(0);
    }
    if (overlay.fields.contains(field)) {
      hits_buffer_.Increment();
      return core::RespValue::Integer(1);
    }
    touched_cold = true;
    auto r = cold_store_.Exec(op);
    record_outcome(r);
    return r;
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
      touched_cold = true;
      auto cold_one = cold_store_.Exec(core::ops::ReadOp{core::ops::HashGet{
          .key = hmget->key,
          .field = field,
      }});
      if (!cold_one.has_value()) return std::unexpected(cold_one.error());
      out.push_back(std::move(*cold_one));
    }
    auto r = core::RespValue::Array(std::move(out));
    if (touched_cold) {
      hits_cold_.Increment();
    } else {
      hits_buffer_.Increment();
    }
    return r;
  }

  // Full-collection reads: take cold's HGETALL and apply the overlay.
  touched_cold = true;
  auto cold_all = cold_store_.Exec(core::ops::ReadOp{core::ops::HashGetAll{.key = key}});
  if (!cold_all.has_value()) return std::unexpected(cold_all.error());
  auto merged = DecodeColdHashMap(*cold_all);
  for (const auto& removed : overlay.removed_fields) {
    merged.erase(removed);
  }
  for (const auto& [field, value] : overlay.fields) {
    merged[field] = value;
  }
  hits_cold_.Increment();

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

core::Result<core::RespValue> TieringEngine::FanOutMget(const core::RespCommand& cmd) {
  if (cmd.args.size() < 2) {
    return std::unexpected(
        core::Error(core::ErrorCode::kInvalidArgument, "MGET requires at least one key"));
  }
  std::vector<core::RespValue> out;
  out.reserve(cmd.args.size() - 1);
  for (size_t i = 1; i < cmd.args.size(); ++i) {
    auto result =
        DispatchSingleKeyRead(core::ops::ReadOp{core::ops::StringGet{.key = cmd.args[i]}});
    if (!result.has_value()) {
      // Redis MGET collapses missing keys and non-string-typed keys to nil.
      if (result.error().code() == core::ErrorCode::kNotFound ||
          result.error().code() == core::ErrorCode::kWrongType) {
        out.push_back(core::RespValue::Null());
        continue;
      }
      return std::unexpected(result.error());
    }
    if (result->IsError() && result->ErrorPrefixOf() == core::ErrorPrefix::kWrongType) {
      out.push_back(core::RespValue::Null());
      continue;
    }
    out.push_back(std::move(*result));
  }
  return core::RespValue::Array(std::move(out));
}

core::Result<core::RespValue> TieringEngine::FanOutExists(const core::RespCommand& cmd) {
  if (cmd.args.size() < 2) {
    return std::unexpected(
        core::Error(core::ErrorCode::kInvalidArgument, "EXISTS requires at least one key"));
  }
  // EXISTS k k counts twice (Redis behaviour): no dedup.
  int64_t count = 0;
  for (size_t i = 1; i < cmd.args.size(); ++i) {
    auto present = ProbeKeyExists(cmd.args[i]);
    if (!present.has_value()) {
      return std::unexpected(present.error());
    }
    if (*present) ++count;
  }
  return core::RespValue::Integer(count);
}

core::Result<bool> TieringEngine::ProbeKeyExists(std::string_view key) {
  // Hot is authoritative for recent writes and deletes: a present key exists, a
  // tombstone is an authoritative delete. Both short-circuit without consulting
  // the lagging overlay. See ADP-006 §Read Path.
  switch (hot_store_.Probe(key)) {
    case core::HotKeyPresence::kPresent:
      return true;
    case core::HotKeyPresence::kTombstoned:
      return false;
    case core::HotKeyPresence::kAbsent:
      break;
  }

  // Hot doesn't know the key. It may be a hot-evicted collection with a buffered
  // partial mutation, so the buffer probe (which overrides cold) must be at
  // least as current as hot before it can suppress a stale cold residual.
  if (!WaitForBufferConsistency(key)) {
    return std::unexpected(
        core::Error{core::ErrorCode::kTimeout,
                    "EXISTS timed out waiting for compaction buffer to catch up to hot"});
  }

  // Buffer probe overrides cold: a not-yet-flushed DEL means the key is absent.
  switch (buffer_router_.Probe(key)) {
    case consumer::BufferKeyPresence::kPresent:
      return true;
    case consumer::BufferKeyPresence::kTombstoned:
      return false;
    case consumer::BufferKeyPresence::kAbsent:
      break;
  }

  auto cold = cold_store_.Exec(core::ops::ReadOp{core::ops::Exists{.keys = {key}}});
  if (!cold.has_value()) {
    if (cold.error().code() == core::ErrorCode::kNotFound) return false;
    return std::unexpected(cold.error());
  }
  return cold->IsInteger() && cold->AsInteger() > 0;
}

void TieringEngine::PromoteThroughQueue(std::string_view key) {
  auto promotion = cold_store_.GetPromotionCommand(key);
  if (!promotion.has_value() || !promotion->has_value()) return;

  core::QueueEntry entry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = std::move(**promotion)},
  };
  const core::ShardId shard = core::ComputeShard(key, config_.shard_count);
  promotions_.Increment();
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
      .flush_total = flush_total_.load(std::memory_order_relaxed),
      .flush_durable_failures = flush_durable_failures_.load(std::memory_order_relaxed),
      .flush_consumer_timeouts = flush_consumer_timeouts_.load(std::memory_order_relaxed),
      .flush_append_failures = flush_append_failures_.load(std::memory_order_relaxed),
      .read_buffer_wait_timeouts = read_buffer_wait_timeouts_.load(std::memory_order_relaxed),
  };
}

core::Result<core::RespValue> TieringEngine::DispatchFlush(core::FlushTarget /*target*/) {
  flush_total_.fetch_add(1, std::memory_order_relaxed);

  struct ShardWait {
    core::ShardId shard = 0;
    core::SequenceId seq = 0;
    queue::DurabilityFuture durable;
    std::future<core::RespValue> hot;
    std::future<core::RespValue> cold;
    std::future<core::RespValue> resolver;
  };

  auto cancel_all = [&](std::vector<ShardWait>& ws) {
    for (auto& w : ws) {
      rpc_.Cancel(core::MakeFlushRpcId(core::kHotConsumer, w.shard, w.seq));
      rpc_.Cancel(core::MakeFlushRpcId(core::kColdConsumer, w.shard, w.seq));
      rpc_.Cancel(core::MakeFlushRpcId(core::kResolverConsumer, w.shard, w.seq));
    }
  };

  std::vector<ShardWait> waits;
  waits.reserve(config_.shard_count);

  for (core::ShardId shard = 0; shard < config_.shard_count; ++shard) {
    core::QueueEntry entry{
        .seq = 0,
        .appended_at = core::WallClock::now(),
        .payload = core::entry::Flush{},
    };
    auto pending = queue_.BeginAppend(shard, std::move(entry));
    if (!pending.has_value()) {
      flush_append_failures_.fetch_add(1, std::memory_order_relaxed);
      cancel_all(waits);
      ABYSS_LOG_ERROR("flush append failed", {"shard", static_cast<int64_t>(shard)},
                      {"err", std::string_view{pending.error().message()}});
      return std::unexpected(pending.error());
    }

    ShardWait w;
    w.shard = shard;
    w.seq = pending->seq();
    w.hot = rpc_.Register(core::MakeFlushRpcId(core::kHotConsumer, shard, w.seq));
    w.cold = rpc_.Register(core::MakeFlushRpcId(core::kColdConsumer, shard, w.seq));
    w.resolver = rpc_.Register(core::MakeFlushRpcId(core::kResolverConsumer, shard, w.seq));
    w.durable = std::move(pending->durable());
    pending->Publish();
    waits.push_back(std::move(w));
  }

  const auto deadline = std::chrono::steady_clock::now() + config_.write_timeout;

  // Every shard's durable wait must succeed before any consumer applies; on
  // timeout, surface one error and cancel — FLUSHDB retry is idempotent.
  for (auto& w : waits) {
    if (w.durable.wait_until(deadline) == std::future_status::timeout) {
      flush_durable_failures_.fetch_add(1, std::memory_order_relaxed);
      cancel_all(waits);
      ABYSS_LOG_WARN("flush durable wait timeout", {"shard", static_cast<int64_t>(w.shard)},
                     {"seq", static_cast<uint64_t>(w.seq)},
                     {"timeout_ms", static_cast<int64_t>(config_.write_timeout.count())});
      return core::RespValue::Error(
          core::ErrorPrefix::kErr,
          "flush durable wait exceeded server timeout; retry to complete the wipe");
    }
    auto durable = w.durable.get();
    if (!durable.has_value()) {
      flush_durable_failures_.fetch_add(1, std::memory_order_relaxed);
      cancel_all(waits);
      ABYSS_LOG_ERROR("flush durable failed", {"shard", static_cast<int64_t>(w.shard)},
                      {"seq", static_cast<uint64_t>(w.seq)},
                      {"err", std::string_view{durable.error().message()}});
      return std::unexpected(durable.error());
    }
  }

  // Floor the apply-phase budget so a slow fsync doesn't leave ~0ms for it.
  const auto now = std::chrono::steady_clock::now();
  const auto min_rpc_budget = std::chrono::milliseconds{static_cast<int64_t>(
      static_cast<double>(config_.write_timeout.count()) * config_.min_rpc_wait_fraction)};
  const auto rpc_deadline = std::max(deadline, now + min_rpc_budget);

  for (auto& w : waits) {
    for (auto* fut : {&w.hot, &w.cold, &w.resolver}) {
      if (fut->wait_until(rpc_deadline) == std::future_status::timeout) {
        flush_consumer_timeouts_.fetch_add(1, std::memory_order_relaxed);
        cancel_all(waits);
        ABYSS_LOG_WARN("flush consumer apply timeout", {"shard", static_cast<int64_t>(w.shard)},
                       {"seq", static_cast<uint64_t>(w.seq)});
        return core::RespValue::Error(
            core::ErrorPrefix::kErr,
            "flush durable in queue but a consumer did not apply within timeout");
      }
      auto val = fut->get();
      if (val.IsError()) {
        cancel_all(waits);
        return val;
      }
    }
  }

  return core::RespValue::SimpleString("OK");
}

namespace {

core::ShardId ShardForCmd(const core::RespCommand& cmd, uint32_t shard_count) {
  if (cmd.args.size() <= 1) return 0;
  return core::ComputeShard(cmd.args[1], shard_count);
}

}  // namespace

core::Result<core::RespValue> TieringEngine::DispatchWrite(std::string_view /*name*/,
                                                           core::RespCommand cmd) {
  return DispatchSingleKeyWrite(std::move(cmd));
}

core::Result<core::RespValue> TieringEngine::DispatchFanOut(core::MultiKeyKind kind,
                                                            core::RespCommand cmd) {
  // See ADP-005 §Multi-Key Commands; ADP-006 §Multi-Key Fan-Out.
  switch (kind) {
    case core::MultiKeyKind::kMget:
      return FanOutMget(cmd);
    case core::MultiKeyKind::kExists:
      return FanOutExists(cmd);
    case core::MultiKeyKind::kMset:
      return FanOutMset(cmd);
    case core::MultiKeyKind::kDelete:
      return FanOutDel(cmd.Name(), cmd);
    case core::MultiKeyKind::kNone:
      break;
  }
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "DispatchFanOut called with MultiKeyKind::kNone"));
}

core::Result<core::RespValue> TieringEngine::DispatchSingleKeyWrite(core::RespCommand cmd) {
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

core::Result<core::RespValue> TieringEngine::FanOutMset(const core::RespCommand& cmd) {
  if (cmd.args.size() < 3 || (cmd.args.size() % 2) == 0) {
    return std::unexpected(
        core::Error(core::ErrorCode::kInvalidArgument, "MSET requires key-value pairs"));
  }
  std::vector<core::RespCommand> subs;
  subs.reserve((cmd.args.size() - 1) / 2);
  for (size_t i = 1; i + 1 < cmd.args.size(); i += 2) {
    subs.push_back(core::RespCommand{.args = {"SET", cmd.args[i], cmd.args[i + 1]}});
  }
  auto fan = FanOutWrite(subs);
  if (!fan.has_value()) return std::unexpected(fan.error());
  if (fan->IsError()) return std::move(*fan);
  for (const auto& v : fan->AsArray()) {
    if (v.IsError()) {
      return core::RespValue::Error(v.ErrorPrefixOf(), std::string(v.ErrorMessage()));
    }
  }
  return core::RespValue::SimpleString("OK");
}

core::Result<core::RespValue> TieringEngine::FanOutDel(std::string_view name,
                                                       const core::RespCommand& cmd) {
  if (cmd.args.size() < 2) {
    std::string msg{name};
    msg += " requires at least one key";
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument, msg));
  }
  std::vector<core::RespCommand> subs;
  subs.reserve(cmd.args.size() - 1);
  for (size_t i = 1; i < cmd.args.size(); ++i) {
    subs.push_back(core::RespCommand{.args = {std::string{name}, cmd.args[i]}});
  }
  auto fan = FanOutWrite(subs);
  if (!fan.has_value()) return std::unexpected(fan.error());
  if (fan->IsError()) return std::move(*fan);
  int64_t total = 0;
  for (const auto& v : fan->AsArray()) {
    if (v.IsError()) return v;
    if (!v.IsInteger()) {
      return core::RespValue::Error(core::ErrorPrefix::kErr,
                                    "internal: DEL sub-result is not an integer");
    }
    total += v.AsInteger();
  }
  return core::RespValue::Integer(total);
}

core::Result<core::RespValue> TieringEngine::FanOutWrite(
    const std::vector<core::RespCommand>& subs) {
  struct InFlight {
    core::ShardId shard;
    core::SequenceId seq;
    core::RpcId rpc_id;
    std::future<core::RespValue> rpc_future;
    queue::DurabilityFuture durable_future;
  };

  std::vector<InFlight> in_flight;
  in_flight.reserve(subs.size());

  // Per-sub Begin → Register → Publish. BeginAppend returns with the per-shard
  // append mutex held; two subs hashing to the same shard would deadlock if we
  // batched Begins before publishing.
  for (const auto& sub : subs) {
    const core::ShardId shard = ShardForCmd(sub, config_.shard_count);
    core::QueueEntry entry{
        .appended_at = core::WallClock::now(),
        .payload = core::entry::Write{.cmd = sub},
    };
    auto pending = queue_.BeginAppend(shard, std::move(entry));
    if (!pending.has_value()) {
      for (auto& f : in_flight) rpc_.Cancel(f.rpc_id);
      return std::unexpected(pending.error());
    }
    const core::SequenceId seq = pending->seq();
    const core::RpcId rpc_id = core::MakeRpcId(shard, seq);
    in_flight.push_back(InFlight{
        .shard = shard,
        .seq = seq,
        .rpc_id = rpc_id,
        .rpc_future = rpc_.Register(rpc_id),
        .durable_future = std::move(pending->durable()),
    });
    pending->Publish();
  }

  // Shared deadline: fan-out doesn't widen the single-key write_timeout.
  const auto durable_deadline = std::chrono::steady_clock::now() + config_.write_timeout;
  auto cancel_remaining = [&](size_t from) {
    for (size_t j = from; j < in_flight.size(); ++j) rpc_.Cancel(in_flight[j].rpc_id);
  };

  for (size_t i = 0; i < in_flight.size(); ++i) {
    auto& f = in_flight[i];
    if (f.durable_future.wait_until(durable_deadline) == std::future_status::timeout) {
      cancel_remaining(i);
      ABYSS_LOG_WARN("fan-out write durable wait timeout", {"shard", static_cast<int64_t>(f.shard)},
                     {"seq", static_cast<uint64_t>(f.seq)},
                     {"sub_index", static_cast<uint64_t>(i)});
      return core::RespValue::Error(
          core::ErrorPrefix::kErr,
          "fan-out write durable wait exceeded server timeout; writes will apply on consumer "
          "catch-up");
    }
    auto durable = f.durable_future.get();
    if (!durable.has_value()) {
      cancel_remaining(i);
      ABYSS_LOG_ERROR("fan-out write durable failed", {"shard", static_cast<int64_t>(f.shard)},
                      {"seq", static_cast<uint64_t>(f.seq)},
                      {"err", std::string_view{durable.error().message()}});
      return std::unexpected(durable.error());
    }
  }

  const auto now = std::chrono::steady_clock::now();
  const auto min_rpc_budget = std::chrono::milliseconds{static_cast<int64_t>(
      static_cast<double>(config_.write_timeout.count()) * config_.min_rpc_wait_fraction)};
  const auto rpc_deadline = std::max(durable_deadline, now + min_rpc_budget);

  std::vector<core::RespValue> per_sub;
  per_sub.reserve(in_flight.size());
  for (size_t i = 0; i < in_flight.size(); ++i) {
    auto& f = in_flight[i];
    if (f.rpc_future.wait_until(rpc_deadline) == std::future_status::timeout) {
      cancel_remaining(i);
      ABYSS_LOG_WARN(
          "fan-out write consumer apply timeout", {"shard", static_cast<int64_t>(f.shard)},
          {"seq", static_cast<uint64_t>(f.seq)}, {"sub_index", static_cast<uint64_t>(i)});
      return core::RespValue::Error(
          core::ErrorPrefix::kErr,
          "fan-out write durable in queue but consumer did not apply within timeout");
    }
    per_sub.push_back(f.rpc_future.get());
  }
  return core::RespValue::Array(std::move(per_sub));
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
