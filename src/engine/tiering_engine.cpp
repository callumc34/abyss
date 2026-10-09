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
#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.engine")

namespace abyss::engine {

TieringEngine::TieringEngine(hot::ShardedHotStore& hot_store, core::ColdStore& cold_store,
                             consumer::CompactionBufferRouter& buffer_router, Sequencer& sequencer,
                             TieringEngineConfig config)
    : hot_store_(hot_store),
      cold_store_(cold_store),
      buffer_router_(buffer_router),
      sequencer_(sequencer),
      config_(config) {
  auto& reg = metrics::Registry::Instance();
  hits_hot_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kHot);
  hits_buffer_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kBuffer);
  hits_cold_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kCold);
  misses_ = reg.Counter(metrics::names::kMissesTotal);
  cold_scan_deadline_exceeded_ = reg.Counter(metrics::names::kColdScanDeadlineExceededTotal);
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

// Set/zset SCALAR reads the compaction buffer can answer authoritatively:
// the buffer overlay holds the exact (member, score, cardinality) delta for a
// hot-evicted collection. Routed through MergeCollectionScalar so the buffer
// tier is never skipped (ENGINE-2).
bool IsCollectionScalarRead(const core::ops::ReadOp& op) {
  return std::holds_alternative<core::ops::SetIsMember>(op) ||
         std::holds_alternative<core::ops::SetCard>(op) ||
         std::holds_alternative<core::ops::ZsetScore>(op) ||
         std::holds_alternative<core::ops::ZsetCard>(op);
}

// Cold reads that scan a whole collection rather than a single point. These get
// the larger cold_scan_deadline (COLD-2); their latency scales with cardinality.
bool IsCollectionScan(const core::ops::ReadOp& op) {
  return std::holds_alternative<core::ops::SetMembers>(op) ||
         std::holds_alternative<core::ops::ZsetRange>(op) ||
         std::holds_alternative<core::ops::HashGetAll>(op) ||
         std::holds_alternative<core::ops::HashKeys>(op) ||
         std::holds_alternative<core::ops::HashVals>(op);
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

core::Duration TieringEngine::ColdDeadlineFor(const core::ops::ReadOp& op) const {
  return IsCollectionScan(op) ? config_.cold_scan_deadline : config_.cold_read_deadline;
}

core::Result<core::RespValue> TieringEngine::ColdExec(const core::ops::ReadOp& op) {
  auto result = cold_store_.Exec(op, ColdDeadlineFor(op));
  if (!result.has_value() && result.error().code() == core::ErrorCode::kTimeout &&
      IsCollectionScan(op)) {
    // A cold scan exceeded its deadline. Surface the error to the client and a
    // metric so an operator sees a legitimately-large collection being capped;
    // never serve a silently truncated partial result (decision 4).
    cold_scan_deadline_exceeded_.Increment();
  }
  return result;
}

core::Result<core::RespValue> TieringEngine::MergeCollectionScalar(const core::ops::ReadOp& op) {
  // Buffer overlay first: a buffered partial SREM/ZREM (or a tombstone) against
  // a cold-resident collection must win over cold's stale residual
  // (read-after-write). Mirrors DispatchHashRead's kWrongType short-circuit.
  auto buffered = buffer_router_.Exec(op);
  if (buffered.has_value()) {
    hits_buffer_.Increment();
    return buffered;
  }
  if (buffered.error().code() == core::ErrorCode::kWrongType) {
    return core::RespValue::Error(core::ErrorPrefix::kWrongType,
                                  "Operation against a key holding the wrong kind of value");
  }
  if (buffered.error().code() != core::ErrorCode::kNotFound) {
    return std::unexpected(buffered.error());
  }

  // True buffer miss: fall through to cold with the point-read deadline.
  auto cold_result = ColdExec(op);
  if (cold_result.has_value() && !cold_result->IsNull()) {
    hits_cold_.Increment();
  } else if (cold_result.has_value()) {
    misses_.Increment();
  }
  return cold_result;
}

core::Result<core::RespValue> TieringEngine::Fenced(core::ShardId shard,
                                                    hot::ShardedHotStore::HotRead read) {
  if (read.fence.has_value()) {
    const ShardSeq fence{.shard = shard, .seq = *read.fence};
    // Hot shows a write once it is applied, which precedes its publish.
    if (auto fenced = sequencer_.Fence(std::span(&fence, 1),
                                       core::SteadyClock::now() + config_.write_timeout);
        !fenced) {
      return std::unexpected(fenced.error());
    }
  }
  return std::move(read.result);
}

core::Result<core::RespValue> TieringEngine::DispatchSingleKeyRead(const core::ops::ReadOp& op) {
  const auto key = core::ops::PrimaryKey(op);
  auto hot = hot_store_.Read(op);
  if (hot.result.has_value() || hot.result.error().code() != core::ErrorCode::kNotFound) {
    if (hot.result.has_value()) hits_hot_.Increment();
    return Fenced(hot_store_.ShardOf(key), std::move(hot));
  }

  // Hot holds none of the key's state, so buffer plus cold hold all of
  // it: nothing to wait for.
  if (IsHashRead(op)) {
    return DispatchHashRead(op);
  }

  // Set/zset scalar reads go through the uniform overlay (buffer first, then
  // cold-with-deadline) so the buffer tier is never skipped (ENGINE-2).
  if (IsCollectionScalarRead(op)) {
    return MergeCollectionScalar(op);
  }

  // Remaining shapes (string GET, full-collection scans): the buffer answers
  // string reads via the StringGet fast path; scans defer to cold.
  if (!key.empty()) {
    auto buffer_result = buffer_router_.Read(key);
    if (buffer_result.has_value()) {
      hits_buffer_.Increment();
      return buffer_result;
    }
  }

  auto cold_result = ColdExec(op);
  if (cold_result.has_value() && !cold_result->IsNull()) {
    hits_cold_.Increment();
  } else if (cold_result.has_value()) {
    misses_.Increment();
  }
  return cold_result;
}

core::Result<core::RespValue> TieringEngine::DispatchHashRead(const core::ops::ReadOp& op) {
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
      auto cold = ColdExec(op);
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
    auto r = ColdExec(op);
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
    auto r = ColdExec(op);
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
      auto cold_one = ColdExec(core::ops::ReadOp{core::ops::HashGet{
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
  auto cold_all = ColdExec(core::ops::ReadOp{core::ops::HashGetAll{.key = key}});
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
  // Hot is authoritative for what it holds, tombstones included.
  auto hot = hot_store_.Read(core::ops::ReadOp{core::ops::Exists{.keys = {key}}});
  if (hot.result.has_value()) {
    auto present = Fenced(hot_store_.ShardOf(key), std::move(hot));
    if (!present.has_value()) return std::unexpected(present.error());
    return present->AsInteger() > 0;
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

  auto cold = ColdExec(core::ops::ReadOp{core::ops::Exists{.keys = {key}}});
  if (!cold.has_value()) {
    if (cold.error().code() == core::ErrorCode::kNotFound) return false;
    return std::unexpected(cold.error());
  }
  return cold->IsInteger() && cold->AsInteger() > 0;
}

core::Result<core::RespValue> TieringEngine::DispatchFlush(core::FlushTarget /*target*/) {
  return sequencer_.Flush();
}

core::Result<core::RespValue> TieringEngine::DispatchWrite(std::string_view /*name*/,
                                                           core::RespCommand cmd) {
  return sequencer_.Execute(std::move(cmd), core::PredicateFlags::kNone);
}

core::Result<core::RespValue> TieringEngine::DispatchConditional(std::string_view /*name*/,
                                                                 core::RespCommand cmd,
                                                                 core::PredicateFlags flags) {
  return sequencer_.Execute(std::move(cmd), flags);
}

core::Result<core::RespValue> TieringEngine::DispatchFanOut(core::MultiKeyKind kind,
                                                            core::RespCommand cmd) {
  // See ADP-005 §Multi-Key Commands; ADP-006 §Multi-Key Fan-Out. Reads
  // stay per key (#170); writes are one atomic decision.
  switch (kind) {
    case core::MultiKeyKind::kMget:
      return FanOutMget(cmd);
    case core::MultiKeyKind::kExists:
      return FanOutExists(cmd);
    case core::MultiKeyKind::kMset:
    case core::MultiKeyKind::kDelete:
      return sequencer_.Execute(std::move(cmd), core::PredicateFlags::kNone);
    case core::MultiKeyKind::kNone:
      break;
  }
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "DispatchFanOut called with MultiKeyKind::kNone"));
}

}  // namespace abyss::engine
