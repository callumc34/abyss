#include "abyss/engine/tiering_engine.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <utility>

#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"

namespace abyss::engine {

namespace {
const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.engine");
  return l;
}
}  // namespace

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
    ABYSS_LOG_WARN(Log(), "promotion append failed", {"shard", static_cast<int64_t>(shard)},
                   {"key_hash", log::KeyHash(key)},
                   {"err", std::string_view{appended.error().message()}});
  }
}

TieringEngineMetrics TieringEngine::Snapshot() const {
  return TieringEngineMetrics{
      .promotion_append_failures = promotion_append_failures_.load(std::memory_order_relaxed),
  };
}

core::Result<core::RespValue> TieringEngine::DispatchWrite(std::string_view /*name*/,
                                                           core::RespCommand cmd) {
  core::ShardId shard = 0;
  if (cmd.args.size() > 1) {
    shard = core::ComputeShard(cmd.args[1], config_.shard_count);
  }

  core::QueueEntry entry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = std::move(cmd)},
  };

  auto pending = queue_.BeginAppend(shard, std::move(entry));
  if (!pending.has_value()) {
    return std::unexpected(pending.error());
  }
  const core::SequenceId seq = pending->seq();
  auto rpc_future = rpc_.Register(seq);
  queue::DurabilityFuture durable_future = std::move(pending->durable());
  pending->Publish();

  const auto durable_deadline = std::chrono::steady_clock::now() + config_.write_timeout;

  // fsync first: a durable-layer failure takes precedence over consumer error.
  if (durable_future.wait_until(durable_deadline) == std::future_status::timeout) {
    rpc_.Cancel(seq);
    ABYSS_LOG_WARN(Log(), "write durable wait timeout", {"shard", static_cast<int64_t>(shard)},
                   {"seq", static_cast<uint64_t>(seq)},
                   {"timeout_ms", static_cast<int64_t>(config_.write_timeout.count())});
    return core::RespValue::Error(
        core::ErrorPrefix::kErr,
        "write durable wait exceeded server timeout; write will apply on consumer catch-up");
  }
  auto durable = durable_future.get();
  if (!durable.has_value()) {
    rpc_.Cancel(seq);
    ABYSS_LOG_ERROR(Log(), "write durable failed", {"shard", static_cast<int64_t>(shard)},
                    {"seq", static_cast<uint64_t>(seq)},
                    {"err", std::string_view{durable.error().message()}});
    return std::unexpected(durable.error());
  }

  const auto now = std::chrono::steady_clock::now();
  const auto min_rpc_budget = std::chrono::milliseconds{static_cast<int64_t>(
      static_cast<double>(config_.write_timeout.count()) * config_.min_rpc_wait_fraction)};
  const auto rpc_deadline = std::max(durable_deadline, now + min_rpc_budget);

  if (rpc_future.wait_until(rpc_deadline) == std::future_status::timeout) {
    rpc_.Cancel(seq);
    ABYSS_LOG_WARN(Log(), "write durable but consumer apply timeout",
                   {"shard", static_cast<int64_t>(shard)}, {"seq", static_cast<uint64_t>(seq)});
    return core::RespValue::Error(
        core::ErrorPrefix::kErr,
        "write durable in queue but consumer did not apply within timeout");
  }
  return rpc_future.get();
}

}  // namespace abyss::engine
