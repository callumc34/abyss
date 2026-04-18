#include "abyss/engine/tiering_engine.h"

#include "abyss/core/ops.h"
#include "abyss/hot/shard_router.h"

namespace abyss::engine {

TieringEngine::TieringEngine(core::Queue& queue, core::HotStore& hot_store,
                             core::ColdStore& cold_store,
                             consumer::CompactionBufferRouter& buffer_router,
                             core::ConsumerRpc& rpc, uint32_t shard_count)
    : queue_(queue),
      hot_store_(hot_store),
      cold_store_(cold_store),
      buffer_router_(buffer_router),
      rpc_(rpc),
      shard_count_(shard_count) {}

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

  return cold_store_.Exec(*op);
}

core::Result<core::RespValue> TieringEngine::DispatchWrite(std::string_view /*name*/,
                                                           core::RespCommand cmd) {
  core::ShardId shard = 0;
  if (cmd.args.size() > 1) {
    shard = hot::ComputeShard(cmd.args[1], shard_count_);
  }

  core::QueueEntry entry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = std::move(cmd)},
  };

  auto appended = queue_.Append(shard, std::move(entry));
  if (!appended.has_value()) {
    return std::unexpected(appended.error());
  }

  auto durable = appended->durable.get();
  if (!durable.has_value()) {
    return std::unexpected(durable.error());
  }

  return core::RespValue::SimpleString("OK");
}

}  // namespace abyss::engine
