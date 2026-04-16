#include "abyss/engine/tiering_engine.h"

namespace abyss::engine {

TieringEngine::TieringEngine(core::Queue& queue, core::HotStore& hot_store,
                             core::ColdStore& cold_store, consumer::CompactionBuffer& buffer)
    : queue_(queue), hot_store_(hot_store), cold_store_(cold_store), buffer_(buffer) {}

core::Result<core::RespValue> TieringEngine::HandleRead(const core::RespCommand& cmd) {
  // Hot -> Buffer -> Cold -> nil
  auto hot_result = hot_store_.Exec(cmd);
  if (hot_result.has_value()) {
    return hot_result;
  }

  // Extract key from command (args[1] for most commands)
  if (cmd.ArgCount() < 2) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument, "missing key"));
  }

  auto buffer_result = buffer_.Read(cmd.args[1]);
  if (buffer_result.has_value()) {
    return buffer_result;
  }

  return cold_store_.Exec(cmd);
}

core::Result<core::RespValue> TieringEngine::HandleWrite(core::ShardId shard,
                                                         core::RespCommand cmd) {
  auto seq = queue_.Append(shard, std::move(cmd));
  if (!seq.has_value()) {
    return std::unexpected(seq.error());
  }
  return core::RespValue::SimpleString("OK");
}

}  // namespace abyss::engine
