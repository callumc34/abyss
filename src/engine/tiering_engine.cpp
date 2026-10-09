#include "abyss/engine/tiering_engine.h"

#include <span>
#include <utility>

#include "abyss/core/ops.h"

namespace abyss::engine {

TieringEngine::TieringEngine(ReadPath& reads, Sequencer& sequencer)
    : reads_(reads), sequencer_(sequencer) {}

core::Result<core::RespValue> TieringEngine::DispatchRead(std::string_view name,
                                                          const core::RespCommand& cmd) {
  auto op = core::ops::ParseReadOp(name, cmd);
  if (!op.has_value()) {
    return std::unexpected(op.error());
  }
  return reads_.Read(*op);
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
  const auto keys = std::span(cmd.args).subspan(cmd.args.empty() ? 0 : 1);
  switch (kind) {
    case core::MultiKeyKind::kMget:
      if (keys.empty()) {
        return std::unexpected(
            core::Error(core::ErrorCode::kInvalidArgument, "MGET requires at least one key"));
      }
      return reads_.Mget(keys);
    case core::MultiKeyKind::kExists:
      if (keys.empty()) {
        return std::unexpected(
            core::Error(core::ErrorCode::kInvalidArgument, "EXISTS requires at least one key"));
      }
      return reads_.Exists(keys);
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
