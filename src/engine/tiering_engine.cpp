#include "abyss/engine/tiering_engine.h"

#include <optional>
#include <span>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"

namespace abyss::engine {

TieringEngine::TieringEngine(ReadPath& reads, Sequencer& sequencer, IoPool& pool)
    : reads_(reads), sequencer_(sequencer), pool_(pool) {}

core::Outcome TieringEngine::Dispatch(core::Request request, core::Slot& slot,
                                      std::unique_ptr<core::Task>& task) {
  slot.deadline = core::SteadyClock::now() + sequencer_.config().command_timeout;
  switch (request.kind) {
    case core::Request::Kind::kRead: {
      auto op = core::ops::ParseReadOp(request.name, request.cmd);
      if (!op.has_value()) return {.reply = std::unexpected(op.error())};
      std::optional<ReadAnswer> hit = reads_.TryHot(*op);
      if (!hit.has_value()) return TaskOf(task).Miss(slot, request.name, std::move(request.cmd));
      const std::span<const ShardSeq> fences = hit->fence.has_value()
                                                   ? std::span<const ShardSeq>(&*hit->fence, 1)
                                                   : std::span<const ShardSeq>();
      return Fenced(sequencer_.queue(), slot, std::move(hit->reply), fences);
    }
    case core::Request::Kind::kWrite:
      return TaskOf(task).Write(slot, std::move(request.cmd), request.flags);
    case core::Request::Kind::kFanOut:
      return FanOut(std::move(request), slot, task);
    case core::Request::Kind::kFlush:
      return TaskOf(task).Flush(slot);
  }
  return {.reply = std::unexpected(core::Error(core::ErrorCode::kInternal, "unknown request"))};
}

core::Outcome TieringEngine::FanOut(core::Request request, core::Slot& slot,
                                    std::unique_ptr<core::Task>& task) {
  // See ADP-005 §Multi-Key Commands; ADP-006 §Multi-Key Fan-Out. Reads
  // stay per key (#170); writes are one atomic decision.
  const bool keyless = request.cmd.args.size() < 2;
  switch (request.multi_key) {
    case core::MultiKeyKind::kMget:
      if (keyless) {
        return {.reply = std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                                     "MGET requires at least one key"))};
      }
      return TaskOf(task).FanOut(slot, request.multi_key, std::move(request.cmd));
    case core::MultiKeyKind::kExists:
      if (keyless) {
        return {.reply = std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                                     "EXISTS requires at least one key"))};
      }
      return TaskOf(task).FanOut(slot, request.multi_key, std::move(request.cmd));
    case core::MultiKeyKind::kMset:
    case core::MultiKeyKind::kDelete:
      return TaskOf(task).Write(slot, std::move(request.cmd), core::PredicateFlags::kNone);
    case core::MultiKeyKind::kNone:
      break;
  }
  return {.reply = std::unexpected(core::Error(core::ErrorCode::kInternal,
                                               "a fan-out request with MultiKeyKind::kNone"))};
}

CommandTask& TieringEngine::TaskOf(std::unique_ptr<core::Task>& task) {
  if (task == nullptr) task = std::make_unique<CommandTask>(sequencer_, reads_, pool_);
  ABYSS_DCHECK(dynamic_cast<CommandTask*>(task.get()) != nullptr,
               "a task this engine did not make");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): made here.
  return static_cast<CommandTask&>(*task);
}

}  // namespace abyss::engine
