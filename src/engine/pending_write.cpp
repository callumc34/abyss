#include "abyss/engine/pending_write.h"

#include <span>
#include <utility>

#include "abyss/core/fatal.h"

namespace abyss::engine {

namespace {

// Touches nothing but the copy it was given.
void PostResume(const core::Completion completion, core::Result<void> result) noexcept {
  const core::CompletionEvent event{.client_id = completion.client_id,
                                    .slot_seq = completion.slot_seq,
                                    .kind = core::CompletionKind::kResume,
                                    .result = std::move(result)};
  completion.sink->Post(std::span(&event, 1));
}

}  // namespace

PendingWrite::PendingWrite(core::RespCommand cmd, core::PredicateFlags flags,
                           core::Completion completion, core::SteadyTime deadline)
    : cmd_(std::move(cmd)),
      flags_(flags),
      flush_(false),
      completion_(completion),
      deadline_(deadline),
      owner_(std::this_thread::get_id()),
      load_wake_(completion) {}

PendingWrite::PendingWrite(FlushAll /*flush*/, core::Completion completion,
                           core::SteadyTime deadline)
    : flush_(true),
      completion_(completion),
      deadline_(deadline),
      owner_(std::this_thread::get_id()),
      load_wake_(completion) {}

PendingWrite::~PendingWrite() {
  const bool cleaned = !waiting_ && owned_.empty() && !node_.has_value();
  ABYSS_DCHECK(cleaned, "a write destroyed with a wait or a load not cleaned up");
}

void PendingWrite::Woken(core::Result<void> result) {
  ABYSS_DCHECK(owner_ == std::this_thread::get_id(), "a wake taken off its write's owner thread");
  ABYSS_DCHECK(waiting_, "a wake for a write not waiting");
  ABYSS_DCHECK(!wake_.has_value(), "a second wake for one wait");
  wake_ = std::move(result);
}

void PendingWrite::Wake(core::Result<void> result) noexcept {
  PostResume(completion_, std::move(result));
}

void PendingWrite::LoadWake::Wake(core::Result<void> result) noexcept {
  PostResume(completion_, std::move(result));
}

ShardSeq PendingWrite::DurableAt(size_t i) const {
  if (durable_kind_ == DurableKind::kFence) return fences_.at(i);
  return {.shard = ranges_.at(i).shard, .seq = ranges_.at(i).last};
}

size_t PendingWrite::DurableCount() const {
  return durable_kind_ == DurableKind::kFence ? fences_.size() : ranges_.size();
}

}  // namespace abyss::engine
