#include "abyss/engine/pending_write.h"

#include <cstddef>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/fatal.h"

namespace abyss::engine {

namespace {

// Storage a write keeps for the next, at most: a huge command's is
// let go rather than held by its connection.
constexpr size_t kKeptElements = 1024;

template <typename T>
void Empty(std::vector<T>& v) {
  v.clear();
  if (v.capacity() > kKeptElements) v.shrink_to_fit();
}

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
                           core::Completion completion, core::SteadyTime deadline) {
  Reset(std::move(cmd), flags, completion, deadline);
}

PendingWrite::PendingWrite(FlushAll flush, core::Completion completion, core::SteadyTime deadline) {
  Reset(flush, completion, deadline);
}

PendingWrite::~PendingWrite() {
  ABYSS_DCHECK(Quiescent(), "a write destroyed with a wait or a load not cleaned up");
}

void PendingWrite::Reset(core::RespCommand cmd, core::PredicateFlags flags,
                         core::Completion completion, core::SteadyTime deadline) {
  Reset(FlushAll{}, completion, deadline);
  cmd_ = std::move(cmd);
  flags_ = flags;
  flush_ = false;
}

void PendingWrite::Reset(FlushAll /*flush*/, core::Completion completion,
                         core::SteadyTime deadline) {
  ABYSS_DCHECK(Quiescent(), "a write reset with a wait or a load not cleaned up");
  cmd_.args.clear();
  flags_ = core::PredicateFlags::kNone;
  flush_ = true;
  completion_ = completion;
  deadline_ = deadline;
  owner_ = std::this_thread::get_id();
  phase_ = Phase::kStart;
  phase_times_ = {};
  Empty(shards_);
  Empty(spare_);
  grows_ = false;
  await_memory_ = false;
  redecided_.reset();
  waiting_ = false;
  wait_reason_ = WaitReason::kLoads;
  wait_shard_ = 0;
  wake_.reset();
  expired_ = false;
  abandoned_ = false;
  admit_next_ = 0;
  refusal_.reset();
  spare_next_ = 0;
  spare_logs_.clear();
  memory_next_ = 0;
  memory_waited_ = false;
  foreign_.clear();
  foreign_next_ = 0;
  full_load_ = false;
  Empty(completions_);
  Empty(owned_);
  load_error_.reset();
  load_wake_.completion = completion;
  reply_ = {};
  durable_kind_ = DurableKind::kWrite;
  record_phases_ = false;
  ranges_.clear();
  fences_.clear();
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
  PostResume(completion, std::move(result));
}

ShardSeq PendingWrite::DurableAt(size_t i) const {
  if (durable_kind_ == DurableKind::kFence) return fences_.at(i);
  return {.shard = ranges_.at(i).shard, .seq = ranges_.at(i).last};
}

size_t PendingWrite::DurableCount() const {
  return durable_kind_ == DurableKind::kFence ? fences_.size() : ranges_.size();
}

}  // namespace abyss::engine
