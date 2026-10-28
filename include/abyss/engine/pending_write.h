#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"
#include "abyss/engine/decide.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"
#include "abyss/queue/reservation.h"

namespace abyss::engine {

class Sequencer;

// What a write waits on before it is linearised.
enum class WaitReason : uint8_t {
  // Room in a shard's durability window.
  kAdmission,
  // A spare segment on a shard's log.
  kSpare,
  // A shard under its memory limit: cold must drain for hot to evict.
  kBackpressure,
  // Another caller's load of a key.
  kForeignLoad,
  // Its own loads, which Sequencer::Load runs.
  kLoads,
};

// What one Sequencer::Attempt came to.
struct Step {
  enum class Kind : uint8_t {
    // Done, with `reply`.
    kReplied,
    // Linearised: Sequencer::Sequence hands its reply to a durable
    // wait, released once what it shows is durable.
    kSequenced,
    // Not linearised: registered on what `reason` names, or for kLoads
    // waiting for its loads to run.
    kWait,
  };
  Kind kind = Kind::kReplied;
  core::Result<core::RespValue> reply;
  WaitReason reason = WaitReason::kLoads;
  // Admission, spare and backpressure: the shard waited on.
  core::ShardId shard = 0;
  // kForeignLoad: the key whose load it waits on.
  std::string_view key;
};

// A write between attempts: everything the write loop carries from one
// pass to the next. It belongs to one owner thread, which runs every
// Attempt; a wait's wake only posts kResume to the owner's sink, and
// the owner hands its result back through Woken before the next one.
// Reset for each write it carries, it keeps what it has grown.
class PendingWrite final : public core::Waiter {
 public:
  struct FlushAll {};

  PendingWrite() = default;
  PendingWrite(core::RespCommand cmd, core::PredicateFlags flags, core::Completion completion,
               core::SteadyTime deadline);
  PendingWrite(FlushAll flush, core::Completion completion, core::SteadyTime deadline);
  ~PendingWrite() override;

  PendingWrite(const PendingWrite&) = delete;
  PendingWrite& operator=(const PendingWrite&) = delete;
  PendingWrite(PendingWrite&&) = delete;
  PendingWrite& operator=(PendingWrite&&) = delete;

  // A new write, on the calling thread, which owns it from here; only
  // once the last is quiescent.
  void Reset(core::RespCommand cmd, core::PredicateFlags flags, core::Completion completion,
             core::SteadyTime deadline);
  void Reset(FlushAll flush, core::Completion completion, core::SteadyTime deadline);

  core::SteadyTime deadline() const { return deadline_; }
  const core::Completion& completion() const { return completion_; }
  // Waiting for its own loads, which a job runs.
  bool loading() const { return waiting_ && wait_reason_ == WaitReason::kLoads; }
  // Nothing linked, no wake owed, no load placeholder held.
  bool Quiescent() const { return !waiting_ && owned_.empty(); }
  // The posted wake's result, on the owner's thread.
  void Woken(core::Result<void> result);

  void Wake(core::Result<void> result) noexcept override;

 private:
  friend class Sequencer;

  enum class Phase : uint8_t {
    kStart,
    kAdmit,
    kSpare,
    // The loop's top: the deadline, memory, then a hold and a decision.
    kTop,
    kForeign,
    kLoads,
    kLoaded,
    // Linearised: Sequence hands its durable wait to a slot.
    kSequenced,
    kDone,
  };
  enum class DurableKind : uint8_t { kWrite, kFlush, kFence };

  // Large arguments copied for the log before any lock, once; `lent`
  // marks a copy an attempt's log entry holds, to take back if it fails.
  struct Spare {
    std::optional<std::string> copy;
    bool lent = false;
  };
  struct Owned {
    std::string key;
    core::ShardId shard;
    Need need;
    hot::LoadToken token;
  };
  struct PhaseTimes {
    bool sampled = false;
    std::chrono::steady_clock::time_point last;
    std::array<std::chrono::steady_clock::duration, metrics::kWritePhaseCount> spent{};
    std::array<bool, metrics::kWritePhaseCount> passed{};
  };
  // Another caller's load ending posts as the write's own wake does.
  class LoadWake final : public hot::LoadWaiter {
   public:
    void Wake(core::Result<void> result) noexcept override;

    // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
    core::Completion completion;
  };

  std::optional<core::Result<void>> TakeWake() { return std::exchange(wake_, std::nullopt); }
  ShardSeq DurableAt(size_t i) const;
  size_t DurableCount() const;

  core::RespCommand cmd_;
  core::PredicateFlags flags_ = core::PredicateFlags::kNone;
  bool flush_ = false;
  core::Completion completion_;
  core::SteadyTime deadline_{};
  std::thread::id owner_;
  Phase phase_ = Phase::kDone;
  PhaseTimes phase_times_;

  std::vector<core::ShardId> shards_;
  std::vector<Spare> spare_;
  bool grows_ = false;
  bool await_memory_ = false;
  std::optional<metrics::RedecideReason> redecided_;

  // The wait the last Attempt returned, and how it ended: the posted
  // wake's result, or a Cancel at the deadline.
  bool waiting_ = false;
  WaitReason wait_reason_ = WaitReason::kLoads;
  core::ShardId wait_shard_ = 0;
  std::optional<core::Result<void>> wake_;
  bool expired_ = false;
  // Past its deadline with its wake or loads still on the way: its
  // timeout reply is given, and the Attempt they run only cleans up.
  bool abandoned_ = false;

  size_t admit_next_ = 0;
  // A refused Reserve's error, the reply if the spare wait fails early.
  std::optional<core::Error> refusal_;
  size_t spare_next_ = 0;
  std::vector<uint32_t> spare_logs_;
  size_t memory_next_ = 0;
  bool memory_waited_ = false;

  std::vector<Owned> owned_;
  std::vector<std::string> foreign_;
  size_t foreign_next_ = 0;
  bool full_load_ = false;
  std::vector<hot::LoadCompletion> completions_;
  std::optional<core::Error> load_error_;
  LoadWake load_wake_;

  core::Result<core::RespValue> reply_;
  DurableKind durable_kind_ = DurableKind::kWrite;
  // Whether a durable reply records its phases: not an error's.
  bool record_phases_ = false;
  std::vector<queue::ReservedRange> ranges_;
  std::vector<ShardSeq> fences_;
};

}  // namespace abyss::engine
