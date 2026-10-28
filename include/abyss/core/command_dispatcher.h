#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "abyss/core/completion.h"
#include "abyss/core/durable_wait.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

// Identifies multi-key Redis commands that the engine decomposes into per-key
// sub-commands before queueing. The dispatch class (read vs write) is still
// carried by the registry; this is the orthogonal aggregator selector.
enum class MultiKeyKind : uint8_t {
  kNone,
  kMget,
  kExists,
  kMset,
  kDelete,
};

// One resolved command for the engine.
struct Request {
  enum class Kind : uint8_t { kRead, kWrite, kFanOut, kFlush };
  Kind kind = Kind::kRead;
  // The registry's name for it, which outlives the request.
  std::string_view name;
  MultiKeyKind multi_key = MultiKeyKind::kNone;
  // The extracted predicates of a write; kNone when it has none.
  PredicateFlags flags = PredicateFlags::kNone;
  RespCommand cmd;
};

struct Outcome {
  enum class Kind : uint8_t {
    // Done, with `reply`.
    kReplied,
    // Linearised. The slot's durable wait holds the reply, and its
    // kDurable posts release it.
    kSequenced,
    // Not linearised: the task's kResume comes, or the deadline.
    kPending,
  };
  Kind kind = Kind::kReplied;
  Result<RespValue> reply;
};

// One command in flight, the caller's: where its events go, its
// deadline, which Dispatch sets, and its durable wait once sequenced.
// Reused for later commands once replied and quiescent.
struct Slot {
  Completion completion;
  SteadyTime deadline{};
  DurableWait durable;
};

// A command's state until it is linearised: a write between attempts,
// or a read miss on the I/O pool. The caller owns one and reuses it for
// each command it dispatches, one at a time. Run, Abandon and the
// deadline all happen on the caller's thread, which alone decides what
// becomes of the command.
class Task {
 public:
  Task() = default;
  virtual ~Task() = default;

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  Task(Task&&) = delete;
  Task& operator=(Task&&) = delete;

  // A kResume for `slot`, the command this task holds: it goes on from
  // its wait. Once abandoned, it only cleans up, and its outcome is
  // to be ignored.
  virtual Outcome Run(Slot& slot, Result<void> wake) = 0;
  // At the deadline of a kPending command: its timeout reply. It will
  // never apply, but stays busy until a wake or a job already on its
  // way arrives through Run.
  virtual Result<RespValue> Abandon(Slot& slot) = 0;
  // Linked on no wait list, owed no wake, and with no job or load
  // placeholder outstanding: safe to reuse or free.
  virtual bool Quiescent() const = 0;
};

class CommandDispatcher {
 public:
  CommandDispatcher() = default;
  virtual ~CommandDispatcher() = default;
  CommandDispatcher(const CommandDispatcher&) = delete;
  CommandDispatcher& operator=(const CommandDispatcher&) = delete;
  CommandDispatcher(CommandDispatcher&&) = delete;
  CommandDispatcher& operator=(CommandDispatcher&&) = delete;

  // Runs `request` on the caller's thread until it replies, is
  // sequenced, or must wait. `task` is the caller's, quiescent here;
  // the dispatcher creates it the first time a command needs one.
  virtual Outcome Dispatch(Request request, Slot& slot, std::unique_ptr<Task>& task) = 0;
};

}  // namespace abyss::core
