#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <thread>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/core/queue.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/decide.h"
#include "abyss/engine/io_pool.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/pending_write.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"

namespace abyss::engine {

// `reply`, once what it showed is durable: now, or the slot's durable
// wait holds it (kSequenced).
core::Outcome Fenced(core::Queue& queue, core::Slot& slot, core::Result<core::RespValue> reply,
                     std::span<const ShardSeq> fences);

// The engine's core::Task, one per caller and reused for each command
// that needs one: a write between attempts, a read miss on the I/O
// pool, or an MGET or EXISTS. Its pool job lives in it, so no command
// allocates a task or a job once the first has.
//
// Quiescent once nothing outside refers to it: its write is linked on
// no wait list, owes no wake and holds no load placeholder, no job of
// it is queued or running (job_ until the job's post is Run), and no
// load in flight will land in its flight join (armed_).
class CommandTask final : public core::Task {
 public:
  CommandTask(Sequencer& sequencer, ReadPath& reads, IoPool& pool);
  ~CommandTask() override;

  CommandTask(const CommandTask&) = delete;
  CommandTask& operator=(const CommandTask&) = delete;
  CommandTask(CommandTask&&) = delete;
  CommandTask& operator=(CommandTask&&) = delete;

  core::Outcome Write(core::Slot& slot, core::RespCommand cmd, core::PredicateFlags flags);
  core::Outcome Flush(core::Slot& slot);
  // A single-key read that missed hot: `name` parses `cmd` again here.
  core::Outcome Miss(core::Slot& slot, std::string_view name, core::RespCommand cmd);
  core::Outcome FanOut(core::Slot& slot, core::MultiKeyKind kind, core::RespCommand cmd);

  core::Outcome Run(core::Slot& slot, core::Result<void> wake) override;
  core::Result<core::RespValue> Abandon(core::Slot& slot) override;
  bool Quiescent() const override { return !job_ && !armed_ && write_.Quiescent(); }

 private:
  enum class Mode : uint8_t { kIdle, kWrite, kMiss, kFanOut };

  // The task's job on the pool: its write's loads, or its read's
  // answer, written into the task. Its post is its last act.
  class Job final : public IoJob {
   public:
    explicit Job(CommandTask& task) : task_(task) {}
    void Run() noexcept override;
    void Fail(core::Error error) noexcept override;
    // What a job that never ran still releases: its write's load
    // placeholders, so the key's waiters go on (a read's job holds
    // none, and no flight is led by a job that never ran). Fail's,
    // and a true Cancel's on the owner's thread.
    void Unrun(const core::Error& error) noexcept;

   private:
    CommandTask& task_;
  };

  void Begin(const core::Slot& slot, Mode mode);
  core::Outcome Drive(core::Slot& slot);
  core::Outcome Submit(metrics::IoKind kind);
  core::Outcome Answered(core::Slot& slot);
  // An abandoned command's late post: it cleans up and goes idle,
  // unless a load it joined is still to land.
  void Settle(core::Result<void> wake, bool from_job);
  // After a job that joined a load in flight: true while it waits for
  // the landing, which posts; false once landed.
  bool Waiting();
  // The joined load's landing, taken: the miss's answer, or the
  // fan-out's next step.
  core::Outcome Joined(core::Slot& slot);
  void Post() const noexcept;

  Sequencer& sequencer_;
  ReadPath& reads_;
  IoPool& pool_;
  Mode mode_ = Mode::kIdle;
  core::Completion completion_;
  core::SteadyTime deadline_{};
  std::thread::id owner_;
  PendingWrite write_;
  // A read's own command, which op_ and fan_ view.
  core::RespCommand cmd_;
  std::optional<core::ops::ReadOp> op_;
  ReadAnswer answer_;
  engine::FanOut fan_;
  // A read's join of another's load in flight; joining_ is set by the
  // job that joined it, before its post.
  Loader::FlightJoin flight_;
  bool joining_ = false;
  // A job is queued or running, or its post is on the way.
  bool job_ = false;
  // Armed on a load in flight, whose landing posts.
  bool armed_ = false;
  // Past its deadline: replied, and only its late post is to come.
  bool abandoned_ = false;
  Job io_{*this};
};

}  // namespace abyss::engine
