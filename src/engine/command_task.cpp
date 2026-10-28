#include "abyss/engine/command_task.h"

#include <span>
#include <string>
#include <utility>

#include "abyss/core/fatal.h"

namespace abyss::engine {

namespace {

core::Error ReadTimedOut() {
  return core::Error{core::ErrorCode::kTimeout, "read timed out loading its key"};
}

}  // namespace

core::Outcome Fenced(core::Queue& queue, core::Slot& slot, core::Result<core::RespValue> reply,
                     std::span<const ShardSeq> fences) {
  if (fences.empty()) return {.reply = std::move(reply)};
  core::DurableWait& wait = slot.durable;
  wait.Begin(queue, core::DurableWait::Kind::kFence, slot.completion, fences.size(),
             std::move(reply));
  for (const ShardSeq& fence : fences) wait.Add(fence.shard, fence.seq);
  if (auto now = wait.Ready()) return {.reply = *std::move(now)};
  return {.kind = core::Outcome::Kind::kSequenced};
}

CommandTask::CommandTask(Sequencer& sequencer, ReadPath& reads, IoPool& pool)
    : sequencer_(sequencer), reads_(reads), pool_(pool) {}

CommandTask::~CommandTask() {
  ABYSS_DCHECK(Quiescent(), "a task freed with a wait, a job or a placeholder outstanding");
}

core::Outcome CommandTask::Write(core::Slot& slot, core::RespCommand cmd,
                                 core::PredicateFlags flags) {
  Begin(slot, Mode::kWrite);
  write_.Reset(std::move(cmd), flags, slot.completion, slot.deadline);
  return Drive(slot);
}

core::Outcome CommandTask::Flush(core::Slot& slot) {
  Begin(slot, Mode::kWrite);
  write_.Reset(PendingWrite::FlushAll{}, slot.completion, slot.deadline);
  return Drive(slot);
}

core::Outcome CommandTask::Miss(core::Slot& slot, std::string_view name, core::RespCommand cmd) {
  Begin(slot, Mode::kMiss);
  cmd_ = std::move(cmd);
  auto op = core::ops::ParseReadOp(name, cmd_);
  if (!op.has_value()) {
    mode_ = Mode::kIdle;
    return {.reply = std::unexpected(op.error())};
  }
  op_.emplace(*std::move(op));
  return Submit(ReadPath::KindOf(*op_));
}

core::Outcome CommandTask::FanOut(core::Slot& slot, core::MultiKeyKind kind,
                                  core::RespCommand cmd) {
  Begin(slot, Mode::kFanOut);
  cmd_ = std::move(cmd);
  const auto keys = std::span<const std::string>(cmd_.args).subspan(1);
  fan_.Reset(kind, keys);
  if (kind == core::MultiKeyKind::kMget) fan_.values.reserve(keys.size());
  reads_.FanOutHot(fan_);
  if (fan_.done()) return Answered(slot);
  return Submit(metrics::IoKind::kPoint);
}

core::Outcome CommandTask::Run(core::Slot& slot, core::Result<void> wake) {
  ABYSS_DCHECK(owner_ == std::this_thread::get_id(), "a task resumed off its owner's thread");
  const bool its_slot = slot.completion.slot_seq == completion_.slot_seq &&
                        slot.completion.client_id == completion_.client_id;
  ABYSS_DCHECK(its_slot, "a task resumed for another command's slot");
  ABYSS_DCHECK(mode_ != Mode::kIdle, "an idle task resumed");
  const bool from_job = std::exchange(job_, false);
  // Abandoned, it only cleans up: it is never applied.
  if (abandoned_) {
    Settle(std::move(wake), from_job);
    return {.reply = std::unexpected(ReadTimedOut())};
  }
  if (mode_ == Mode::kWrite) {
    if (!from_job) write_.Woken(std::move(wake));
    return Drive(slot);
  }
  return Answered(slot);
}

core::Result<core::RespValue> CommandTask::Abandon(core::Slot& /*slot*/) {
  ABYSS_DCHECK(owner_ == std::this_thread::get_id(), "a task abandoned off its owner's thread");
  const bool pending = mode_ != Mode::kIdle && !abandoned_;
  ABYSS_DCHECK(pending, "a task abandoned while not pending");
  // A job still queued is taken back: it never runs and never posts,
  // and what it would have released is released here, off the pool's
  // lock. One a thread has taken lands, and its post finds this.
  const bool cancelled = job_ && pool_.Cancel(io_);
  if (cancelled) {
    job_ = false;
    io_.Unrun(ReadTimedOut());
  }
  core::Result<core::RespValue> reply;
  if (mode_ == Mode::kWrite) {
    reply = sequencer_.Abandon(write_);
    // Its loads ended unrun: the attempt that ends its wait runs now.
    if (cancelled) (void)sequencer_.Attempt(write_);
  } else {
    reply = mode_ == Mode::kMiss && op_.has_value()
                ? reads_.Failed(*op_, ReadTimedOut())
                : std::unexpected(
                      core::Error{core::ErrorCode::kTimeout, "read timed out loading its keys"});
  }
  const bool owes = cancelled && !Quiescent();
  ABYSS_DCHECK(!owes, "a task owes a post after its job was cancelled");
  if (Quiescent()) {
    op_.reset();
    answer_ = {};
    mode_ = Mode::kIdle;
  } else {
    abandoned_ = true;
  }
  return reply;
}

void CommandTask::Begin(const core::Slot& slot, Mode mode) {
  const bool idle = Quiescent() && mode_ == Mode::kIdle;
  ABYSS_DCHECK(idle, "a command started on a busy task");
  mode_ = mode;
  completion_ = slot.completion;
  deadline_ = slot.deadline;
  owner_ = std::this_thread::get_id();
  abandoned_ = false;
}

core::Outcome CommandTask::Drive(core::Slot& slot) {
  Step step = sequencer_.Attempt(write_);
  switch (step.kind) {
    case Step::Kind::kReplied:
      mode_ = Mode::kIdle;
      return {.reply = std::move(step.reply)};
    case Step::Kind::kSequenced: {
      mode_ = Mode::kIdle;
      auto now = sequencer_.Sequence(write_, slot.durable, slot.completion);
      if (now.has_value()) return {.reply = *std::move(now)};
      return {.kind = core::Outcome::Kind::kSequenced};
    }
    case Step::Kind::kWait:
      if (step.reason == WaitReason::kLoads) return Submit(metrics::IoKind::kLoad);
      return {.kind = core::Outcome::Kind::kPending};
  }
  return {.kind = core::Outcome::Kind::kPending};
}

core::Outcome CommandTask::Submit(metrics::IoKind kind) {
  job_ = true;
  pool_.Submit(io_, kind, deadline_);
  return {.kind = core::Outcome::Kind::kPending};
}

core::Outcome CommandTask::Answered(core::Slot& slot) {
  const Mode mode = std::exchange(mode_, Mode::kIdle);
  if (mode == Mode::kFanOut) {
    core::Result<core::RespValue> reply = fan_.Reply();
    return Fenced(sequencer_.queue(), slot, std::move(reply), fan_.fences);
  }
  op_.reset();
  ReadAnswer answer = std::exchange(answer_, {});
  const std::span<const ShardSeq> fences = answer.fence.has_value()
                                               ? std::span<const ShardSeq>(&*answer.fence, 1)
                                               : std::span<const ShardSeq>();
  return Fenced(sequencer_.queue(), slot, std::move(answer.reply), fences);
}

void CommandTask::Settle(core::Result<void> wake, bool from_job) {
  if (mode_ == Mode::kWrite) {
    if (!from_job) write_.Woken(std::move(wake));
    // Its wait ends: a hand-off for an ok wake, N2's installs.
    (void)sequencer_.Attempt(write_);
  }
  answer_ = {};
  op_.reset();
  abandoned_ = false;
  mode_ = Mode::kIdle;
}

void CommandTask::Post() const noexcept {
  // Copied first: once posted, the task may be reused.
  const core::Completion completion = completion_;
  const core::CompletionEvent event{.client_id = completion.client_id,
                                    .slot_seq = completion.slot_seq,
                                    .kind = core::CompletionKind::kResume,
                                    .result = {}};
  completion.sink->Post(std::span(&event, 1));
}

// NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
void CommandTask::Job::Run() noexcept {
  CommandTask& task = task_;
  switch (task.mode_) {
    case Mode::kWrite:
      task.sequencer_.Load(task.write_);
      break;
    case Mode::kMiss:
      if (task.op_.has_value()) task.answer_ = task.reads_.Miss(*task.op_, task.deadline_);
      break;
    case Mode::kFanOut:
      task.reads_.FanOutMiss(task.fan_, task.deadline_);
      break;
    case Mode::kIdle:
      break;
  }
  task.Post();
}

// NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
void CommandTask::Job::Fail(core::Error error) noexcept {
  CommandTask& task = task_;
  Unrun(error);
  if (task.mode_ == Mode::kMiss && task.op_.has_value()) {
    task.answer_ = ReadAnswer{.reply = task.reads_.Failed(*task.op_, error)};
  } else if (task.mode_ == Mode::kFanOut) {
    task.fan_.error = std::move(error);
  }
  task.Post();
}

// NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
void CommandTask::Job::Unrun(const core::Error& error) noexcept {
  if (task_.mode_ == Mode::kWrite) task_.sequencer_.Unload(task_.write_, error);
}

}  // namespace abyss::engine
