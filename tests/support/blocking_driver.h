#pragma once

#include <optional>
#include <utility>

#include "abyss/core/completion.h"
#include "abyss/core/durable_wait.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/pending_write.h"
#include "abyss/engine/sequencer.h"
#include "event_inbox.h"

namespace abyss::testing {

// A durable wait's posts, to its reply or its deadline, then on until
// nothing of it is on its way.
inline core::Result<core::RespValue> AwaitDurable(core::DurableWait& wait, EventInbox& inbox,
                                                  core::SteadyTime deadline) {
  std::optional<core::Result<core::RespValue>> reply;
  while (!reply.has_value() || !wait.Quiescent()) {
    std::optional<core::CompletionEvent> event =
        inbox.Take(reply.has_value() ? std::nullopt : std::optional(deadline));
    if (!event.has_value()) {
      reply = wait.Expire();
      continue;
    }
    auto released = wait.Arrived(std::move(event->result));
    if (released.has_value() && !reply.has_value()) reply = *std::move(released);
  }
  return *std::move(reply);
}

// A write's attempts run to its reply on the calling thread, which
// blocks on `inbox` between them. Its own loads run here too. At the
// deadline it abandons the write, then waits for a wake already on its
// way, as the reactor will.
inline core::Result<core::RespValue> DriveWrite(engine::Sequencer& sequencer,
                                                engine::PendingWrite& write, EventInbox& inbox) {
  for (;;) {
    engine::Step step = sequencer.Attempt(write);
    if (step.kind == engine::Step::Kind::kReplied) return std::move(step.reply);
    if (step.kind == engine::Step::Kind::kSequenced) {
      core::DurableWait wait;
      if (auto now = sequencer.Sequence(write, wait, write.completion())) return *std::move(now);
      return AwaitDurable(wait, inbox, write.deadline());
    }
    if (step.reason == engine::WaitReason::kLoads) {
      sequencer.Load(write);
      continue;
    }
    std::optional<core::CompletionEvent> event = inbox.Take(write.deadline());
    if (!event.has_value()) {
      core::Result<core::RespValue> reply = sequencer.Abandon(write);
      if (!write.Quiescent()) {
        write.Woken(inbox.Take(std::nullopt)->result);
        (void)sequencer.Attempt(write);
      }
      return reply;
    }
    write.Woken(std::move(event->result));
  }
}

inline core::Result<core::RespValue> RunToCompletion(engine::Sequencer& sequencer,
                                                     core::RespCommand cmd,
                                                     core::PredicateFlags flags) {
  EventInbox inbox;
  engine::PendingWrite write(std::move(cmd), flags, core::Completion{.sink = &inbox},
                             core::SteadyClock::now() + sequencer.config().command_timeout);
  return DriveWrite(sequencer, write, inbox);
}

// FLUSHDB, the same way.
inline core::Result<core::RespValue> RunFlushToCompletion(engine::Sequencer& sequencer) {
  EventInbox inbox;
  engine::PendingWrite write(engine::PendingWrite::FlushAll{}, core::Completion{.sink = &inbox},
                             core::SteadyClock::now() + sequencer.config().command_timeout);
  return DriveWrite(sequencer, write, inbox);
}

}  // namespace abyss::testing
