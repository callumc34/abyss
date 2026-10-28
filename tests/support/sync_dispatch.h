#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "event_inbox.h"

namespace abyss::testing {

inline uint64_t NextClientId() {
  static std::atomic<uint64_t> next{1U << 20U};
  return next.fetch_add(1);
}

// One connection's commands through Dispatch, each run to its reply on
// the calling thread: depth one, as RequestPipeline runs them. Its
// task is kept from command to command. One thread at a time.
class SyncConnection {
 public:
  explicit SyncConnection(core::CommandDispatcher& dispatcher, uint64_t client_id = NextClientId())
      : dispatcher_(dispatcher), client_id_(client_id) {}

  SyncConnection(const SyncConnection&) = delete;
  SyncConnection& operator=(const SyncConnection&) = delete;
  SyncConnection(SyncConnection&&) = delete;
  SyncConnection& operator=(SyncConnection&&) = delete;
  ~SyncConnection() = default;

  core::Result<core::RespValue> Run(core::Request request) {
    slot_.completion =
        core::Completion{.sink = &inbox_, .client_id = client_id_, .slot_seq = ++seq_};
    core::Outcome outcome = dispatcher_.Dispatch(std::move(request), slot_, task_);
    std::optional<core::Result<core::RespValue>> reply;
    bool pending = outcome.kind == core::Outcome::Kind::kPending;
    if (outcome.kind == core::Outcome::Kind::kReplied) reply = std::move(outcome.reply);
    while (!reply.has_value() || !Quiescent()) {
      std::optional<core::CompletionEvent> event =
          inbox_.Take(reply.has_value() ? std::nullopt : std::optional(slot_.deadline));
      if (!event.has_value()) {
        reply = pending ? task_->Abandon(slot_) : slot_.durable.Expire();
        continue;
      }
      if (event->client_id != client_id_ || event->slot_seq != seq_) continue;
      if (event->kind == core::CompletionKind::kDurable) {
        auto released = slot_.durable.Arrived(std::move(event->result));
        if (released.has_value() && !reply.has_value()) reply = *std::move(released);
        continue;
      }
      if (event->kind != core::CompletionKind::kResume) continue;
      core::Outcome next = task_->Run(slot_, std::move(event->result));
      if (reply.has_value()) continue;
      pending = next.kind == core::Outcome::Kind::kPending;
      if (next.kind == core::Outcome::Kind::kReplied) reply = std::move(next.reply);
    }
    return *std::move(reply);
  }

  core::Result<core::RespValue> Read(std::string_view name, core::RespCommand cmd) {
    return Run(
        core::Request{.kind = core::Request::Kind::kRead, .name = name, .cmd = std::move(cmd)});
  }
  core::Result<core::RespValue> Write(core::RespCommand cmd,
                                      core::PredicateFlags flags = core::PredicateFlags::kNone) {
    return Run(
        core::Request{.kind = core::Request::Kind::kWrite, .flags = flags, .cmd = std::move(cmd)});
  }
  core::Result<core::RespValue> FanOut(core::MultiKeyKind kind, core::RespCommand cmd) {
    return Run(core::Request{
        .kind = core::Request::Kind::kFanOut, .multi_key = kind, .cmd = std::move(cmd)});
  }
  core::Result<core::RespValue> Flush() {
    return Run(core::Request{.kind = core::Request::Kind::kFlush});
  }

  bool Quiescent() const {
    return (task_ == nullptr || task_->Quiescent()) && slot_.durable.Quiescent();
  }
  const core::Task* task() const { return task_.get(); }
  core::Slot& slot() { return slot_; }
  EventInbox& inbox() { return inbox_; }
  uint64_t client_id() const { return client_id_; }
  uint64_t slot_seq() const { return seq_; }

 private:
  core::CommandDispatcher& dispatcher_;
  const uint64_t client_id_;
  uint64_t seq_ = 0;
  // Declared in the order they must outlive one another.
  EventInbox inbox_;
  core::Slot slot_;
  std::unique_ptr<core::Task> task_;
};

// The four calls the dispatcher had, each on a connection of its own,
// so any thread may make them.
class SyncDispatcher {
 public:
  explicit SyncDispatcher(core::CommandDispatcher& dispatcher) : dispatcher_(&dispatcher) {}

  core::Result<core::RespValue> DispatchRead(std::string_view name, const core::RespCommand& cmd) {
    SyncConnection connection(*dispatcher_);
    return connection.Read(name, cmd);
  }
  core::Result<core::RespValue> DispatchWrite(std::string_view /*name*/, core::RespCommand cmd,
                                              core::PredicateFlags flags) {
    SyncConnection connection(*dispatcher_);
    return connection.Write(std::move(cmd), flags);
  }
  core::Result<core::RespValue> DispatchFanOut(core::MultiKeyKind kind, core::RespCommand cmd) {
    SyncConnection connection(*dispatcher_);
    return connection.FanOut(kind, std::move(cmd));
  }
  core::Result<core::RespValue> DispatchFlush() {
    SyncConnection connection(*dispatcher_);
    return connection.Flush();
  }

 private:
  core::CommandDispatcher* dispatcher_;
};

}  // namespace abyss::testing
