#include "abyss/resp/request_pipeline.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/ascii.h"
#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/loading_state.h"
#include "abyss/resp/metrics.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/serializer.h"
#include "abyss/version.h"

ABYSS_LOG_COMPONENT("abyss.resp")

namespace abyss::resp {
namespace {

using core::ErrorPrefix;
using core::RespCommand;
using core::RespValue;
using metrics::RequestStatus;

// A ring grows towards the cap only as a deep pipeline needs it, and
// gives back what is past kKeptSlots once it empties.
constexpr size_t kFirstSlots = 4;
constexpr size_t kKeptSlots = 16;

ErrorPrefix MapErrorCode(core::ErrorCode code) {
  switch (code) {
    case core::ErrorCode::kWrongType:
      return ErrorPrefix::kWrongType;
    case core::ErrorCode::kResourceExhausted:
      return ErrorPrefix::kOom;
    default:
      return ErrorPrefix::kErr;
  }
}

}  // namespace

struct RequestPipeline::Entry {
  enum class State : uint8_t { kFree, kPending, kSequenced, kReplied, kDrained };

  State state = State::kFree;
  uint64_t seq = 0;
  core::Slot slot;
  RespValue reply;
  // The encoded size of its reply, or of the one its durable wait holds.
  size_t reply_bytes = 0;
  // For its metrics, and an engine error's log line.
  std::string_view name;
  std::string_view what;
  std::chrono::steady_clock::time_point started;
};

RequestPipeline::RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                                 Dependencies deps, PipelineHost host, PipelineLimits limits)
    : registry_(registry),
      deps_(deps),
      host_(host),
      limits_(limits),
      state_(std::move(state)),
      timer_(state_.client_id) {}

size_t RequestPipeline::EntryBytesForTesting() {
  return sizeof(Entry) + sizeof(std::unique_ptr<Entry>);
}

RequestPipeline::~RequestPipeline() {
  ABYSS_DCHECK(Quiescent(), "a pipeline closed with a command outstanding");
}

RequestPipeline::FeedResult RequestPipeline::Feed(std::span<const uint8_t> input,
                                                  size_t unwritten) {
  FeedResult result;
  while (result.consumed < input.size()) {
    if (!CanDispatch(unwritten)) {
      result.blocked = true;
      break;
    }
    auto parsed = Parser::ParseCommand(input.subspan(result.consumed));
    if (!parsed.has_value()) {
      if (parsed.error().code() == core::ErrorCode::kIncomplete) break;
      ABYSS_LOG_DEBUG("parse error", {"client_id", state_.client_id},
                      {"err", std::string_view{parsed.error().message()}});
      if (deps_.metrics != nullptr) deps_.metrics->RecordParseError();
      // Unframable input: its error goes out in order, the rest is
      // dropped, and the connection closes once the replies drain.
      Store(Acquire(), RespValue::Error(ErrorPrefix::kErr, std::string("Protocol error: ") +
                                                               parsed.error().message()));
      result.consumed = input.size();
      close_request_ = CloseRequest::kProtocolError;
      break;
    }
    result.consumed += parsed->bytes_consumed;
    DispatchCommand(std::move(parsed->command));
    if (close_request_ != CloseRequest::kNone) break;
  }
  SyncTimer();
  return result;
}

void RequestPipeline::Drain(std::vector<uint8_t>& output) {
  while (drain_ < tail_) {
    Entry& entry = At(drain_);
    if (entry.state != Entry::State::kReplied) break;
    Serializer::Append(entry.reply, output);
    Release(entry);
    ++drain_;
  }
  Reclaim();
}

bool RequestPipeline::OnEvent(const core::CompletionEvent& event) {
  if (event.slot_seq <= head_ || event.slot_seq > tail_) return false;
  Entry& entry = At(event.slot_seq - 1);
  switch (event.kind) {
    case core::CompletionKind::kResume: {
      if (task_ == nullptr || task_seq_ != event.slot_seq) return false;
      core::Outcome outcome = task_->Run(entry.slot, event.result);
      if (entry.state == Entry::State::kPending) {
        Settle(entry, std::move(outcome));
      } else if (!TaskBusy()) {
        // Its command already ended, at its deadline or with its
        // connection: this was cleanup.
        task_seq_ = 0;
      }
      break;
    }
    case core::CompletionKind::kDurable: {
      if (entry.slot.durable.Quiescent()) return false;
      auto released = entry.slot.durable.Arrived(event.result);
      if (released.has_value() && entry.state == Entry::State::kSequenced) {
        Reply(entry, *std::move(released));
      }
      break;
    }
    case core::CompletionKind::kSequenced:
    case core::CompletionKind::kReplied:
      return false;
  }
  Reclaim();
  SyncTimer();
  return true;
}

std::unique_ptr<OrphanTask> RequestPipeline::Expire(core::SteadyTime now) {
  std::unique_ptr<OrphanTask> orphan;
  for (uint64_t pos = drain_; pos < tail_; ++pos) {
    Entry& entry = At(pos);
    if (entry.state != Entry::State::kPending && entry.state != Entry::State::kSequenced) continue;
    // Every command gets the same timeout, so later ones are later.
    if (entry.slot.deadline > now) break;
    if (entry.state == Entry::State::kSequenced) {
      // The write stays applied.
      Reply(entry, entry.slot.durable.Expire());
      continue;
    }
    // Never applied from here: what is still out of it lands as cleanup.
    core::Result<RespValue> reply = task_->Abandon(entry.slot);
    if (!TaskBusy()) {
      task_seq_ = 0;
    } else if (!closed_) {
      orphan = std::make_unique<OrphanTask>();
      orphan->slot.completion = entry.slot.completion;
      orphan->slot.deadline = entry.slot.deadline;
      orphan->task = std::move(task_);
      task_seq_ = 0;
    }
    Reply(entry, std::move(reply));
  }
  Reclaim();
  SyncTimer();
  return orphan;
}

std::optional<core::SteadyTime> RequestPipeline::NextDeadline() const {
  for (uint64_t pos = drain_; pos < tail_; ++pos) {
    const Entry& entry = At(pos);
    if (entry.state == Entry::State::kPending || entry.state == Entry::State::kSequenced) {
      return entry.slot.deadline;
    }
  }
  return std::nullopt;
}

void RequestPipeline::StopDispatch() { stopped_ = true; }

void RequestPipeline::Close() {
  if (closed_) return;
  closed_ = true;
  for (uint64_t pos = drain_; pos < tail_; ++pos) {
    Entry& entry = At(pos);
    // It runs on to its outcome or its deadline, which nothing hears.
    if (entry.state == Entry::State::kPending) continue;
    if (entry.state == Entry::State::kSequenced) entry.slot.durable.Abandon();
    Release(entry);
  }
  while (drain_ < tail_ && At(drain_).state == Entry::State::kDrained) ++drain_;
  Reclaim();
  SyncTimer();
}

bool RequestPipeline::Quiescent() const {
  if (TaskBusy()) return false;
  for (uint64_t pos = head_; pos < tail_; ++pos) {
    if (!At(pos).slot.durable.Quiescent()) return false;
  }
  return true;
}

bool RequestPipeline::CanDispatch(size_t unwritten) const {
  return !closed_ && !stopped_ && close_request_ == CloseRequest::kNone && task_seq_ == 0 &&
         Slots() < limits_.max_inflight && unwritten + reply_bytes_ < limits_.reply_bytes;
}

RequestPipeline::Entry& RequestPipeline::Acquire() {
  if (Slots() == ring_.size()) Grow();
  Entry& entry = At(tail_);
  const bool free = entry.state == Entry::State::kFree && entry.slot.durable.Quiescent();
  ABYSS_DCHECK(free, "a slot reused while owed a post");
  entry.seq = ++tail_;
  entry.started = std::chrono::steady_clock::now();
  return entry;
}

void RequestPipeline::Grow() {
  const size_t size = ring_.size();
  const size_t grown = std::min(std::max(size * 2, kFirstSlots), limits_.max_inflight);
  ABYSS_DCHECK(grown > size, "a ring grown past its cap");
  // Entries keep their addresses: a slot's nodes are linked by them.
  std::vector<std::unique_ptr<Entry>> ring(grown);
  for (uint64_t pos = head_; pos < tail_; ++pos) ring[pos % grown] = std::move(ring_[pos % size]);
  for (auto& entry : ring) {
    if (entry == nullptr) entry = std::make_unique<Entry>();
  }
  ring_ = std::move(ring);
}

void RequestPipeline::Reclaim() {
  while (head_ < drain_) {
    Entry& entry = At(head_);
    // A slot is reused only once nothing will post to it, and a busy
    // task's slot is the one it runs on.
    if (!entry.slot.durable.Quiescent() || entry.seq == task_seq_) break;
    entry.state = Entry::State::kFree;
    ++head_;
  }
  if (head_ == tail_ && ring_.size() > kKeptSlots) ring_.resize(kKeptSlots);
}

void RequestPipeline::SyncTimer() {
  if (host_.timers == nullptr) return;
  if (auto deadline = NextDeadline()) {
    host_.timers->Schedule(timer_, *deadline);
  } else {
    host_.timers->Cancel(timer_);
  }
}

void RequestPipeline::DispatchCommand(RespCommand cmd) {
  Entry& entry = Acquire();
  const auto resolution = registry_.Resolve(cmd);
  std::optional<Immediate> now;
  switch (resolution.status) {
    case CommandRegistry::ResolveStatus::kUnknownCommand: {
      const std::string_view name = cmd.ArgCount() > 0 ? std::string_view(cmd.Name()) : "";
      const std::string_view first_arg = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
      now =
          Immediate{MakeUnknownCommand(name, first_arg), RequestStatus::kUnknown, kUnknownCmdLabel};
      break;
    }
    case CommandRegistry::ResolveStatus::kArityMismatch:
      now = Immediate{MakeArityError(resolution.resolved), RequestStatus::kArity,
                      resolution.resolved.parent->name};
      break;
    case CommandRegistry::ResolveStatus::kUnknownSubcommand: {
      const std::string_view raw = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
      now = Immediate{MakeUnknownSubcommand(*resolution.resolved.parent, raw),
                      RequestStatus::kError, resolution.resolved.parent->name};
      break;
    }
    case CommandRegistry::ResolveStatus::kOk: {
      const auto& resolved = resolution.resolved;
      if (deps_.loading != nullptr && deps_.loading->IsLoading() && !resolved.LoadingSafe()) {
        now = Immediate{MakeLoading(), RequestStatus::kLoading, resolved.parent->name};
      } else {
        now = DispatchResolved(entry, resolved, std::move(cmd));
      }
      break;
    }
  }
  if (now.has_value()) Finish(entry, std::move(now->response), now->status, now->cmd_name);
}

std::optional<RequestPipeline::Immediate> RequestPipeline::DispatchResolved(
    Entry& entry, const ResolvedCommand& resolved, RespCommand cmd) {
  const auto& parent = *resolved.parent;
  auto finish = [&parent](RespValue response) -> Immediate {
    const auto status = response.IsError() ? RequestStatus::kError : RequestStatus::kOk;
    return {std::move(response), status, parent.name};
  };
  auto engine = [this, &entry](core::Request request,
                               std::string_view what) -> std::optional<Immediate> {
    Submit(entry, std::move(request), what);
    return std::nullopt;
  };

  // Fan-out commands route as their own kind instead of read/write. The
  // kind is only set on parent specs, never on subcommands.
  if (resolved.subcommand == nullptr && parent.multi_key_kind != core::MultiKeyKind::kNone) {
    if (deps_.dispatcher == nullptr) {
      return finish(InternalServerError("dispatcher missing for fan-out", parent.name));
    }
    return engine(core::Request{.kind = core::Request::Kind::kFanOut,
                                .name = parent.name,
                                .multi_key = parent.multi_key_kind,
                                .cmd = std::move(cmd)},
                  "engine fan-out error");
  }

  switch (resolved.DispatchClass()) {
    case Dispatch::kStateless:
      return HandleAdminStateless(resolved, cmd);

    case Dispatch::kTieredRead:
      if (deps_.dispatcher == nullptr) {
        return finish(InternalServerError("dispatcher missing for tiered read", parent.name));
      }
      return engine(
          core::Request{
              .kind = core::Request::Kind::kRead, .name = parent.name, .cmd = std::move(cmd)},
          "engine read error");

    case Dispatch::kWritePath: {
      if (deps_.dispatcher == nullptr) {
        return finish(InternalServerError("dispatcher missing for write path", parent.name));
      }
      // Registry class is a hint; the extractor decides per args (e.g. SET vs SET NX).
      core::PredicateFlags flags = core::PredicateFlags::kNone;
      if (parent.predicate != nullptr) {
        auto extracted = parent.predicate(cmd);
        if (!extracted.has_value()) {
          return finish(
              RespValue::Error(ErrorPrefix::kErr, std::string{extracted.error().message()}));
        }
        flags = *extracted;
      }
      // Arity alone is a weaker check than the parser: `HSET k f v f` and
      // `SET k v BOGUS` both satisfy the registry and are still malformed,
      // so the parser replies before dispatch. Commands with no parser are
      // exempt: that is a capability gap, not malformed input, and the
      // registry stays the sole authority for them.
      if (core::ops::HasWriteParser(parent.name)) {
        auto parsed = core::ops::ParseWriteOp(parent.name, cmd);
        if (!parsed.has_value()) {
          return finish(RespValue::Error(MapErrorCode(parsed.error().code()),
                                         std::string{parsed.error().message()}));
        }
      }

      // The client's spelling: the engine logs the canonical effect it
      // decides, its TTLs made absolute at the instant it decides at.
      return engine(core::Request{.kind = core::Request::Kind::kWrite,
                                  .name = parent.name,
                                  .flags = flags,
                                  .cmd = std::move(cmd)},
                    "engine write error");
    }

    case Dispatch::kAdmin:
      if (parent.name == "DBSIZE") {
        if (deps_.stats == nullptr) {
          return finish(InternalServerError("stats missing for DBSIZE", parent.name));
        }
        return finish(HandleDbsize(*deps_.stats));
      }
      {
        std::string label{parent.name};
        if (resolved.subcommand != nullptr) {
          label.push_back('|');
          label.append(resolved.subcommand->name);
        }
        std::string msg = "not supported in this build (";
        msg.append(label);
        msg.append("); tracked in abyss#97");
        return finish(RespValue::Error(ErrorPrefix::kErr, std::move(msg)));
      }

    case Dispatch::kFlush: {
      if (deps_.dispatcher == nullptr) {
        return finish(InternalServerError("dispatcher missing for flush", parent.name));
      }
      // Optional [ASYNC|SYNC] modifier — accepted but ignored.
      if (cmd.args.size() > 2) {
        return finish(RespValue::Error(
            ErrorPrefix::kErr,
            "wrong number of arguments for '" + std::string{parent.name} + "' command"));
      }
      if (cmd.args.size() == 2) {
        if (!core::AsciiEqualsIgnoreCase(cmd.args[1], "ASYNC") &&
            !core::AsciiEqualsIgnoreCase(cmd.args[1], "SYNC")) {
          return finish(RespValue::Error(ErrorPrefix::kErr, "syntax error"));
        }
      }
      // One database: FLUSHALL and FLUSHDB both wipe it.
      return engine(core::Request{.kind = core::Request::Kind::kFlush, .name = parent.name},
                    "flush dispatch error");
    }
  }
  return finish(InternalServerError("unhandled dispatch class", parent.name));
}

void RequestPipeline::Submit(Entry& entry, core::Request request, std::string_view what) {
  entry.name = request.name;
  entry.what = what;
  entry.slot.completion =
      core::Completion{.sink = host_.sink, .client_id = state_.client_id, .slot_seq = entry.seq};
  core::Outcome outcome = deps_.dispatcher->Dispatch(std::move(request), entry.slot, task_);
  Settle(entry, std::move(outcome));
}

void RequestPipeline::Settle(Entry& entry, core::Outcome outcome) {
  switch (outcome.kind) {
    case core::Outcome::Kind::kReplied:
      Reply(entry, std::move(outcome.reply));
      break;
    case core::Outcome::Kind::kSequenced:
      if (closed_) {
        // Applied; nothing waits to hear when it is durable.
        entry.slot.durable.Abandon();
        Drop(entry);
        break;
      }
      entry.state = Entry::State::kSequenced;
      {
        const auto& held = entry.slot.durable.held();
        entry.reply_bytes = held.has_value() ? Serializer::Size(*held) : 0;
      }
      reply_bytes_ += entry.reply_bytes;
      break;
    case core::Outcome::Kind::kPending:
      entry.state = Entry::State::kPending;
      break;
  }
  if (entry.state == Entry::State::kPending || TaskBusy()) {
    task_seq_ = entry.seq;
  } else if (task_seq_ == entry.seq) {
    task_seq_ = 0;
  }
}

void RequestPipeline::Reply(Entry& entry, core::Result<RespValue> result) {
  if (closed_) {
    Drop(entry);
    return;
  }
  if (!result.has_value()) {
    ABYSS_LOG_WARN(entry.what, {"client_id", state_.client_id}, {"cmd", entry.name},
                   {"err", std::string_view{result.error().message()}});
    Finish(entry, RespValue::Error(MapErrorCode(result.error().code()), result.error().message()),
           RequestStatus::kError, entry.name);
    return;
  }
  RespValue response = *std::move(result);
  const auto status = response.IsError() ? RequestStatus::kError : RequestStatus::kOk;
  Finish(entry, std::move(response), status, entry.name);
}

void RequestPipeline::Finish(Entry& entry, RespValue response, RequestStatus status,
                             std::string_view cmd_name) {
  if (deps_.metrics != nullptr) {
    const auto elapsed = std::chrono::steady_clock::now() - entry.started;
    deps_.metrics->RecordDuration(cmd_name, std::chrono::duration<double>(elapsed).count());
    deps_.metrics->RecordRequest(cmd_name, status);
  }
  Store(entry, std::move(response));
}

void RequestPipeline::Store(Entry& entry, RespValue response) {
  reply_bytes_ -= entry.reply_bytes;
  entry.reply = std::move(response);
  entry.reply_bytes = Serializer::Size(entry.reply);
  reply_bytes_ += entry.reply_bytes;
  entry.state = Entry::State::kReplied;
}

void RequestPipeline::Release(Entry& entry) {
  reply_bytes_ -= entry.reply_bytes;
  entry.reply_bytes = 0;
  entry.reply = RespValue{};
  entry.state = Entry::State::kDrained;
}

void RequestPipeline::Drop(Entry& entry) {
  Release(entry);
  while (drain_ < tail_ && At(drain_).state == Entry::State::kDrained) ++drain_;
}

RequestPipeline::Immediate RequestPipeline::HandleAdminStateless(const ResolvedCommand& resolved,
                                                                 const RespCommand& cmd) {
  const auto& parent = *resolved.parent;
  const std::string_view sub_name =
      resolved.subcommand != nullptr ? resolved.subcommand->name : std::string_view{};

  auto finish = [&parent](RespValue response, RequestStatus status) -> Immediate {
    return {std::move(response), status, parent.name};
  };
  auto ok_or_err = [&finish](RespValue response) -> Immediate {
    const auto status = response.IsError() ? RequestStatus::kError : RequestStatus::kOk;
    return finish(std::move(response), status);
  };

  if (parent.name == "PING") return ok_or_err(HandlePing(cmd));
  if (parent.name == "ECHO") return ok_or_err(HandleEcho(cmd));
  if (parent.name == "QUIT") return ok_or_err(HandleQuit());
  if (parent.name == "HELLO") return HandleHello(parent, cmd);
  if (parent.name == "TIME") return ok_or_err(HandleTime());
  if (parent.name == "RESET") return ok_or_err(HandleReset());

  if (parent.name == "CLIENT") {
    return ok_or_err(HandleClient(sub_name, cmd));
  }
  if (parent.name == "COMMAND") {
    return ok_or_err(HandleCommandIntrospect(sub_name, cmd, registry_));
  }

  if (parent.name == "INFO") {
    if (deps_.stats == nullptr) {
      return finish(InternalServerError("stats missing for INFO", parent.name),
                    RequestStatus::kError);
    }
    return ok_or_err(HandleInfo(cmd, *deps_.stats));
  }

  if (parent.name == "CLUSTER") {
    if (deps_.stats == nullptr || deps_.identity == nullptr) {
      return finish(InternalServerError("stats/identity missing for CLUSTER", parent.name),
                    RequestStatus::kError);
    }
    return ok_or_err(HandleCluster(sub_name, cmd, *deps_.stats, *deps_.identity));
  }

  if (parent.name == "CONFIG") {
    if (deps_.config == nullptr) {
      return finish(InternalServerError("config missing for CONFIG", parent.name),
                    RequestStatus::kError);
    }
    return ok_or_err(HandleConfig(sub_name, cmd, *deps_.config));
  }

  return finish(InternalServerError("unhandled stateless command", parent.name),
                RequestStatus::kError);
}

RespValue RequestPipeline::HandlePing(const RespCommand& cmd) {
  if (cmd.ArgCount() == 1) return RespValue::SimpleString("PONG");
  // Registry arity stays -1; the one-message upper bound is a PING-only rule.
  if (cmd.ArgCount() > 2) {
    return RespValue::Error(ErrorPrefix::kErr, "wrong number of arguments for 'ping' command");
  }
  return RespValue::BulkString(cmd.args[1]);
}

RespValue RequestPipeline::HandleEcho(const RespCommand& cmd) {
  return RespValue::BulkString(cmd.args[1]);
}

RespValue RequestPipeline::HandleQuit() {
  close_request_ = CloseRequest::kClientQuit;
  return RespValue::SimpleString("OK");
}

RequestPipeline::Immediate RequestPipeline::HandleHello(const CommandSpec& spec,
                                                        const RespCommand& cmd) {
  auto record_response = [&spec](RespValue response) -> Immediate {
    if (response.IsError()) {
      const auto status = response.ErrorPrefixOf() == ErrorPrefix::kNoProto
                              ? RequestStatus::kNoProto
                              : RequestStatus::kError;
      return {std::move(response), status, spec.name};
    }
    return {std::move(response), RequestStatus::kOk, spec.name};
  };

  uint8_t negotiated = state_.protocol_version;
  if (cmd.ArgCount() >= 2) {
    const auto& proto_arg = cmd.args[1];
    if (proto_arg != "2") {
      // Record the asked-for version on rejection so v3 attempts are visible.
      if (proto_arg == "3" && deps_.metrics != nullptr) {
        deps_.metrics->RecordProtocol(3);
      }
      return record_response(
          RespValue::Error(ErrorPrefix::kNoProto, "unsupported protocol version"));
    }
    negotiated = 2;

    // AUTH args are accepted and discarded until AUTH/ACL is implemented.
    for (size_t i = 2; i < cmd.ArgCount(); ++i) {
      if (core::AsciiEqualsIgnoreCase(cmd.args[i], "AUTH")) {
        if (i + 2 >= cmd.ArgCount()) {
          return record_response(RespValue::Error(ErrorPrefix::kErr, "syntax error in HELLO AUTH"));
        }
        i += 2;
      } else if (core::AsciiEqualsIgnoreCase(cmd.args[i], "SETNAME")) {
        if (i + 1 >= cmd.ArgCount()) {
          return record_response(
              RespValue::Error(ErrorPrefix::kErr, "syntax error in HELLO SETNAME"));
        }
        state_.client_name = cmd.args[i + 1];
        ++i;
      } else {
        return record_response(RespValue::Error(ErrorPrefix::kErr, "syntax error"));
      }
    }
  }

  state_.protocol_version = negotiated;
  if (deps_.metrics != nullptr) deps_.metrics->RecordProtocol(negotiated);
  ABYSS_LOG_DEBUG("hello handshake", {"client_id", state_.client_id},
                  {"proto", static_cast<int64_t>(negotiated)});

  return record_response(RespValue::Array({
      RespValue::BulkString("server"),
      RespValue::BulkString("abyss"),
      RespValue::BulkString("version"),
      RespValue::BulkString(kVersion),
      RespValue::BulkString("proto"),
      RespValue::Integer(negotiated),
      RespValue::BulkString("id"),
      RespValue::Integer(static_cast<int64_t>(state_.client_id)),
      RespValue::BulkString("mode"),
      RespValue::BulkString("standalone"),
      RespValue::BulkString("role"),
      RespValue::BulkString("master"),
      RespValue::BulkString("modules"),
      RespValue::Array({}),
  }));
}

RespValue RequestPipeline::HandleTime() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now).count();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now).count() % 1'000'000;
  return RespValue::Array({
      RespValue::BulkString(std::to_string(seconds)),
      RespValue::BulkString(std::to_string(micros)),
  });
}

RespValue RequestPipeline::HandleClient(std::string_view subcommand, const RespCommand& cmd) {
  if (subcommand == "ID") return RespValue::Integer(static_cast<int64_t>(state_.client_id));
  if (subcommand == "GETNAME") {
    if (state_.client_name.empty()) return RespValue::Null();
    return RespValue::BulkString(state_.client_name);
  }
  if (subcommand == "SETNAME") {
    state_.client_name = cmd.args[2];
    return RespValue::SimpleString("OK");
  }
  if (subcommand == "NO-EVICT") {
    // Accepted and acked; per-client eviction opt-out is not honoured.
    return RespValue::SimpleString("OK");
  }
  return RespValue::Error(
      ErrorPrefix::kErr,
      std::string("internal: unhandled CLIENT subcommand '").append(subcommand).append("'"));
}

RespValue RequestPipeline::HandleReset() {
  state_.client_name.clear();
  state_.protocol_version = 2;
  return RespValue::SimpleString("RESET");
}

RespValue RequestPipeline::InternalServerError(std::string_view context,
                                               std::string_view cmd_name) {
  ABYSS_LOG_ERROR("request pipeline internal error", {"client_id", state_.client_id},
                  {"cmd", std::string_view{cmd_name}}, {"context", context});
  return RespValue::Error(ErrorPrefix::kErr, "internal server error");
}

RespValue RequestPipeline::MakeLoading() {
  return RespValue::Error(ErrorPrefix::kLoading, "Abyss is loading the dataset in memory");
}

RespValue RequestPipeline::MakeUnknownCommand(std::string_view name, std::string_view first_arg) {
  std::string message = "unknown command '";
  message.append(name);
  message.append("'");
  if (!first_arg.empty()) {
    message.append(", with args beginning with: '");
    message.append(first_arg);
    message.append("'");
  } else {
    message.append(", with args beginning with: ");
  }
  return RespValue::Error(ErrorPrefix::kErr, std::move(message));
}

RespValue RequestPipeline::MakeArityError(const ResolvedCommand& resolved) {
  std::string label = core::AsciiLower(resolved.parent->name);
  if (resolved.subcommand != nullptr) {
    label.push_back('|');
    label.append(core::AsciiLower(resolved.subcommand->name));
  }
  std::string message = "wrong number of arguments for '";
  message.append(label);
  message.append("' command");
  return RespValue::Error(ErrorPrefix::kErr, std::move(message));
}

RespValue RequestPipeline::MakeUnknownSubcommand(const CommandSpec& parent,
                                                 std::string_view raw_sub) {
  std::string message = "Unknown ";
  message.append(parent.name);
  message.append(" subcommand or wrong number of arguments for '");
  message.append(raw_sub);
  message.push_back('\'');
  return RespValue::Error(ErrorPrefix::kErr, std::move(message));
}

}  // namespace abyss::resp
