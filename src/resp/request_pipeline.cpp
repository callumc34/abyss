#include "abyss/resp/request_pipeline.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/ascii.h"
#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"
#include "abyss/core/thread_annotations.h"
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

// The engine's events for this pipeline, in order. Warm, a post
// allocates nothing; the loop lets it go only once nothing of its
// command is outstanding, so no poster is still inside it.
class RequestPipeline::Inbox final : public core::CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    const std::scoped_lock lock(mu_);
    events_.insert(events_.end(), events.begin(), events.end());
    cv_.notify_one();
  }

  // The next event; nullopt once `deadline` passes with none.
  std::optional<core::CompletionEvent> Take(std::optional<core::SteadyTime> deadline) {
    std::unique_lock lock(mu_);
    const auto ready = [this] ABYSS_REQUIRES(mu_) { return next_ < events_.size(); };
    if (!deadline.has_value()) {
      cv_.wait(lock, ready);
    } else if (!cv_.wait_until(lock, *deadline, ready)) {
      return std::nullopt;
    }
    core::CompletionEvent event = std::move(events_[next_++]);
    if (next_ == events_.size()) {
      events_.clear();
      next_ = 0;
    }
    return event;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::vector<core::CompletionEvent> events_ ABYSS_GUARDED_BY(mu_);
  size_t next_ ABYSS_GUARDED_BY(mu_) = 0;
};

RequestPipeline::RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                                 Dependencies deps)
    : registry_(registry),
      deps_(deps),
      state_(std::move(state)),
      inbox_(std::make_unique<Inbox>()) {}

RequestPipeline::~RequestPipeline() {
  ABYSS_DCHECK(Quiescent(), "a pipeline closed with a command outstanding");
}

RequestPipeline::ProcessResult RequestPipeline::Process(std::span<const uint8_t> input,
                                                        std::vector<uint8_t>& output) {
  size_t consumed = 0;
  while (consumed < input.size()) {
    auto remaining = input.subspan(consumed);
    auto parsed = Parser::ParseCommand(remaining);
    if (!parsed.has_value()) {
      if (parsed.error().code() == core::ErrorCode::kIncomplete) {
        break;
      }
      ABYSS_LOG_DEBUG("parse error", {"client_id", state_.client_id},
                      {"err", std::string_view{parsed.error().message()}});
      if (deps_.metrics != nullptr) deps_.metrics->RecordParseError();
      auto response = RespValue::Error(ErrorPrefix::kErr,
                                       std::string("Protocol error: ") + parsed.error().message());
      auto bytes = Serializer::Serialize(response);
      output.insert(output.end(), bytes.begin(), bytes.end());
      // Unframable input: emit the error, consume the lot, and fail-closed so
      // the connection is torn down once the reply drains.
      consumed = input.size();
      close_reason_ = ProcessCloseReason::kProtocolError;
      break;
    }
    auto response = Dispatch(std::move(parsed->command));
    auto bytes = Serializer::Serialize(response);
    output.insert(output.end(), bytes.begin(), bytes.end());
    consumed += parsed->bytes_consumed;

    if (close_reason_ != ProcessCloseReason::kNone) {
      break;
    }
  }
  return {.bytes_consumed = consumed, .close_reason = close_reason_};
}

RespValue RequestPipeline::Dispatch(RespCommand cmd) {
  const auto start = std::chrono::steady_clock::now();
  auto outcome = DispatchImpl(std::move(cmd));

  if (deps_.metrics != nullptr) {
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const double seconds = std::chrono::duration<double>(elapsed).count();
    deps_.metrics->RecordDuration(outcome.cmd_name, seconds);
    deps_.metrics->RecordRequest(outcome.cmd_name, outcome.status);
  }
  return std::move(outcome.response);
}

RequestPipeline::DispatchOutcome RequestPipeline::DispatchImpl(RespCommand cmd) {
  const auto resolution = registry_.Resolve(cmd);

  switch (resolution.status) {
    case CommandRegistry::ResolveStatus::kUnknownCommand: {
      const std::string_view name = cmd.ArgCount() > 0 ? std::string_view(cmd.Name()) : "";
      const std::string_view first_arg = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
      return {MakeUnknownCommand(name, first_arg), RequestStatus::kUnknown, kUnknownCmdLabel};
    }

    case CommandRegistry::ResolveStatus::kArityMismatch:
      return {MakeArityError(resolution.resolved), RequestStatus::kArity,
              resolution.resolved.parent->name};

    case CommandRegistry::ResolveStatus::kUnknownSubcommand: {
      const std::string_view raw = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
      return {MakeUnknownSubcommand(*resolution.resolved.parent, raw), RequestStatus::kError,
              resolution.resolved.parent->name};
    }

    case CommandRegistry::ResolveStatus::kOk:
      break;
  }

  const auto& resolved = resolution.resolved;
  if (deps_.loading != nullptr && deps_.loading->IsLoading() && !resolved.LoadingSafe()) {
    return {MakeLoading(), RequestStatus::kLoading, resolved.parent->name};
  }

  return DispatchResolved(resolved, std::move(cmd));
}

RequestPipeline::DispatchOutcome RequestPipeline::DispatchResolved(const ResolvedCommand& resolved,
                                                                   RespCommand cmd) {
  const auto& parent = *resolved.parent;
  auto finish = [&parent](RespValue response) -> DispatchOutcome {
    const auto status = response.IsError() ? RequestStatus::kError : RequestStatus::kOk;
    return {std::move(response), status, parent.name};
  };
  auto engine = [this, &parent, &finish](core::Request request, std::string_view what) {
    auto result = RunToReply(std::move(request));
    if (!result.has_value()) {
      ABYSS_LOG_WARN(what, {"client_id", state_.client_id}, {"cmd", std::string_view{parent.name}},
                     {"err", std::string_view{result.error().message()}});
      return finish(
          RespValue::Error(MapErrorCode(result.error().code()), result.error().message()));
    }
    return finish(*std::move(result));
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

core::Result<RespValue> RequestPipeline::RunToReply(core::Request request) {
  slot_.completion = core::Completion{
      .sink = inbox_.get(), .client_id = state_.client_id, .slot_seq = ++slot_seq_};
  core::Outcome outcome = deps_.dispatcher->Dispatch(std::move(request), slot_, task_);
  std::optional<core::Result<RespValue>> reply;
  bool pending = false;
  switch (outcome.kind) {
    case core::Outcome::Kind::kReplied:
      reply = std::move(outcome.reply);
      break;
    case core::Outcome::Kind::kSequenced:
      break;
    case core::Outcome::Kind::kPending:
      pending = true;
      break;
  }
  // Once replied it waits only for what is already on its way, which
  // the deadline just passed bounds: a wake a Cancel lost, or a job.
  while (!reply.has_value() || !Quiescent()) {
    std::optional<core::CompletionEvent> event =
        inbox_->Take(reply.has_value() ? std::nullopt : std::optional(slot_.deadline));
    if (!event.has_value()) {
      ABYSS_DCHECK(pending || slot_.durable.waiting(), "a deadline for a command not waiting");
      reply = pending ? task_->Abandon(slot_) : slot_.durable.Expire();
      continue;
    }
    // Another command's event never runs this one's task.
    if (event->client_id != slot_.completion.client_id ||
        event->slot_seq != slot_.completion.slot_seq) {
      continue;
    }
    if (event->kind == core::CompletionKind::kDurable) {
      auto released = slot_.durable.Arrived(std::move(event->result));
      if (released.has_value() && !reply.has_value()) reply = *std::move(released);
      continue;
    }
    if (event->kind != core::CompletionKind::kResume) continue;
    ABYSS_DCHECK(task_ != nullptr, "a kResume for a command with no task");
    core::Outcome next = task_->Run(slot_, std::move(event->result));
    if (reply.has_value()) continue;
    pending = next.kind == core::Outcome::Kind::kPending;
    if (next.kind == core::Outcome::Kind::kReplied) reply = std::move(next.reply);
  }
  return *std::move(reply);
}

bool RequestPipeline::Quiescent() const {
  return (task_ == nullptr || task_->Quiescent()) && slot_.durable.Quiescent();
}

RequestPipeline::DispatchOutcome RequestPipeline::HandleAdminStateless(
    const ResolvedCommand& resolved, const RespCommand& cmd) {
  const auto& parent = *resolved.parent;
  const std::string_view sub_name =
      resolved.subcommand != nullptr ? resolved.subcommand->name : std::string_view{};

  auto finish = [&parent](RespValue response, RequestStatus status) -> DispatchOutcome {
    return {std::move(response), status, parent.name};
  };
  auto ok_or_err = [&finish](RespValue response) -> DispatchOutcome {
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
  close_reason_ = ProcessCloseReason::kClientQuit;
  return RespValue::SimpleString("OK");
}

RequestPipeline::DispatchOutcome RequestPipeline::HandleHello(const CommandSpec& spec,
                                                              const RespCommand& cmd) {
  auto record_response = [&spec](RespValue response) -> DispatchOutcome {
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
