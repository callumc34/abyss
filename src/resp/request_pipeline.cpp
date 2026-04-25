#include "abyss/resp/request_pipeline.h"

#include <cctype>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/metrics/names.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/loading_state.h"
#include "abyss/resp/metrics.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/serializer.h"
#include "abyss/version.h"

namespace abyss::resp {
namespace {

using core::ErrorPrefix;
using core::RespCommand;
using core::RespValue;
using metrics::RequestStatus;

const log::Logger& Logger() {
  static const log::Logger l = log::Get("abyss.resp");
  return l;
}

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return out;
}

std::string Lowercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return out;
}

bool IEquals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(a[i])) !=
        std::toupper(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

RespValue MakeUnknownCommandError(std::string_view name, std::string_view first_arg) {
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

RespValue MakeArityError(std::string_view name) {
  std::string message = "wrong number of arguments for '";
  message.append(Lowercase(name));
  message.append("' command");
  return RespValue::Error(ErrorPrefix::kErr, std::move(message));
}

}  // namespace

RequestPipeline::RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                                 Dependencies deps)
    : registry_(registry), deps_(deps), state_(std::move(state)) {}

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
      ABYSS_LOG_DEBUG(Logger(), "parse error", {"client_id", state_.client_id},
                      {"err", std::string_view{parsed.error().message()}});
      if (deps_.metrics != nullptr) deps_.metrics->RecordParseError();
      auto response = RespValue::Error(ErrorPrefix::kErr,
                                       std::string("Protocol error: ") + parsed.error().message());
      auto bytes = Serializer::Serialize(response);
      output.insert(output.end(), bytes.begin(), bytes.end());
      // Can't resync on malformed input; consume the lot and let the caller close.
      consumed = input.size();
      break;
    }
    auto response = Dispatch(parsed->command);
    auto bytes = Serializer::Serialize(response);
    output.insert(output.end(), bytes.begin(), bytes.end());
    consumed += parsed->bytes_consumed;

    if (close_requested_) {
      break;
    }
  }
  return {.bytes_consumed = consumed, .close_requested = close_requested_};
}

RespValue RequestPipeline::Dispatch(const RespCommand& cmd) {
  const auto start = std::chrono::steady_clock::now();
  auto outcome = DispatchImpl(cmd);

  if (deps_.metrics != nullptr) {
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const double seconds = std::chrono::duration<double>(elapsed).count();
    deps_.metrics->RecordDuration(outcome.cmd_name, seconds);
    deps_.metrics->RecordRequest(outcome.cmd_name, outcome.status);
  }
  return std::move(outcome.response);
}

RequestPipeline::DispatchOutcome RequestPipeline::DispatchImpl(const RespCommand& cmd) {
  if (cmd.ArgCount() == 0) {
    return {MakeUnknownCommandError("", ""), RequestStatus::kUnknown, kUnknownCmdLabel};
  }

  const auto* spec = registry_.Find(cmd.Name());
  if (spec == nullptr) {
    const std::string_view first_arg = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
    return {MakeUnknownCommandError(cmd.Name(), first_arg), RequestStatus::kUnknown,
            kUnknownCmdLabel};
  }

  // Arity is validated after the name lookup so the metric label can be bound
  // to the canonical name rather than the "unknown" sentinel.
  auto classified = registry_.Classify(cmd);
  if (!classified.has_value()) {
    return {MakeArityError(spec->name), RequestStatus::kArity, spec->name};
  }

  if (deps_.loading != nullptr && deps_.loading->IsLoading() && !spec->loading_safe) {
    return {MakeLoading(), RequestStatus::kLoading, spec->name};
  }

  return DispatchKnown(*spec, cmd);
}

RequestPipeline::DispatchOutcome RequestPipeline::DispatchKnown(const CommandSpec& spec,
                                                                const RespCommand& cmd) {
  auto finish = [&spec](RespValue response) -> DispatchOutcome {
    const auto status = response.IsError() ? RequestStatus::kError : RequestStatus::kOk;
    return {std::move(response), status, spec.name};
  };

  switch (spec.dispatch) {
    case Dispatch::kStateless:
      return HandleAdminStateless(spec, cmd);

    case Dispatch::kTieredRead:
      if (deps_.dispatcher == nullptr) {
        return finish(MakeUnsupported("tiered read", spec.name));
      }
      {
        auto result = deps_.dispatcher->DispatchRead(spec.name, cmd);
        if (!result.has_value()) {
          ABYSS_LOG_WARN(Logger(), "engine read error", {"client_id", state_.client_id},
                         {"cmd", std::string_view{spec.name}},
                         {"err", std::string_view{result.error().message()}});
          return finish(RespValue::Error(ErrorPrefix::kErr, result.error().message()));
        }
        return finish(std::move(*result));
      }

    case Dispatch::kWritePath:
      if (deps_.dispatcher == nullptr) {
        return finish(MakeUnsupported("write path", spec.name));
      }
      {
        auto result = deps_.dispatcher->DispatchWrite(spec.name, RespCommand(cmd));
        if (!result.has_value()) {
          ABYSS_LOG_WARN(Logger(), "engine write error", {"client_id", state_.client_id},
                         {"cmd", std::string_view{spec.name}},
                         {"err", std::string_view{result.error().message()}});
          return finish(RespValue::Error(ErrorPrefix::kErr, result.error().message()));
        }
        return finish(std::move(*result));
      }

    case Dispatch::kConditionalWrite: {
      std::string msg = "conditional writes are not supported in this build (";
      msg.append(spec.name);
      msg.append("); tracked in abyss#97");
      return finish(RespValue::Error(ErrorPrefix::kErr, std::move(msg)));
    }

    case Dispatch::kConsumerRpc:
      if (spec.name == "DBSIZE") {
        if (deps_.stats == nullptr) {
          return finish(
              RespValue::Error(ErrorPrefix::kErr, "server stats provider not configured"));
        }
        return finish(HandleDbsize(*deps_.stats));
      }
      {
        std::string msg = "consumer RPC commands are not supported in this build (";
        msg.append(spec.name);
        msg.append("); tracked in abyss#97");
        return finish(RespValue::Error(ErrorPrefix::kErr, std::move(msg)));
      }
  }
  return finish(MakeUnsupported("dispatch", spec.name));
}

RequestPipeline::DispatchOutcome RequestPipeline::HandleAdminStateless(const CommandSpec& spec,
                                                                       const RespCommand& cmd) {
  auto finish = [&spec](RespValue response, RequestStatus status) -> DispatchOutcome {
    return {std::move(response), status, spec.name};
  };
  auto ok_or_err = [&finish](RespValue response) -> DispatchOutcome {
    const auto status = response.IsError() ? RequestStatus::kError : RequestStatus::kOk;
    return finish(std::move(response), status);
  };

  if (spec.name == "PING") return ok_or_err(HandlePing(cmd));
  if (spec.name == "ECHO") return ok_or_err(HandleEcho(cmd));
  if (spec.name == "QUIT") return ok_or_err(HandleQuit(cmd));
  if (spec.name == "HELLO") {
    auto response = HandleHello(cmd);
    if (!response.IsError()) {
      return finish(std::move(response), RequestStatus::kOk);
    }
    const auto is_noproto = response.AsString().starts_with("unsupported protocol");
    return finish(std::move(response),
                  is_noproto ? RequestStatus::kNoProto : RequestStatus::kError);
  }
  if (spec.name == "TIME") return ok_or_err(HandleTime(cmd));
  if (spec.name == "COMMAND") return ok_or_err(HandleCommandIntrospect(cmd, registry_));
  if (spec.name == "CLIENT") return ok_or_err(HandleClient(cmd));
  if (spec.name == "RESET") return ok_or_err(HandleReset(cmd));

  if (spec.name == "INFO") {
    if (deps_.stats == nullptr) {
      return ok_or_err(RespValue::Error(ErrorPrefix::kErr, "server stats provider not configured"));
    }
    return ok_or_err(HandleInfo(cmd, *deps_.stats));
  }

  if (spec.name == "CLUSTER") {
    if (deps_.stats == nullptr || deps_.identity == nullptr) {
      return ok_or_err(RespValue::Error(
          ErrorPrefix::kErr, "cluster introspection not configured (stats/identity missing)"));
    }
    const bool loading = deps_.loading != nullptr && deps_.loading->IsLoading();
    return ok_or_err(HandleCluster(cmd, *deps_.stats, *deps_.identity, loading));
  }

  if (spec.name == "CONFIG") {
    if (deps_.config == nullptr) {
      return ok_or_err(RespValue::Error(ErrorPrefix::kErr, "config provider not configured"));
    }
    return ok_or_err(HandleConfig(cmd, *deps_.config));
  }

  return ok_or_err(MakeUnsupported("stateless admin", spec.name));
}

RespValue RequestPipeline::HandlePing(const RespCommand& cmd) {
  if (cmd.ArgCount() == 1) return RespValue::SimpleString("PONG");
  return RespValue::BulkString(cmd.args[1]);
}

RespValue RequestPipeline::HandleEcho(const RespCommand& cmd) {
  return RespValue::BulkString(cmd.args[1]);
}

RespValue RequestPipeline::HandleQuit(const RespCommand& /*cmd*/) {
  close_requested_ = true;
  return RespValue::SimpleString("OK");
}

RespValue RequestPipeline::HandleHello(const RespCommand& cmd) {
  uint8_t negotiated = state_.protocol_version;
  if (cmd.ArgCount() >= 2) {
    const auto& proto = cmd.args[1];
    if (proto == "3") {
      if (deps_.metrics != nullptr) deps_.metrics->RecordProtocol(3);
      return RespValue::Error(ErrorPrefix::kNoProto, "unsupported protocol version");
    }
    if (proto != "2") {
      return RespValue::Error(ErrorPrefix::kNoProto, "unsupported protocol version");
    }
    negotiated = 2;

    // AUTH args are accepted and discarded; credential verification lands with AUTH.
    for (size_t i = 2; i < cmd.ArgCount(); ++i) {
      if (IEquals(cmd.args[i], "AUTH")) {
        if (i + 2 >= cmd.ArgCount()) {
          return RespValue::Error(ErrorPrefix::kErr, "syntax error in HELLO AUTH");
        }
        i += 2;
      } else if (IEquals(cmd.args[i], "SETNAME")) {
        if (i + 1 >= cmd.ArgCount()) {
          return RespValue::Error(ErrorPrefix::kErr, "syntax error in HELLO SETNAME");
        }
        state_.client_name = cmd.args[i + 1];
        ++i;
      } else {
        return RespValue::Error(ErrorPrefix::kErr, "syntax error");
      }
    }
  }

  if (deps_.metrics != nullptr) deps_.metrics->RecordProtocol(negotiated);
  ABYSS_LOG_DEBUG(Logger(), "hello handshake", {"client_id", state_.client_id},
                  {"proto", static_cast<int64_t>(negotiated)});

  return RespValue::Array({
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
  });
}

RespValue RequestPipeline::HandleTime(const RespCommand& /*cmd*/) {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now).count();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now).count() % 1'000'000;
  return RespValue::Array({
      RespValue::BulkString(std::to_string(seconds)),
      RespValue::BulkString(std::to_string(micros)),
  });
}

RespValue RequestPipeline::HandleClient(const RespCommand& cmd) {
  if (cmd.ArgCount() < 2) return MakeArityError("CLIENT");
  const auto sub = Uppercase(cmd.args[1]);
  if (sub == "ID") return RespValue::Integer(static_cast<int64_t>(state_.client_id));
  if (sub == "GETNAME") {
    if (state_.client_name.empty()) return RespValue::Null();
    return RespValue::BulkString(state_.client_name);
  }
  if (sub == "SETNAME") {
    if (cmd.ArgCount() != 3) return MakeArityError("CLIENT SETNAME");
    state_.client_name = cmd.args[2];
    return RespValue::SimpleString("OK");
  }
  if (sub == "NO-EVICT") {
    // Phase 1 records the flag; the hot store honours memory pressure policy,
    // not per-client opt-out. Reserved for future eviction-policy work.
    return RespValue::SimpleString("OK");
  }
  std::string msg = "Unknown CLIENT subcommand or wrong number of arguments for '";
  msg.append(cmd.args[1]);
  msg.push_back('\'');
  return RespValue::Error(ErrorPrefix::kErr, std::move(msg));
}

RespValue RequestPipeline::HandleReset(const RespCommand& /*cmd*/) {
  state_.client_name.clear();
  return RespValue::SimpleString("RESET");
}

RespValue RequestPipeline::MakeUnsupported(std::string_view category, std::string_view cmd_name) {
  std::string msg;
  msg.append(category);
  msg.append(" not configured (");
  msg.append(cmd_name);
  msg.append(")");
  return RespValue::Error(ErrorPrefix::kErr, std::move(msg));
}

RespValue RequestPipeline::MakeLoading() {
  return RespValue::Error(ErrorPrefix::kLoading, "Abyss is loading the dataset in memory");
}

}  // namespace abyss::resp
