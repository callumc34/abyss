#include "abyss/resp/request_pipeline.h"

#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

#include "abyss/resp/parser.h"
#include "abyss/resp/serializer.h"
#include "abyss/version.h"

namespace abyss::resp {
namespace {

using core::ErrorPrefix;
using core::RespCommand;
using core::RespValue;

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return out;
}

bool IEquals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
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
  std::string lower;
  lower.reserve(name.size());
  for (const char c : name) {
    lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  message.append(lower);
  message.append("' command");
  return RespValue::Error(ErrorPrefix::kErr, std::move(message));
}

}  // namespace

RequestPipeline::RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                                 core::CommandDispatcher* dispatcher)
    : registry_(registry), dispatcher_(dispatcher), state_(std::move(state)) {}

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
      auto response = RespValue::Error(ErrorPrefix::kErr,
                                       std::string("Protocol error: ") + parsed.error().message());
      auto bytes = Serializer::Serialize(response);
      output.insert(output.end(), bytes.begin(), bytes.end());
      // Malformed input: we can't know how far to advance. Consume everything
      // and let the caller decide whether to close the connection.
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
  auto result = registry_.Classify(cmd);
  if (!result.has_value()) {
    if (result.error().code() == core::ErrorCode::kNotFound) {
      const std::string_view first_arg = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
      return MakeUnknownCommandError(cmd.Name(), first_arg);
    }
    return MakeArityError(cmd.Name());
  }
  return DispatchKnown(**result, cmd);
}

RespValue RequestPipeline::DispatchKnown(const CommandSpec& spec, const RespCommand& cmd) {
  switch (spec.dispatch) {
    case Dispatch::kStateless:
      return HandleAdminStateless(spec.name, cmd);
    case Dispatch::kTieredRead:
      if (dispatcher_ == nullptr) return NotImplemented(spec.name, "no engine configured");
      {
        auto result = dispatcher_->DispatchRead(spec.name, cmd);
        if (!result.has_value()) {
          return RespValue::Error(ErrorPrefix::kErr, result.error().message());
        }
        return std::move(*result);
      }
    case Dispatch::kWritePath:
      if (dispatcher_ == nullptr) return NotImplemented(spec.name, "no engine configured");
      {
        auto result = dispatcher_->DispatchWrite(spec.name, core::RespCommand(cmd));
        if (!result.has_value()) {
          return RespValue::Error(ErrorPrefix::kErr, result.error().message());
        }
        return std::move(*result);
      }
    case Dispatch::kConditionalWrite:
      return NotImplemented(spec.name, "requires resolver");
    case Dispatch::kConsumerRpc:
      return NotImplemented(spec.name, "requires consumer RPC integration");
  }
  return NotImplemented(spec.name, "unreachable dispatch");
}

RespValue RequestPipeline::HandleAdminStateless(std::string_view name, const RespCommand& cmd) {
  using Handler = RespValue (RequestPipeline::*)(const RespCommand&);
  struct Entry {
    std::string_view name;
    Handler handler;
  };
  static constexpr auto kDispatch = std::to_array<Entry>({
      {"PING", &RequestPipeline::HandlePing},
      {"ECHO", &RequestPipeline::HandleEcho},
      {"QUIT", &RequestPipeline::HandleQuit},
      {"HELLO", &RequestPipeline::HandleHello},
      {"TIME", &RequestPipeline::HandleTime},
      {"COMMAND", &RequestPipeline::HandleCommand},
      {"CLIENT", &RequestPipeline::HandleClient},
      {"RESET", &RequestPipeline::HandleReset},
  });
  for (const auto& entry : kDispatch) {
    if (entry.name == name) {
      return (this->*entry.handler)(cmd);
    }
  }
  return NotImplemented(name, "stateless admin handler not yet wired");
}

RespValue RequestPipeline::HandlePing(const RespCommand& cmd) {
  if (cmd.ArgCount() == 1) {
    return RespValue::SimpleString("PONG");
  }
  // PING with a message echoes it as a bulk string.
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
  if (cmd.ArgCount() >= 2) {
    const auto& proto = cmd.args[1];
    if (proto != "2") {
      return RespValue::Error(ErrorPrefix::kNoProto, "unsupported protocol version");
    }

    for (size_t i = 2; i < cmd.ArgCount(); ++i) {
      if (IEquals(cmd.args[i], "AUTH")) {
        if (i + 2 >= cmd.ArgCount()) {
          return RespValue::Error(ErrorPrefix::kErr, "syntax error in HELLO AUTH");
        }
        i += 2;  // TODO: AUthentication
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

  return RespValue::Array({
      RespValue::BulkString("server"),
      RespValue::BulkString("abyss"),
      RespValue::BulkString("version"),
      RespValue::BulkString(kVersion),
      RespValue::BulkString("proto"),
      RespValue::Integer(2),
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

RespValue RequestPipeline::HandleCommand(const RespCommand& cmd) {
  if (cmd.ArgCount() == 1) {
    // TODO: Implement
    return RespValue::Array({});
  }
  if (IEquals(cmd.args[1], "COUNT")) {
    return RespValue::Integer(static_cast<int64_t>(registry_.Size()));
  }
  return NotImplemented("COMMAND", "only COMMAND COUNT is wired; INFO/DOCS pending");
}

RespValue RequestPipeline::HandleClient(const RespCommand& cmd) {
  if (cmd.ArgCount() < 2) {
    return MakeArityError("CLIENT");
  }
  auto sub = Uppercase(cmd.args[1]);
  if (sub == "ID") {
    return RespValue::Integer(static_cast<int64_t>(state_.client_id));
  }
  if (sub == "GETNAME") {
    if (state_.client_name.empty()) {
      return RespValue::Null();
    }
    return RespValue::BulkString(state_.client_name);
  }
  if (sub == "SETNAME") {
    if (cmd.ArgCount() != 3) {
      return MakeArityError("CLIENT SETNAME");
    }
    state_.client_name = cmd.args[2];
    return RespValue::SimpleString("OK");
  }
  if (sub == "NO-EVICT") {
    // TODO: Enforce
    return RespValue::SimpleString("OK");
  }
  return NotImplemented("CLIENT", "subcommand not implemented");
}

RespValue RequestPipeline::HandleReset(const RespCommand& /*cmd*/) {
  state_.client_name.clear();
  return RespValue::SimpleString("RESET");
}

RespValue RequestPipeline::NotImplemented(std::string_view name, std::string_view reason) {
  std::string message = "not implemented (";
  message.append(reason);
  message.append("): ");
  message.append(name);
  return RespValue::Error(ErrorPrefix::kErr, std::move(message));
}

}  // namespace abyss::resp
