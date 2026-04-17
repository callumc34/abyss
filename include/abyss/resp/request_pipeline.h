#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/resp/command_registry.h"

namespace abyss::resp {

// Per-connection state maintained by the frontend. Not persisted.
struct ConnectionState {
  uint64_t client_id = 0;
  std::string client_name;
  int protocol_version = 2;  // RESP2 only.
};

// Orchestrates parse, classify, dispatch, serialize for a single connection.
//
// Currently handles kStateless admin commands directly (PING, ECHO, QUIT,
// HELLO, TIME, COMMAND COUNT, CLIENT ID|GETNAME|SETNAME). Other dispatch
// classes (kTieredRead, kWritePath, kConditionalWrite, kConsumerRpc) return a
// not-implemented error — they will route to the tiering engine / Resolver
// when those components land.
//
// Thread safety: NOT thread-safe. One RequestPipeline instance belongs to one
// connection and is driven by one I/O thread at a time. Concurrent connections
// each have their own pipeline. The shared `CommandRegistry` is read-only and
// safe to reference from many pipelines simultaneously.
class RequestPipeline {
 public:
  RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                  core::CommandDispatcher* dispatcher = nullptr);
  ~RequestPipeline() = default;

  RequestPipeline(const RequestPipeline&) = delete;
  RequestPipeline& operator=(const RequestPipeline&) = delete;
  RequestPipeline(RequestPipeline&&) = delete;
  RequestPipeline& operator=(RequestPipeline&&) = delete;

  // Indicates whether the connection should be closed after this batch.
  struct ProcessResult {
    size_t bytes_consumed = 0;
    bool close_requested = false;
  };

  ProcessResult Process(std::span<const uint8_t> input, std::vector<uint8_t>& output);

  // Dispatch a single parsed command and return the response RespValue.
  core::RespValue Dispatch(const core::RespCommand& cmd);

  const ConnectionState& state() const { return state_; }

 private:
  core::RespValue DispatchKnown(const CommandSpec& spec, const core::RespCommand& cmd);
  core::RespValue HandleAdminStateless(std::string_view name, const core::RespCommand& cmd);
  core::RespValue NotImplemented(std::string_view name, std::string_view reason);

  core::RespValue HandlePing(const core::RespCommand& cmd);
  core::RespValue HandleEcho(const core::RespCommand& cmd);
  core::RespValue HandleHello(const core::RespCommand& cmd);
  core::RespValue HandleQuit(const core::RespCommand& cmd);
  core::RespValue HandleTime(const core::RespCommand& cmd);
  core::RespValue HandleCommand(const core::RespCommand& cmd);
  core::RespValue HandleClient(const core::RespCommand& cmd);
  core::RespValue HandleReset(const core::RespCommand& cmd);

  const CommandRegistry& registry_;
  core::CommandDispatcher* dispatcher_;
  ConnectionState state_;
  bool close_requested_ = false;
};

}  // namespace abyss::resp
