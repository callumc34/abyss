#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/metrics/names.h"
#include "abyss/resp/command_registry.h"

namespace abyss::resp {

class ConfigProvider;
class LoadingStateProvider;
class NodeIdentity;
class RespMetrics;
class ServerStatsProvider;

struct ConnectionState {
  uint64_t client_id = 0;
  std::string client_name;
  uint8_t protocol_version = 2;
};

// Non-owning. A null required dependency surfaces as ERR internal server error
// (logged at ERROR); see RequestPipeline::InternalServerError.
struct PipelineDependencies {
  core::CommandDispatcher* dispatcher = nullptr;
  const LoadingStateProvider* loading = nullptr;
  const ServerStatsProvider* stats = nullptr;
  const ConfigProvider* config = nullptr;
  const NodeIdentity* identity = nullptr;
  RespMetrics* metrics = nullptr;
};

// One pipeline per connection; not thread-safe. Injected providers must be.
class RequestPipeline {
 public:
  using Dependencies = PipelineDependencies;

  RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                  PipelineDependencies deps = {});
  ~RequestPipeline() = default;

  RequestPipeline(const RequestPipeline&) = delete;
  RequestPipeline& operator=(const RequestPipeline&) = delete;
  RequestPipeline(RequestPipeline&&) = delete;
  RequestPipeline& operator=(RequestPipeline&&) = delete;

  struct ProcessResult {
    size_t bytes_consumed = 0;
    bool close_requested = false;
  };

  ProcessResult Process(std::span<const uint8_t> input, std::vector<uint8_t>& output);

  core::RespValue Dispatch(const core::RespCommand& cmd);

  const ConnectionState& state() const { return state_; }

 private:
  struct DispatchOutcome {
    core::RespValue response;
    metrics::RequestStatus status;
    std::string_view cmd_name;
  };

  DispatchOutcome DispatchImpl(const core::RespCommand& cmd);
  DispatchOutcome DispatchResolved(const ResolvedCommand& resolved, const core::RespCommand& cmd);
  DispatchOutcome HandleAdminStateless(const ResolvedCommand& resolved,
                                       const core::RespCommand& cmd);

  core::RespValue HandlePing(const core::RespCommand& cmd);
  core::RespValue HandleEcho(const core::RespCommand& cmd);
  DispatchOutcome HandleHello(const CommandSpec& spec, const core::RespCommand& cmd);
  core::RespValue HandleQuit();
  core::RespValue HandleTime();
  core::RespValue HandleClient(std::string_view subcommand, const core::RespCommand& cmd);
  core::RespValue HandleReset();

  // Logs `context` at ERROR; returns ERR internal server error to the wire so
  // dependency identity isn't leaked to clients.
  core::RespValue InternalServerError(std::string_view context, std::string_view cmd_name);

  static core::RespValue MakeLoading();
  static core::RespValue MakeUnknownCommand(std::string_view name, std::string_view first_arg);
  static core::RespValue MakeArityError(const ResolvedCommand& resolved);
  static core::RespValue MakeUnknownSubcommand(const CommandSpec& parent, std::string_view raw_sub);

  const CommandRegistry& registry_;
  Dependencies deps_;
  ConnectionState state_;
  bool close_requested_ = false;
};

}  // namespace abyss::resp
