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

// Per-connection state maintained by the frontend. Not persisted.
struct ConnectionState {
  uint64_t client_id = 0;
  std::string client_name;
  int protocol_version = 2;  // RESP2 only.
};

// Nullable non-owning pointers; unconfigured services yield a client error.
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

  // Signals whether the connection should be closed after this batch.
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
  DispatchOutcome DispatchKnown(const CommandSpec& spec, const core::RespCommand& cmd);
  DispatchOutcome HandleAdminStateless(const CommandSpec& spec, const core::RespCommand& cmd);

  core::RespValue HandlePing(const core::RespCommand& cmd);
  core::RespValue HandleEcho(const core::RespCommand& cmd);
  core::RespValue HandleHello(const core::RespCommand& cmd);
  core::RespValue HandleQuit(const core::RespCommand& cmd);
  core::RespValue HandleTime(const core::RespCommand& cmd);
  core::RespValue HandleClient(const core::RespCommand& cmd);
  core::RespValue HandleReset(const core::RespCommand& cmd);

  static core::RespValue MakeUnsupported(std::string_view category, std::string_view cmd_name);
  static core::RespValue MakeLoading();

  const CommandRegistry& registry_;
  Dependencies deps_;
  ConnectionState state_;
  bool close_requested_ = false;
};

}  // namespace abyss::resp
