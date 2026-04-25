#pragma once

#include <string_view>

#include "abyss/core/resp_types.h"

namespace abyss::resp {

class CommandRegistry;
class ConfigProvider;
class NodeIdentity;
class ServerStatsProvider;

// `subcommand` is the registry-canonical uppercase name; arity and loading
// gate are already enforced by the pipeline.

core::RespValue HandleInfo(const core::RespCommand& cmd, const ServerStatsProvider& stats);

core::RespValue HandleDbsize(const ServerStatsProvider& stats);

core::RespValue HandleCluster(std::string_view subcommand, const core::RespCommand& cmd,
                              const ServerStatsProvider& stats, const NodeIdentity& identity);

core::RespValue HandleConfig(std::string_view subcommand, const core::RespCommand& cmd,
                             const ConfigProvider& config);

// Empty `subcommand` means COMMAND was invoked alone.
core::RespValue HandleCommandIntrospect(std::string_view subcommand, const core::RespCommand& cmd,
                                        const CommandRegistry& registry);

}  // namespace abyss::resp
