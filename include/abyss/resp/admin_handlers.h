#pragma once

#include "abyss/core/resp_types.h"

namespace abyss::resp {

class CommandRegistry;
class ConfigProvider;
class NodeIdentity;
class ServerStatsProvider;

core::RespValue HandleInfo(const core::RespCommand& cmd, const ServerStatsProvider& stats);

core::RespValue HandleDbsize(const ServerStatsProvider& stats);

// The loading flag enables the narrower {SLOTS, INFO, MYID} allowlist.
core::RespValue HandleCluster(const core::RespCommand& cmd, const ServerStatsProvider& stats,
                              const NodeIdentity& identity, bool loading);

core::RespValue HandleConfig(const core::RespCommand& cmd, const ConfigProvider& config);

core::RespValue HandleCommandIntrospect(const core::RespCommand& cmd,
                                        const CommandRegistry& registry);

}  // namespace abyss::resp
