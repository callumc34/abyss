#include <cctype>
#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/slot.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/node_identity.h"
#include "abyss/resp/server_stats.h"

namespace abyss::resp {
namespace {

using core::ErrorPrefix;
using core::RespValue;

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return out;
}

RespValue UnknownSubcommand(std::string_view sub) {
  std::string msg = "Unknown CLUSTER subcommand or wrong number of arguments for '";
  msg.append(sub);
  msg.push_back('\'');
  return RespValue::Error(ErrorPrefix::kErr, std::move(msg));
}

RespValue Loading() {
  return RespValue::Error(ErrorPrefix::kLoading, "Abyss is loading the dataset in memory");
}

RespValue WrongArity(std::string_view sub) {
  std::string msg = "Wrong number of arguments for 'CLUSTER ";
  msg.append(sub);
  msg.append("'");
  return RespValue::Error(ErrorPrefix::kErr, std::move(msg));
}

RespValue HandleSlots(const ServerStats& stats, const NodeIdentity& identity) {
  return RespValue::Array({
      RespValue::Array({
          RespValue::Integer(0),
          RespValue::Integer(static_cast<int64_t>(core::kSlotCount) - 1),
          RespValue::Array({
              RespValue::BulkString(std::string(stats.bind_address)),
              RespValue::Integer(stats.tcp_port),
              RespValue::BulkString(std::string(identity.Id())),
          }),
      }),
  });
}

RespValue HandleShards(const ServerStats& stats, const NodeIdentity& identity) {
  return RespValue::Array({
      RespValue::Array({
          RespValue::BulkString("slots"),
          RespValue::Array({
              RespValue::Integer(0),
              RespValue::Integer(static_cast<int64_t>(core::kSlotCount) - 1),
          }),
          RespValue::BulkString("nodes"),
          RespValue::Array({
              RespValue::Array({
                  RespValue::BulkString("id"),
                  RespValue::BulkString(std::string(identity.Id())),
                  RespValue::BulkString("endpoint"),
                  RespValue::BulkString(std::string(stats.bind_address)),
                  RespValue::BulkString("ip"),
                  RespValue::BulkString(std::string(stats.bind_address)),
                  RespValue::BulkString("port"),
                  RespValue::Integer(stats.tcp_port),
                  RespValue::BulkString("role"),
                  RespValue::BulkString(std::string(stats.role)),
                  RespValue::BulkString("health"),
                  RespValue::BulkString("online"),
              }),
          }),
      }),
  });
}

RespValue HandleNodes(const ServerStats& stats, const NodeIdentity& identity) {
  std::string line;
  line.append(identity.Id());
  line.push_back(' ');
  line.append(stats.bind_address);
  line.push_back(':');
  line.append(std::to_string(stats.tcp_port));
  line.append("@0 myself,master - 0 0 0 connected 0-");
  line.append(std::to_string(core::kSlotCount - 1));
  line.push_back('\n');
  return RespValue::BulkString(std::move(line));
}

RespValue HandleInfo() {
  std::string out;
  out.append("cluster_enabled:0\r\n");
  out.append("cluster_state:ok\r\n");
  out.append("cluster_slots_assigned:");
  out.append(std::to_string(core::kSlotCount));
  out.append("\r\ncluster_slots_ok:");
  out.append(std::to_string(core::kSlotCount));
  out.append("\r\ncluster_slots_pfail:0\r\n");
  out.append("cluster_slots_fail:0\r\n");
  out.append("cluster_known_nodes:1\r\n");
  out.append("cluster_size:1\r\n");
  out.append("cluster_current_epoch:0\r\n");
  out.append("cluster_my_epoch:0\r\n");
  return RespValue::BulkString(std::move(out));
}

RespValue HandleMyId(const NodeIdentity& identity) {
  return RespValue::BulkString(std::string(identity.Id()));
}

RespValue HandleKeyslot(const core::RespCommand& cmd) {
  if (cmd.ArgCount() != 3) return WrongArity("KEYSLOT");
  return RespValue::Integer(core::KeySlot(cmd.args[2]));
}

RespValue HandleCountKeysInSlot(const core::RespCommand& cmd, const ServerStats& stats) {
  if (cmd.ArgCount() != 3) return WrongArity("COUNTKEYSINSLOT");
  uint32_t slot = 0;
  const auto& arg = cmd.args[2];
  auto [ptr, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), slot);
  if (ec != std::errc{} || ptr != arg.data() + arg.size() || slot >= core::kSlotCount) {
    return RespValue::Integer(0);
  }
  return RespValue::Integer(static_cast<int64_t>(stats.hot_key_count + stats.cold_key_count));
}

}  // namespace

core::RespValue HandleCluster(const core::RespCommand& cmd, const ServerStatsProvider& stats_prov,
                              const NodeIdentity& identity, bool loading) {
  if (cmd.ArgCount() < 2) return WrongArity("");
  const auto sub = Uppercase(cmd.args[1]);

  // Narrower loading allowlist per ADP-005.
  if (loading && sub != "SLOTS" && sub != "INFO" && sub != "MYID") {
    return Loading();
  }

  const auto snap = stats_prov.Snapshot();

  if (sub == "SLOTS") return HandleSlots(snap, identity);
  if (sub == "SHARDS") return HandleShards(snap, identity);
  if (sub == "NODES") return HandleNodes(snap, identity);
  if (sub == "INFO") return HandleInfo();
  if (sub == "MYID") return HandleMyId(identity);
  if (sub == "KEYSLOT") return HandleKeyslot(cmd);
  if (sub == "COUNTKEYSINSLOT") return HandleCountKeysInSlot(cmd, snap);
  return UnknownSubcommand(cmd.args[1]);
}

}  // namespace abyss::resp
