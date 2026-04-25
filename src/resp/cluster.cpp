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

// Redis convention: cluster bus port = client port + 10000.
constexpr uint16_t kClusterBusPortOffset = 10000;

std::string_view AdvertiseAddress(const ServerStats& s) {
  return s.advertise_address.empty() ? s.bind_address : s.advertise_address;
}

uint16_t BusPort(uint16_t tcp_port) {
  return static_cast<uint16_t>(tcp_port + kClusterBusPortOffset);
}

RespValue HandleSlots(const ServerStats& stats, const NodeIdentity& identity) {
  return RespValue::Array({
      RespValue::Array({
          RespValue::Integer(0),
          RespValue::Integer(static_cast<int64_t>(core::kSlotCount) - 1),
          RespValue::Array({
              RespValue::BulkString(std::string(AdvertiseAddress(stats))),
              RespValue::Integer(stats.tcp_port),
              RespValue::BulkString(std::string(identity.Id())),
          }),
      }),
  });
}

RespValue HandleShards(const ServerStats& stats, const NodeIdentity& identity) {
  const auto addr = AdvertiseAddress(stats);
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
                  RespValue::BulkString(std::string(addr)),
                  RespValue::BulkString("ip"),
                  RespValue::BulkString(std::string(addr)),
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
  line.append(AdvertiseAddress(stats));
  line.push_back(':');
  line.append(std::to_string(stats.tcp_port));
  line.push_back('@');
  line.append(std::to_string(BusPort(stats.tcp_port)));
  line.append(" myself,master - 0 0 0 connected 0-");
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
  return RespValue::Integer(core::KeySlot(cmd.args[2]));
}

RespValue HandleCountKeysInSlot(const core::RespCommand& cmd, const ServerStats& stats) {
  uint32_t slot = 0;
  const auto& arg = cmd.args[2];
  auto [ptr, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), slot);
  if (ec != std::errc{} || ptr != arg.data() + arg.size() || slot >= core::kSlotCount) {
    return RespValue::Integer(0);
  }
  return RespValue::Integer(static_cast<int64_t>(stats.hot_key_count + stats.cold_key_count));
}

}  // namespace

core::RespValue HandleCluster(std::string_view subcommand, const core::RespCommand& cmd,
                              const ServerStatsProvider& stats_prov, const NodeIdentity& identity) {
  const auto snap = stats_prov.Snapshot();

  if (subcommand == "SLOTS") return HandleSlots(snap, identity);
  if (subcommand == "SHARDS") return HandleShards(snap, identity);
  if (subcommand == "NODES") return HandleNodes(snap, identity);
  if (subcommand == "INFO") return HandleInfo();
  if (subcommand == "MYID") return HandleMyId(identity);
  if (subcommand == "KEYSLOT") return HandleKeyslot(cmd);
  if (subcommand == "COUNTKEYSINSLOT") return HandleCountKeysInSlot(cmd, snap);

  return RespValue::Error(
      ErrorPrefix::kErr,
      std::string("internal: unhandled CLUSTER subcommand '").append(subcommand).append("'"));
}

}  // namespace abyss::resp
