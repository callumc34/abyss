#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/slot.h"
#include "abyss/core/slot_shard_map.h"
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

// One owning shard's contiguous slot range. ShardForSlot is monotonic in slot
// (contiguous range division), so each shard owns exactly one [first,last] run.
struct SlotRange {
  core::ShardId shard;
  uint16_t first;
  uint16_t last;
};

// The per-shard partition of [0, kSlotCount) that CLUSTER SLOTS/SHARDS advertise
// and that MOVED targeting resolves through (ADP-014). In single-pod every shard
// maps to this node, so the union of ranges is the full slot space, disjoint.
std::vector<SlotRange> OwnedSlotRanges(uint32_t shard_count) {
  std::vector<SlotRange> ranges;
  if (shard_count == 0) shard_count = 1;
  uint32_t slot = 0;
  while (slot < core::kSlotCount) {
    const core::ShardId shard = core::ShardForSlot(static_cast<uint16_t>(slot), shard_count);
    uint32_t last = slot;
    while (last + 1 < core::kSlotCount &&
           core::ShardForSlot(static_cast<uint16_t>(last + 1), shard_count) == shard) {
      ++last;
    }
    ranges.push_back({
        .shard = shard,
        .first = static_cast<uint16_t>(slot),
        .last = static_cast<uint16_t>(last),
    });
    slot = last + 1;
  }
  return ranges;
}

RespValue HandleSlots(const ServerStats& stats, const NodeIdentity& identity) {
  // One entry per owning shard, each advertising its contiguous slot range and
  // this node (single-pod owns every shard). The ranges cover [0, kSlotCount)
  // disjointly, so a cluster-aware client always finds the owner of any slot.
  std::vector<RespValue> entries;
  for (const auto& r : OwnedSlotRanges(stats.shard_count)) {
    entries.push_back(RespValue::Array({
        RespValue::Integer(r.first),
        RespValue::Integer(r.last),
        RespValue::Array({
            RespValue::BulkString(std::string(AdvertiseAddress(stats))),
            RespValue::Integer(stats.tcp_port),
            RespValue::BulkString(std::string(identity.Id())),
        }),
    }));
  }
  return RespValue::Array(std::move(entries));
}

RespValue HandleShards(const ServerStats& stats, const NodeIdentity& identity) {
  const auto addr = AdvertiseAddress(stats);
  std::vector<RespValue> shards;
  for (const auto& r : OwnedSlotRanges(stats.shard_count)) {
    shards.push_back(RespValue::Array({
        RespValue::BulkString("slots"),
        RespValue::Array({
            RespValue::Integer(r.first),
            RespValue::Integer(r.last),
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
    }));
  }
  return RespValue::Array(std::move(shards));
}

RespValue HandleNodes(const ServerStats& stats, const NodeIdentity& identity) {
  // CLUSTER NODES lists each node once with all its owned slot ranges trailing
  // on the line. Single-pod owns every range, so they collapse to 0-<max>, but
  // the ranges are computed (not stubbed) so multi-pod is correct by the same
  // line builder.
  std::string line;
  line.append(identity.Id());
  line.push_back(' ');
  line.append(AdvertiseAddress(stats));
  line.push_back(':');
  line.append(std::to_string(stats.tcp_port));
  line.push_back('@');
  line.append(std::to_string(BusPort(stats.tcp_port)));
  line.append(" myself,master - 0 0 0 connected");
  for (const auto& r : OwnedSlotRanges(stats.shard_count)) {
    line.push_back(' ');
    line.append(std::to_string(r.first));
    if (r.last != r.first) {
      line.push_back('-');
      line.append(std::to_string(r.last));
    }
  }
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
  // The WIRE slot (CRC16/16384, hashtag-honouring) — the value a cluster-aware
  // client routes by. Placement derives the data shard from this same slot.
  return RespValue::Integer(core::SlotForKey(cmd.args[2]));
}

RespValue HandleCountKeysInSlot(const core::RespCommand& cmd, const ServerStats& /*stats*/) {
  uint32_t slot = 0;
  const auto& arg = cmd.args[2];
  auto [ptr, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), slot);
  if (ec != std::errc{} || ptr != arg.data() + arg.size() || slot >= core::kSlotCount) {
    return RespValue::Integer(0);
  }
  // Per-slot occupancy: the key count for keys whose CRC16 slot equals `slot`.
  // ServerStats carries only whole-keyspace aggregates and no per-slot index,
  // so returning the keyspace total would be wrong (every slot would report the
  // total). Report 0 — the honest count for "no per-slot index" — rather than a
  // misleading total. The wire contract is stable for a populated slot index.
  return RespValue::Integer(0);
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
