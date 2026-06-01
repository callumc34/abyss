#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/slot.h"
#include "abyss/core/slot_shard_map.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/node_identity.h"
#include "abyss/resp/server_stats.h"

namespace abyss::resp {
namespace {

using core::RespValue;

class FakeStats : public ServerStatsProvider {
 public:
  explicit FakeStats(ServerStats stats) : stats_(stats) {}
  ServerStats Snapshot() const override { return stats_; }

 private:
  ServerStats stats_;
};

ServerStats MakeStats(uint32_t shard_count) {
  ServerStats s{};
  s.shard_count = shard_count;
  s.tcp_port = 6379;
  s.bind_address = "127.0.0.1";
  s.role = "master";
  return s;
}

core::RespCommand KeyslotCmd(std::string key) {
  return core::RespCommand{.args = {"CLUSTER", "KEYSLOT", std::move(key)}};
}

// Collects the [first,last] ranges from a CLUSTER SLOTS reply.
std::vector<std::pair<int64_t, int64_t>> SlotRangesOf(const RespValue& reply) {
  std::vector<std::pair<int64_t, int64_t>> ranges;
  for (const auto& entry : reply.AsArray()) {
    const auto& e = entry.AsArray();
    ranges.emplace_back(e[0].AsInteger(), e[1].AsInteger());
  }
  return ranges;
}

TEST(ClusterTest, SlotsAdvertisePerShardDisjointCover) {
  const FakeStats stats(MakeStats(/*shard_count=*/8));
  const NodeIdentity identity("00000000-0000-4000-8000-000000000000");

  const auto reply = HandleCluster("SLOTS", {}, stats, identity);
  const auto ranges = SlotRangesOf(reply);

  // One entry per owning shard (not the single [0,16383] stub).
  ASSERT_EQ(ranges.size(), 8u);

  // Disjoint, gap-free cover of [0, kSlotCount).
  int64_t expected = 0;
  for (const auto& [first, last] : ranges) {
    EXPECT_EQ(first, expected);
    EXPECT_GE(last, first);
    expected = last + 1;
  }
  EXPECT_EQ(expected, static_cast<int64_t>(core::kSlotCount));
}

TEST(ClusterTest, ShardsMirrorTheSamePartition) {
  const FakeStats stats(MakeStats(/*shard_count=*/4));
  const NodeIdentity identity("11111111-1111-4111-8111-111111111111");

  const auto reply = HandleCluster("SHARDS", {}, stats, identity);
  ASSERT_EQ(reply.AsArray().size(), 4u);

  int64_t expected = 0;
  for (const auto& shard : reply.AsArray()) {
    const auto& fields = shard.AsArray();
    // ["slots", [first, last], "nodes", [...]]
    ASSERT_GE(fields.size(), 2u);
    EXPECT_EQ(fields[0].AsString(), "slots");
    const auto& range = fields[1].AsArray();
    EXPECT_EQ(range[0].AsInteger(), expected);
    expected = range[1].AsInteger() + 1;
  }
  EXPECT_EQ(expected, static_cast<int64_t>(core::kSlotCount));
}

TEST(ClusterTest, SingleShardIsOneFullRange) {
  const FakeStats stats(MakeStats(/*shard_count=*/1));
  const NodeIdentity identity("22222222-2222-4222-8222-222222222222");

  const auto ranges = SlotRangesOf(HandleCluster("SLOTS", {}, stats, identity));
  ASSERT_EQ(ranges.size(), 1u);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, static_cast<int64_t>(core::kSlotCount) - 1);
}

TEST(ClusterTest, MovedTargetOwnsKeysDataShard) {
  // The shard whose advertised SLOTS range contains SlotForKey(k) is exactly
  // ShardForKey(k) — invariant 4, true by construction under slot-unified
  // routing. We verify it through the advertised ranges.
  constexpr uint32_t kShards = 8;
  const FakeStats stats(MakeStats(kShards));
  const NodeIdentity identity("33333333-3333-4333-8333-333333333333");
  const auto ranges = SlotRangesOf(HandleCluster("SLOTS", {}, stats, identity));

  for (const char* k : {"foo", "bar", "{user}.a", "key:42", "user1000"}) {
    const auto slot = core::SlotForKey(k);
    // Find the advertised range index that contains this slot.
    uint32_t owning_index = 0;
    for (uint32_t i = 0; i < ranges.size(); ++i) {
      if (slot >= ranges[i].first && slot <= ranges[i].second) {
        owning_index = i;
        break;
      }
    }
    // Ranges are emitted in shard order, so the index is the owning shard id.
    EXPECT_EQ(owning_index, core::ShardForKey(k, kShards)) << "key=" << k;
  }
}

TEST(ClusterTest, KeyslotReturnsWireSlotUnchanged) {
  const FakeStats stats(MakeStats(/*shard_count=*/8));
  const NodeIdentity identity("44444444-4444-4444-8444-444444444444");

  EXPECT_EQ(HandleCluster("KEYSLOT", KeyslotCmd("foo"), stats, identity).AsInteger(),
            core::KeySlot("foo"));
  EXPECT_EQ(HandleCluster("KEYSLOT", KeyslotCmd("foo"), stats, identity).AsInteger(), 12182);
}

TEST(ClusterTest, KeyslotHonoursHashtagCoLocation) {
  const FakeStats stats(MakeStats(/*shard_count=*/8));
  const NodeIdentity identity("55555555-5555-4555-8555-555555555555");

  const auto a = HandleCluster("KEYSLOT", KeyslotCmd("{user1}.a"), stats, identity).AsInteger();
  const auto b = HandleCluster("KEYSLOT", KeyslotCmd("{user1}.b"), stats, identity).AsInteger();
  EXPECT_EQ(a, b);
}

}  // namespace
}  // namespace abyss::resp
