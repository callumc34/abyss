#include "abyss/core/slot_shard_map.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "abyss/core/shard_router.h"
#include "abyss/core/slot.h"

namespace abyss::core {
namespace {

// --- SlotForKey is exactly the wire slot (CRC16/16384) ----------------------

TEST(SlotShardMapTest, SlotForKeyEqualsKeySlot) {
  for (const char* k : {"", "foo", "bar", "{user1000}.name", "{}foo", "a{b}c", "key:42"}) {
    EXPECT_EQ(SlotForKey(k), KeySlot(k)) << "key=" << k;
  }
}

TEST(SlotShardMapTest, SlotForKeyMatchesRedisReferenceVectors) {
  // Verified against redis-cli CLUSTER KEYSLOT on Redis 7.x (same as slot_test).
  EXPECT_EQ(SlotForKey("foo"), 12182);
  EXPECT_EQ(SlotForKey("bar"), 5061);
}

// --- ShardForSlot is a total, contiguous partition of [0, kSlotCount) -------

TEST(SlotShardMapTest, ShardForSlotIsTotalContiguousPartition) {
  for (uint32_t shard_count : {1U, 3U, 16U, 64U, 256U, 16384U}) {
    std::set<ShardId> seen;
    ShardId prev = 0;
    for (uint32_t slot = 0; slot < kSlotCount; ++slot) {
      const ShardId shard = ShardForSlot(static_cast<uint16_t>(slot), shard_count);
      EXPECT_LT(shard, shard_count) << "shard_count=" << shard_count << " slot=" << slot;
      // Monotonic non-decreasing => each shard owns a contiguous slot range.
      EXPECT_GE(shard, prev) << "shard_count=" << shard_count << " slot=" << slot;
      prev = shard;
      seen.insert(shard);
    }
    // Every shard owns at least one slot when shard_count <= kSlotCount.
    EXPECT_EQ(seen.size(), shard_count) << "shard_count=" << shard_count;
  }
}

TEST(SlotShardMapTest, ShardForSlotBoundaries) {
  EXPECT_EQ(ShardForSlot(0, 1), 0U);
  EXPECT_EQ(ShardForSlot(kSlotCount - 1, 1), 0U);
  // Even split for shard_count=2: slot 8191 -> shard 0, slot 8192 -> shard 1.
  EXPECT_EQ(ShardForSlot(8191, 2), 0U);
  EXPECT_EQ(ShardForSlot(8192, 2), 1U);
}

// --- ShardForKey == ShardForSlot . SlotForKey -------------------------------

TEST(SlotShardMapTest, ShardForKeyIsShardForSlotComposedWithSlotForKey) {
  std::mt19937 rng(1234);
  for (uint32_t shard_count : {1U, 3U, 16U, 64U, 256U}) {
    for (int i = 0; i < 2000; ++i) {
      const std::string key = "key:" + std::to_string(rng());
      EXPECT_EQ(ShardForKey(key, shard_count), ShardForSlot(SlotForKey(key), shard_count))
          << "key=" << key << " shard_count=" << shard_count;
    }
  }
}

// --- Hashtag co-location holds end-to-end on the DATA shard ------------------

TEST(SlotShardMapTest, HashtagKeysShareDataShard) {
  for (uint32_t shard_count : {1U, 3U, 16U, 64U, 256U, 16384U}) {
    EXPECT_EQ(ShardForKey("{tag}a", shard_count), ShardForKey("{tag}b", shard_count))
        << "shard_count=" << shard_count;
    EXPECT_EQ(ShardForKey("{user1000}.followers", shard_count),
              ShardForKey("{user1000}.following", shard_count))
        << "shard_count=" << shard_count;
    EXPECT_EQ(ShardForKey("prefix{x}suffix", shard_count), ShardForKey("{x}", shard_count))
        << "shard_count=" << shard_count;
  }
}

// --- ComputeShard now routes through the slot (no longer xxHash) -------------

TEST(SlotShardMapTest, ComputeShardEqualsSlotDerivedPlacement) {
  std::mt19937 rng(99);
  for (uint32_t shard_count : {1U, 3U, 16U, 64U, 256U}) {
    for (int i = 0; i < 2000; ++i) {
      const std::string key = std::to_string(rng());
      EXPECT_EQ(ComputeShard(key, shard_count), ShardForSlot(SlotForKey(key), shard_count))
          << "key=" << key << " shard_count=" << shard_count;
    }
  }
}

TEST(SlotShardMapTest, ComputeShardHonoursHashtagCoLocation) {
  // Under the old xxHash placement these would scatter; under slot-unified
  // routing they co-locate, which is the defining behaviour change of ADP-014.
  EXPECT_EQ(ComputeShard("{tag}a", 64), ComputeShard("{tag}b", 64));
  EXPECT_EQ(ComputeShard("{tag}a", 64), ComputeShard("{tag}", 64));
}

}  // namespace
}  // namespace abyss::core
