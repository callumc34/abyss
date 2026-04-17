#include "abyss/hot/shard_router.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace abyss::hot {
namespace {

TEST(ShardRouterTest, ConsistentForSameKey) {
  auto a = ComputeShard("mykey", 64);
  auto b = ComputeShard("mykey", 64);
  EXPECT_EQ(a, b);
}

TEST(ShardRouterTest, DeterministicKnownValue) {
  auto shard = ComputeShard("hello", 64);
  EXPECT_LT(shard, 64u);
  auto shard2 = ComputeShard("hello", 64);
  EXPECT_EQ(shard, shard2);
}

TEST(ShardRouterTest, ResultWithinRange) {
  for (int i = 0; i < 1000; ++i) {
    auto key = "key:" + std::to_string(i);
    auto shard = ComputeShard(key, 64);
    EXPECT_LT(shard, 64u);
  }
}

TEST(ShardRouterTest, ReasonableDistribution) {
  constexpr uint32_t kShardCount = 64;
  constexpr int kKeyCount = 100000;
  std::vector<int> counts(kShardCount, 0);

  std::mt19937 rng(42);
  for (int i = 0; i < kKeyCount; ++i) {
    auto key = std::to_string(rng());
    auto shard = ComputeShard(key, kShardCount);
    counts[shard]++;
  }

  int fair_share = kKeyCount / kShardCount;
  for (uint32_t i = 0; i < kShardCount; ++i) {
    EXPECT_GT(counts[i], fair_share / 4) << "shard " << i << " is severely underrepresented";
    EXPECT_LT(counts[i], fair_share * 4) << "shard " << i << " is severely overrepresented";
  }
}

TEST(ShardRouterTest, EmptyKey) {
  auto shard = ComputeShard("", 64);
  EXPECT_LT(shard, 64u);
}

TEST(ShardRouterTest, SingleByteKeys) {
  for (int c = 0; c < 256; ++c) {
    std::string key(1, static_cast<char>(c));
    auto shard = ComputeShard(key, 64);
    EXPECT_LT(shard, 64u);
  }
}

TEST(ShardRouterTest, LargeKey) {
  std::string key(65536, 'x');
  auto shard = ComputeShard(key, 64);
  EXPECT_LT(shard, 64u);
}

TEST(ShardRouterTest, ShardCountOne) {
  EXPECT_EQ(ComputeShard("any_key", 1), 0u);
  EXPECT_EQ(ComputeShard("another", 1), 0u);
}

TEST(ShardRouterTest, ShardCountTwo) {
  bool seen_zero = false;
  bool seen_one = false;
  for (int i = 0; i < 100; ++i) {
    auto shard = ComputeShard("key:" + std::to_string(i), 2);
    EXPECT_LT(shard, 2u);
    if (shard == 0) seen_zero = true;
    if (shard == 1) seen_one = true;
  }
  EXPECT_TRUE(seen_zero);
  EXPECT_TRUE(seen_one);
}

TEST(ShardRouterTest, BinaryKeyWithNullBytes) {
  std::string key = "hello\0world";
  key.push_back('\0');
  key += "more";
  auto shard = ComputeShard(key, 64);
  EXPECT_LT(shard, 64u);
}

}  // namespace
}  // namespace abyss::hot
