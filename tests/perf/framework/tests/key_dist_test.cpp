#include "key_dist.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>

namespace abyss::perf {
namespace {

TEST(UniformDistributionTest, AllKeysReachable) {
  constexpr uint64_t kCount = 100;
  constexpr int kIters = 100'000;
  UniformDistribution dist{kCount, 42};
  std::map<uint64_t, uint64_t> hits;
  for (int i = 0; i < kIters; ++i) hits[dist.Next()]++;
  EXPECT_EQ(hits.size(), kCount) << "every key should appear at least once";
  for (const auto& [key, count] : hits) {
    EXPECT_GT(count, 0u);
    EXPECT_LT(key, kCount);
  }
}

TEST(UniformDistributionTest, RoughlyEvenFrequency) {
  constexpr uint64_t kCount = 10;
  constexpr int kIters = 100'000;
  UniformDistribution dist{kCount, 7};
  std::map<uint64_t, uint64_t> hits;
  for (int i = 0; i < kIters; ++i) hits[dist.Next()]++;
  const auto expected = kIters / kCount;
  for (const auto& [_, count] : hits) {
    const auto delta = count > expected ? count - expected : expected - count;
    EXPECT_LT(delta, expected / 5) << "expected uniform-ish";
  }
}

TEST(ZipfianDistributionTest, RejectsInvalidTheta) {
  EXPECT_THROW(ZipfianDistribution(10, 1.0, 0), std::invalid_argument);
  EXPECT_THROW(ZipfianDistribution(10, -0.1, 0), std::invalid_argument);
}

TEST(ZipfianDistributionTest, SkewProducesHotKey) {
  constexpr uint64_t kCount = 1000;
  constexpr int kIters = 100'000;
  ZipfianDistribution dist{kCount, 0.99, 42};
  std::map<uint64_t, uint64_t> hits;
  for (int i = 0; i < kIters; ++i) hits[dist.Next()]++;
  // With theta=0.99, the top key should take >5% of traffic.
  uint64_t top_hits = 0;
  for (const auto& [_, count] : hits) top_hits = std::max(top_hits, count);
  EXPECT_GT(top_hits, static_cast<uint64_t>(kIters / 20))
      << "zipf(0.99) should concentrate >5% on the hottest key";
}

TEST(LatestDistributionTest, EmphasisesHighIndices) {
  constexpr uint64_t kCount = 1000;
  constexpr int kIters = 50'000;
  LatestDistribution dist{kCount, 0.99, 42};
  uint64_t sum_upper_half = 0;
  uint64_t sum_lower_half = 0;
  for (int i = 0; i < kIters; ++i) {
    if (dist.Next() >= kCount / 2) {
      ++sum_upper_half;
    } else {
      ++sum_lower_half;
    }
  }
  EXPECT_GT(sum_upper_half, sum_lower_half * 3) << "latest should weight high indices heavily";
}

TEST(MakeKeyDistributionTest, ConstructsEachKind) {
  for (auto kind : {KeyDistConfig::Kind::kUniform, KeyDistConfig::Kind::kZipfian,
                    KeyDistConfig::Kind::kLatest}) {
    KeyDistConfig cfg;
    cfg.kind = kind;
    cfg.theta = 0.5;
    cfg.seed = 1;
    auto dist = MakeKeyDistribution(cfg, 100);
    ASSERT_NE(dist, nullptr);
    EXPECT_EQ(dist->KeyCount(), 100u);
    const auto k = dist->Next();
    EXPECT_LT(k, 100u);
  }
}

}  // namespace
}  // namespace abyss::perf
