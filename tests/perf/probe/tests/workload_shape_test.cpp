#include "workload_shape.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace abyss::perf::probe {
namespace {

constexpr uint64_t kKeys = 100'000;

// The TTL a key was created with, or 0.
uint64_t CreatedTtlMs(const WorkloadShape& shape, uint64_t index, std::mt19937_64& rng) {
  for (const core::RespCommand& cmd : shape.Create(index, rng)) {
    if (cmd.args.front() == "PEXPIRE") return std::stoull(cmd.args[2]);
    if (cmd.args.front() == "SET" && cmd.args.size() == 5 && cmd.args[3] == "PX") {
      return std::stoull(cmd.args[4]);
    }
  }
  return 0;
}

TEST(WorkloadShapeTest, TraceCreatesMostKeysOfEveryTypeWithATtl) {
  const WorkloadShape shape{{.trace = true}, kKeys};
  std::mt19937_64 rng{7};  // NOLINT(bugprone-random-generator-seed): reproducible.
  uint64_t with_ttl = 0;
  uint64_t short_ttl = 0;
  uint64_t collections = 0;
  uint64_t collections_with_ttl = 0;
  for (uint64_t i = 0; i < kKeys; ++i) {
    const uint64_t ttl = CreatedTtlMs(shape, i, rng);
    const bool collection = shape.TypeOf(i) != KeyType::kString;
    with_ttl += ttl > 0 ? 1 : 0;
    short_ttl += ttl > 0 && ttl < 300'000 ? 1 : 0;
    collections += collection ? 1 : 0;
    collections_with_ttl += collection && ttl > 0 ? 1 : 0;
  }
  EXPECT_NEAR(static_cast<double>(with_ttl) / kKeys, 0.85, 0.01);
  EXPECT_NEAR(static_cast<double>(short_ttl) / static_cast<double>(with_ttl), 0.47, 0.01);
  EXPECT_NEAR(static_cast<double>(collections) / kKeys, 0.2, 0.01);
  EXPECT_NEAR(static_cast<double>(collections_with_ttl) / static_cast<double>(collections), 0.85,
              0.02);
}

TEST(WorkloadShapeTest, CollectionFractionSplitsSetsAndSortedSets) {
  const WorkloadShape shape{{.trace = true, .collection_fraction = 1.0}, kKeys};
  uint64_t sets = 0;
  uint64_t zsets = 0;
  for (uint64_t i = 0; i < kKeys; ++i) {
    sets += shape.TypeOf(i) == KeyType::kSet ? 1 : 0;
    zsets += shape.TypeOf(i) == KeyType::kZset ? 1 : 0;
  }
  EXPECT_EQ(sets + zsets, kKeys);
  EXPECT_NEAR(static_cast<double>(sets) / kKeys, 0.5, 0.01);
}

TEST(WorkloadShapeTest, FixedShapeWritesStringsWithNoTtl) {
  const WorkloadShape shape{{.value_bytes = 64}, 100};
  std::mt19937_64 rng{7};  // NOLINT(bugprone-random-generator-seed): reproducible.
  for (uint64_t i = 0; i < 100; ++i) {
    const auto created = shape.Create(i, rng);
    ASSERT_EQ(created.size(), 1U);
    EXPECT_EQ(created.front().args,
              (std::vector<std::string>{"SET", shape.Key(i), std::string(64, 'x')}));
  }
}

}  // namespace
}  // namespace abyss::perf::probe
