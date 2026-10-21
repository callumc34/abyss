#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/hot/single_shard_store.h"
#include "alloc_counter.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;
using abyss::testing::Allocs;
using abyss::testing::BigAllocs;
using abyss::testing::Frees;
namespace ops = core::ops;

// Runs `step` in capped holds until it reports done.
void InHolds(const std::function<bool(HoldBudget&)>& step) {
  for (;;) {
    HoldBudget budget = HoldBudget::Capped();
    if (step(budget)) return;
  }
}

// A capped hold examines at most 64 entries in about 1 ms, so no step
// in one may grow a structure the shard's size: a regrowing parked heap
// copied a shard's pointers, and a rehashing stub index moved every
// stub, under the lock.
TEST(MaintenanceHoldAllocTest, NoCappedHoldGrowsAStructureTheShardsSize) {
  constexpr int kKeys = 200'000;
  abyss::testing::TestClock clock;
  SingleShardStore store{SingleShardConfig{
      .stub_max_entries = kKeys,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
          .count();
  for (int i = 0; i < kKeys; ++i) {
    // Half expire by TTL, half go idle past their eviction.
    const uint64_t ttl = i % 2 == 0 ? static_cast<uint64_t>(now_ms + 500) : 0;
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{
                               .key = "k" + std::to_string(i), .value = "v", .abs_ttl_ms = ttl}},
                           core::EvictionTTL{1}, static_cast<core::SequenceId>(i) + 1)
                    .has_value());
  }
  clock.Advance(4s);
  const size_t before = BigAllocs();
  const size_t allocs_before = Allocs();
  SingleShardStore::EvictExpiredReport report;

  // Nothing drained: every due key parks.
  InHolds(
      [&](HoldBudget& budget) { return store.ExpireTtl(clock.SteadyNow(), 0, budget, report); });
  InHolds([&](HoldBudget& budget) {
    return store.EvictPastDeadline(clock.SteadyNow(), 0, budget, report);
  });
  ASSERT_EQ(report.parked, static_cast<size_t>(kKeys));
  EXPECT_EQ(BigAllocs(), before) << "a hold regrew the parked heap";
  // A chunk per 512 parked, not a block per few, as MSVC's deque gave.
  EXPECT_LE(Allocs() - allocs_before, static_cast<size_t>(kKeys / 256));

  // All drained: each leaves, an idle one leaving a stub.
  InHolds([&](HoldBudget& budget) {
    return store.ReleaseParked(clock.SteadyNow(), kAllDrained, budget, report);
  });
  EXPECT_EQ(report.Total(), static_cast<size_t>(kKeys));
  EXPECT_EQ(store.Stats().stub_entries, static_cast<size_t>(kKeys / 2));
  EXPECT_EQ(BigAllocs(), before) << "a hold rehashed the stub index";
}

// A Flush's hold takes an entry map and a stub index sized before it, at
// a shard's production reserve, and leaves the heaps' chunks to the
// graveyard: it neither allocates nor zeroes a bucket array, and frees
// no chunk per 512 parked keys or tombstones.
TEST(FlushHoldAllocTest, AFlushSwapsInBucketsBuiltBeforeItsHold) {
  constexpr size_t kReserve = size_t{1} << 20;
  constexpr int kParked = 64 * 512;
  constexpr int kDeleted = 64 * 512;
  abyss::testing::TestClock clock;
  SingleShardStore store{SingleShardConfig{
      .stub_max_entries = kReserve,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
      .reserve_keys = kReserve,
  }};
  const uint64_t empty = store.Stats().used_bytes;
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
          .count();
  core::SequenceId seq = core::kFirstSeq;
  for (int i = 0; i < kParked; ++i) {
    ASSERT_TRUE(
        store
            .Apply(ops::WriteOp{ops::StringSet{.key = "k" + std::to_string(i),
                                               .value = "v",
                                               .abs_ttl_ms = static_cast<uint64_t>(now_ms + 500)}},
                   core::EvictionTTL{86400}, seq++)
            .has_value());
  }
  for (int i = 0; i < kDeleted; ++i) {
    const std::string key = "d" + std::to_string(i);
    ASSERT_TRUE(store.Apply(ops::WriteOp{ops::Del{.keys = {key}}}, core::EvictionTTL{86400}, seq++)
                    .has_value());
  }
  clock.Advance(4s);
  SingleShardStore::EvictExpiredReport report;
  InHolds(
      [&](HoldBudget& budget) { return store.ExpireTtl(clock.SteadyNow(), 0, budget, report); });
  ASSERT_EQ(report.parked, static_cast<size_t>(kParked));

  FreshMaps fresh = store.ReservedMaps();
  Graveyard graveyard;
  store.SetGraveyard(&graveyard);
  const size_t big_allocs = BigAllocs();
  const size_t frees = Frees();
  store.Wipe(seq, std::move(fresh));
  EXPECT_EQ(BigAllocs(), big_allocs) << "the hold allocated a bucket array";
  EXPECT_LT(Frees() - frees, 16U) << "the hold freed the heaps' chunks";
  store.SetGraveyard(nullptr);
  EXPECT_EQ(store.Stats().used_bytes, empty) << "the swapped-in buckets are counted";
  EXPECT_EQ(store.Stats().key_count, 0U);
}

}  // namespace
}  // namespace abyss::hot
