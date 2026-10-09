#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;
namespace ops = core::ops;

constexpr int kKeys = 1'000'000;

std::string Key(int i) {
  std::array<char, 16> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%07d", i);
  return buf.data();
}

struct PassHolds {
  size_t count = 0;
  size_t max_examined = 0;
  core::SteadyClock::duration max_held{};
  // Holds past the 1 ms cap.
  size_t over = 0;
};

// One shard of 1M keys: a quarter deleted, most of the rest past their
// TTL or idle past their eviction, and what is left over the budget.
// Every pass must work in holds capped at 64 entries or about 1 ms.
TEST(HotHoldCapTest, NoMaintenanceHoldOverTheCapWithAMillionKeys) {
  abyss::testing::TestClock clock;
  // A second eviction class, so a second LRU list.
  core::EvictionPolicy policy{core::EvictionTTL{1},
                              {{.prefix = "k09", .eviction = core::EvictionTTL{86400}}}};
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = size_t{16} << 20,
      .shard_count = 1,
      .backpressure_ratio = 100,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
          .count();
  // Load past the budget: the memory pass is under test, not the writes.
  store.SetReplayMode(true);
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = Key(i);
    const bool expires = i % 4 == 0 && i < 900'000;
    const uint64_t ttl = expires ? static_cast<uint64_t>(now_ms + 500 + (i % 3000)) : 0;
    ASSERT_TRUE(
        store
            .Apply(ops::WriteOp{ops::StringSet{.key = key, .value = "v", .abs_ttl_ms = ttl}},
                   static_cast<core::SequenceId>(i) + 1)
            .has_value());
  }
  for (int i = 1; i < kKeys; i += 4) {
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::Del{.keys = {Key(i)}}},
                           static_cast<core::SequenceId>(kKeys + i) + 1)
                    .has_value());
  }
  store.SetReplayMode(false);
  ASSERT_GT(store.Stats()->used_bytes, store.Stats()->max_bytes);

  std::array<PassHolds, 5> holds{};
  store.SetHoldObserverForTesting(
      [&holds](metrics::MaintenancePass pass, core::SteadyClock::duration held, size_t examined) {
        PassHolds& of = holds.at(static_cast<size_t>(pass));
        ++of.count;
        of.max_examined = std::max(of.max_examined, examined);
        of.max_held = std::max(of.max_held, held);
        if (held > HoldBudget::kHoldTime) ++of.over;
      });

  clock.Advance(4s);
  store.SetAccessTime(clock.SteadyNow());
  const size_t reclaimed = store.GcTombstones();
  const auto expired = store.EvictExpired(clock.SteadyNow());
  const size_t memory = store.EvictToMemoryTarget();

  EXPECT_EQ(reclaimed, static_cast<size_t>(kKeys / 4));
  EXPECT_EQ(expired.by_ttl, 225'000U);
  EXPECT_EQ(expired.by_deadline, 450'000U);
  EXPECT_GT(memory, 0U);
  for (size_t pass = 0; pass < holds.size(); ++pass) {
    const PassHolds& of = holds.at(pass);
    SCOPED_TRACE(metrics::ToStringView(static_cast<metrics::MaintenancePass>(pass)));
    EXPECT_GT(of.count, 0U);
    EXPECT_LE(of.max_examined, HoldBudget::kHoldEntries);
    // The 1 ms is checked every eighth entry; the slack is for a
    // preempted hold on a loaded machine, not for unbounded work.
    EXPECT_LT(of.max_held, 25ms);
    std::cout << "pass " << metrics::ToStringView(static_cast<metrics::MaintenancePass>(pass))
              << ": " << of.count << " holds, max " << of.max_examined << " examined, max "
              << std::chrono::duration<double, std::micro>(of.max_held).count() << " us held, "
              << of.over << " over 1 ms\n";
  }
  EXPECT_LE(store.Stats()->used_bytes, store.Stats()->max_bytes);
}

}  // namespace
}  // namespace abyss::hot
