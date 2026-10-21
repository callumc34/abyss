#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>

#ifdef _WIN32
#include <windows.h>
#else
#include <ctime>
#endif

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

// This thread's CPU time, which a preemption does not add to.
std::chrono::nanoseconds ThreadCpu() {
#ifdef _WIN32
  FILETIME creation;
  FILETIME exit;
  FILETIME kernel;
  FILETIME user;
  if (GetThreadTimes(GetCurrentThread(), &creation, &exit, &kernel, &user) == 0) return {};
  const auto ticks = [](const FILETIME& t) {
    return (static_cast<int64_t>(t.dwHighDateTime) << 32) | t.dwLowDateTime;
  };
  return std::chrono::nanoseconds{(ticks(kernel) + ticks(user)) * 100};
#else
  timespec ts{};
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) return {};
  return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
#endif
}

// Windows counts thread CPU in ~15.6 ms ticks, so one short hold can
// read as a whole tick.
#ifdef _WIN32
constexpr std::chrono::milliseconds kMaxHoldCpu{40};
#else
constexpr std::chrono::milliseconds kMaxHoldCpu{10};
#endif

struct PassHolds {
  size_t count = 0;
  size_t max_examined = 0;
  core::SteadyClock::duration max_held{};
  std::chrono::nanoseconds max_cpu{};
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
  // Nothing drained while loading, so the writes cannot evict.
  core::SequenceId drained = 0;
  // A pass reads the horizon just before each hold, so its CPU time
  // then starts the hold's.
  bool measuring = false;
  std::chrono::nanoseconds cpu_mark{};
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = size_t{16} << 20,
      .shard_count = 1,
      .backpressure_ratio = 100,
      .drained =
          [&](core::ShardId) {
            if (measuring) cpu_mark = ThreadCpu();
            return drained;
          },
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
          .count();
  // Load past the budget: the memory pass is under test, not the writes.
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
  drained = kAllDrained;
  ASSERT_GT(store.Stats()->used_bytes, store.Stats()->max_bytes);

  std::array<PassHolds, 5> holds{};
  store.SetHoldObserverForTesting([&holds, &cpu_mark](metrics::MaintenancePass pass,
                                                      core::SteadyClock::duration held,
                                                      size_t examined) {
    PassHolds& of = holds.at(static_cast<size_t>(pass));
    ++of.count;
    of.max_examined = std::max(of.max_examined, examined);
    of.max_held = std::max(of.max_held, held);
    of.max_cpu = std::max(of.max_cpu, ThreadCpu() - cpu_mark);
    if (held > HoldBudget::kHoldTime) ++of.over;
  });

  clock.Advance(4s);
  store.SetAccessTime(clock.SteadyNow());
  measuring = true;
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
    // Work the entry count misses, as a scan, still shows in CPU time,
    // which a preempted hold does not gain, where its wall time does.
    EXPECT_LT(of.max_cpu, kMaxHoldCpu);
    std::cout << "pass " << metrics::ToStringView(static_cast<metrics::MaintenancePass>(pass))
              << ": " << of.count << " holds, max " << of.max_examined << " examined, max "
              << std::chrono::duration<double, std::micro>(of.max_held).count() << " us held, "
              << std::chrono::duration<double, std::micro>(of.max_cpu).count() << " us CPU, "
              << of.over << " over 1 ms\n";
  }
  EXPECT_LE(store.Stats()->used_bytes, store.Stats()->max_bytes);
}

}  // namespace
}  // namespace abyss::hot
