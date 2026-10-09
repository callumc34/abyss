#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/hot/eviction_worker.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;
namespace ops = core::ops;

constexpr core::EvictionTTL kShortEviction{1};
constexpr core::EvictionTTL kLongEviction{86400};

int64_t WallMs(const abyss::testing::TestClock& clock) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
      .count();
}

std::string Key(std::string_view prefix, int i) { return std::string(prefix) + std::to_string(i); }

// What every capped hold of a pass examined.
struct Holds {
  std::vector<size_t> examined;

  size_t Count() const { return examined.size(); }
  size_t Max() const { return examined.empty() ? 0 : *std::ranges::max_element(examined); }
  size_t Total() const {
    size_t total = 0;
    for (const size_t n : examined) total += n;
    return total;
  }
};

class MaintenanceTest : public ::testing::Test {
 protected:
  static bool ResetMetrics() {
    abyss::metrics::testing::Reset();
    return true;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  bool metrics_reset_ = ResetMetrics();
  abyss::testing::TestClock clock_;
  core::EvictionPolicy policy_{kShortEviction};
  core::SequenceId horizon_ = kAllDrained;
  ShardedHotStore store_{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 1,
      .drained = [this](core::ShardId) { return horizon_; },
      .eviction_policy = &policy_,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  Holds holds_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  void SetUp() override {
    store_.SetHoldObserverForTesting(
        [this](metrics::MaintenancePass /*pass*/, core::SteadyClock::duration /*held*/,
               size_t examined) { holds_.examined.push_back(examined); });
  }

  void Set(std::string_view key, core::SequenceId seq, uint64_t abs_ttl_ms = 0) {
    auto r = store_.Apply(
        ops::WriteOp{ops::StringSet{.key = key, .value = "v", .abs_ttl_ms = abs_ttl_ms}}, seq);
    ASSERT_TRUE(r.has_value()) << r.error().message();
  }

  bool Resident(std::string_view key) {
    return store_.Exec(ops::ReadOp{ops::Exists{.keys = {key}}})->AsInteger() == 1;
  }
};

// --- Tombstone GC ---

TEST(TombstoneGcTest, TakesThoseAtOrBelowTheHorizonInSeqOrderSkippingRewrites) {
  SingleShardStore store{SingleShardConfig{}};
  const auto apply = [&store](const ops::WriteOp& op, core::SequenceId seq) {
    ASSERT_TRUE(store.Apply(op, kLongEviction, seq).has_value());
  };
  const auto del = [&apply](std::string_view key, core::SequenceId seq) {
    apply(ops::Del{.keys = {key}}, seq);
  };
  const auto tombstoned = [&store](std::string_view key) {
    return store.Probe(key) == core::HotKeyPresence::kTombstoned;
  };
  // Logged out of seq order.
  del("c", 30);
  del("a", 10);
  del("b", 20);
  del("d", 25);
  // Rewritten since: live again, and deleted again later.
  del("r", 5);
  apply(ops::StringSet{.key = "r", .value = "v"}, 6);
  del("x", 7);
  del("x", 40);

  size_t reclaimed = 0;
  HoldBudget one(1);
  EXPECT_FALSE(store.GcTombstones(20, one, reclaimed));
  EXPECT_EQ(reclaimed, 1U);
  EXPECT_FALSE(tombstoned("a")) << "the lowest seq first";
  EXPECT_TRUE(tombstoned("b"));

  HoldBudget next(1);
  EXPECT_TRUE(store.GcTombstones(20, next, reclaimed)) << "nothing else is at or below 20";
  EXPECT_FALSE(tombstoned("b"));
  EXPECT_EQ(reclaimed, 2U);

  HoldBudget last(1);
  EXPECT_TRUE(store.GcTombstones(20, last, reclaimed));
  EXPECT_EQ(last.examined(), 0U);
  EXPECT_TRUE(tombstoned("c"));
  EXPECT_TRUE(tombstoned("d"));
  EXPECT_TRUE(tombstoned("x")) << "its later delete is not drained";
  EXPECT_EQ(store.Probe("r"), core::HotKeyPresence::kPresent);

  EXPECT_EQ(store.GcTombstones(39), 2U);
  EXPECT_EQ(store.GcTombstones(40), 1U);
  EXPECT_EQ(store.Probe("r"), core::HotKeyPresence::kPresent);
}

// --- TTL expiry ---

TEST_F(MaintenanceTest, ExpiryRemovesDrainedKeysInATickAndNeverAnUndrainedOne) {
  // Long-lived, so only TTL removes them.
  policy_ = core::EvictionPolicy{kLongEviction};
  const auto ttl = static_cast<uint64_t>(WallMs(clock_) + 500);
  for (int i = 0; i < 10; ++i) Set(Key("k", i), static_cast<core::SequenceId>(i) + 1, ttl);
  horizon_ = 5;
  EvictionWorker worker(store_, EvictionWorker::Config{.tick = 1s}, clock_.SteadyFn());

  clock_.Advance(1s);
  worker.TickOnce();
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(Resident(Key("k", i))) << "reads see every expired key as absent";
  }
  EXPECT_EQ(store_.Stats()->key_count, 5U) << "the drained ones went in one tick";
  for (int tick = 0; tick < 5; ++tick) worker.TickOnce();
  EXPECT_EQ(store_.Stats()->key_count, 5U) << "an undrained key is never removed";

  horizon_ = 10;
  worker.TickOnce();
  EXPECT_EQ(store_.Stats()->key_count, 0U);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kTtlExpiredTotal, metrics::Tier::kHot)
                .value_or(0.0),
            10.0);
  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kHotExpirySweepSeconds).value_or(0),
            7U)
      << "one sweep a tick";
}

TEST(TtlIndexTest, NoKeyExpiresBeforeItsTtl) {
  abyss::testing::TestClock clock;
  SingleShardStore store{SingleShardConfig{
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  // Mid-bucket, so the bucket's start passes first.
  const auto ttl = static_cast<uint64_t>(WallMs(clock) + 1500);
  ASSERT_TRUE(store
                  .Apply(ops::WriteOp{ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = ttl}},
                         kLongEviction)
                  .has_value());
  clock.Advance(1499ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow(), kAllDrained).by_ttl, 0U);
  clock.Advance(1ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow(), kAllDrained).by_ttl, 1U);
}

TEST(TtlIndexTest, ItsBucketsAreCountedAndGoWithTheirKeys) {
  abyss::testing::TestClock clock;
  SingleShardStore store{SingleShardConfig{
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const int64_t now_ms = WallMs(clock);
  for (int i = 0; i < 30; ++i) {
    // Three seconds' buckets.
    const auto ttl = static_cast<uint64_t>(now_ms + 1000 + (int64_t{i % 3} * 1000));
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{
                               .key = Key("k", i), .value = "v", .abs_ttl_ms = ttl}},
                           kLongEviction)
                    .has_value());
  }
  ASSERT_TRUE(
      store.Apply(ops::WriteOp{ops::StringSet{.key = "no_ttl", .value = "v"}}, kLongEviction)
          .has_value());
  const auto stats = store.Stats();
  EXPECT_EQ(stats.ttl_index_bytes, 3 * kTtlBucketBytes);

  // PERSIST takes a key out of the index.
  for (int i = 0; i < 30; i += 3) {
    ASSERT_TRUE(
        store.Apply(ops::WriteOp{ops::Persist{.key = Key("k", i)}}, kLongEviction).has_value());
  }
  EXPECT_EQ(store.Stats().ttl_index_bytes, 2 * kTtlBucketBytes);
  EXPECT_EQ(store.Stats().used_bytes, stats.used_bytes - kTtlBucketBytes);

  clock.Advance(4s);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow(), kAllDrained).by_ttl, 20U);
  EXPECT_EQ(store.Stats().ttl_index_bytes, 0U);
  EXPECT_EQ(store.Stats().key_count, 11U);
}

TEST_F(MaintenanceTest, TtlBucketsEmptyInOrderAcrossCappedHolds) {
  policy_ = core::EvictionPolicy{kLongEviction};
  const int64_t now_ms = WallMs(clock_);
  for (int i = 0; i < 300; ++i) {
    Set(Key("k", i), core::kFirstSeq, static_cast<uint64_t>(now_ms + 100 + (int64_t{i} * 20)));
  }
  clock_.Advance(10s);
  const auto report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.by_ttl, 300U);
  EXPECT_LE(holds_.Max(), HoldBudget::kHoldEntries);
  EXPECT_GE(holds_.Count(), 300 / HoldBudget::kHoldEntries);
}

// --- Deadline eviction ---

TEST_F(MaintenanceTest, AReadKeyAtTheColdEndGetsASecondChance) {
  Set("read", core::kFirstSeq);
  for (int i = 0; i < 1000; ++i) Set(Key("idle:", i), core::kFirstSeq);

  clock_.Advance(2s);
  store_.SetAccessTime(clock_.SteadyNow());
  ASSERT_TRUE(store_.Exec(ops::ReadOp{ops::StringGet{.key = "read"}}).has_value());

  const auto report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.by_deadline, 1000U);
  EXPECT_TRUE(Resident("read"));
  EXPECT_EQ(store_.Stats()->key_count, 1U);
  EXPECT_GT(holds_.Count(), 1001 / HoldBudget::kHoldEntries) << "one pass, many holds";
  EXPECT_LE(holds_.Max(), HoldBudget::kHoldEntries);

  clock_.Advance(1s);
  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow()).by_deadline, 1U) << "one second after its read";
}

TEST_F(MaintenanceTest, DueUndrainedKeysParkSoNoHoldExaminesThemTwice) {
  for (int i = 0; i < 200; ++i) Set(Key("k", i), static_cast<core::SequenceId>(i) + 1);
  horizon_ = 100;
  clock_.Advance(2s);

  auto report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.by_deadline, 100U);
  EXPECT_EQ(report.parked, 100U);
  EXPECT_LE(holds_.Max(), HoldBudget::kHoldEntries);
  EXPECT_GT(store_.Stats()->unevictable_bytes, 0U);

  holds_.examined.clear();
  report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.Total() + report.parked, 0U);
  EXPECT_EQ(holds_.Total(), 0U) << "parked keys wait for the horizon, unexamined";

  // A parked key read since gets its second chance once released.
  store_.SetAccessTime(clock_.SteadyNow());
  ASSERT_TRUE(store_.Exec(ops::ReadOp{ops::StringGet{.key = "k150"}}).has_value());
  horizon_ = 200;
  report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.by_deadline, 99U);
  EXPECT_TRUE(Resident("k150"));
  EXPECT_EQ(store_.Stats()->unevictable_bytes, 0U);
}

TEST_F(MaintenanceTest, EachEvictionClassIsWalkedOnItsOwn) {
  policy_ = core::EvictionPolicy{kLongEviction, {{.prefix = "s:", .eviction = kShortEviction}}};
  // Long-lived keys first, at the cold end of one list.
  for (int i = 0; i < 100; ++i) Set(Key("l:", i), core::kFirstSeq);
  for (int i = 0; i < 100; ++i) Set(Key("s:", i), core::kFirstSeq);
  clock_.Advance(2s);
  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow()).by_deadline, 100U);
  EXPECT_EQ(store_.Stats()->key_count, 100U);
  EXPECT_TRUE(Resident("l:0"));
}

// --- Memory ---

TEST(MemoryWalkTest, PassesUndrainedKeysOnceAHorizon) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  SingleShardStore probe{SingleShardConfig{}};
  ASSERT_TRUE(
      probe.Apply(ops::WriteOp{ops::StringSet{.key = "k000", .value = value}}, kLongEviction)
          .has_value());
  const uint64_t per_entry = probe.Stats().used_bytes;
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = per_entry * 1000,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  // Undrained keys at the cold end, drained ones behind them; written
  // while nothing has drained, so the writes evict none.
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{.key = Key("u", 100 + i), .value = value}},
                           kLongEviction, static_cast<core::SequenceId>(1000 + i), 0)
                    .has_value());
  }
  for (int i = 0; i < 900; ++i) {
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{.key = Key("d", 100 + i), .value = value}},
                           kLongEviction, static_cast<core::SequenceId>(1 + i), 0)
                    .has_value());
  }
  const auto walk_to = [&store](size_t target) {
    size_t evicted = 0;
    for (;;) {
      HoldBudget hold = HoldBudget::Capped();
      if (store.EvictLru(target, 900, hold, evicted)) return evicted;
      EXPECT_LE(hold.examined(), HoldBudget::kHoldEntries);
    }
  };

  const uint64_t before = store.LruVisitsForTesting();
  EXPECT_EQ(walk_to(per_entry * 950), 50U);
  EXPECT_EQ(store.LruVisitsForTesting() - before, 150U) << "100 passed, 50 evicted";

  const uint64_t again = store.LruVisitsForTesting();
  EXPECT_EQ(walk_to(per_entry * 900), 50U);
  EXPECT_EQ(store.LruVisitsForTesting() - again, 50U) << "the undrained ones not again";
}

TEST(MemoryWalkTest, AnApplyEvictsAtMostOneHold) {
  const std::string value(64, 'v');
  SingleShardStore probe{SingleShardConfig{}};
  ASSERT_TRUE(
      probe.Apply(ops::WriteOp{ops::StringSet{.key = "k000", .value = value}}, kLongEviction)
          .has_value());
  const uint64_t per_entry = probe.Stats().used_bytes;
  const size_t budget = per_entry * 1000;
  SingleShardStore store{SingleShardConfig{.max_memory_bytes = budget}};
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{.key = Key("k", 100 + i), .value = value}},
                           kLongEviction)
                    .has_value());
  }
  const uint64_t before = store.Stats().eviction_count;
  const std::string large(per_entry * 200, 'x');
  ASSERT_TRUE(
      store.Apply(ops::WriteOp{ops::StringSet{.key = "large", .value = large}}, kLongEviction)
          .has_value());
  const uint64_t evicted = store.Stats().eviction_count - before;
  EXPECT_GT(evicted, 0U);
  EXPECT_LE(evicted, HoldBudget::kHoldEntries);
  EXPECT_GT(store.Stats().used_bytes, budget) << "it lands, over the budget";

  store.EvictLru(budget, kAllDrained);
  EXPECT_LE(store.Stats().used_bytes, budget);
}

TEST(MemoryWalkTest, AFillEvictsAtMostOneHoldElseIsSkipped) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  ShardedHotStore probe{ShardedHotStoreConfig{.max_memory_bytes = 0, .shard_count = 1}};
  ASSERT_TRUE(
      probe.Apply(ops::WriteOp{ops::StringSet{.key = "k000", .value = value}}, core::kFirstSeq)
          .has_value());
  const uint64_t per_entry = probe.Stats()->used_bytes;
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = per_entry * 1000,
      .shard_count = 1,
      .fill_max_fraction = 1.0,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{.key = Key("k", 100 + i), .value = value}},
                           core::kFirstSeq)
                    .has_value());
  }
  ASSERT_LE(store.Stats()->used_bytes, store.Stats()->max_bytes);
  const auto fill = [&store](std::string_view key, size_t bytes) {
    const LoadStart start = store.BeginLoad(key);
    EXPECT_TRUE(start.started());
    return store.Fill(key, start.token, MakeLoadedFull(std::string(bytes, 'x'), 0));
  };

  uint64_t before = store.Stats()->eviction_count;
  EXPECT_EQ(fill("large", per_entry * 200), ShardedHotStore::FillResult::kNoRoom);
  EXPECT_LE(store.Stats()->eviction_count - before, HoldBudget::kHoldEntries);
  EXPECT_FALSE(store.LoadPending("large"));
  EXPECT_EQ(store.Probe("large"), core::HotKeyPresence::kAbsent);

  before = store.Stats()->eviction_count;
  EXPECT_EQ(fill("small", per_entry * 2), ShardedHotStore::FillResult::kInstalled);
  EXPECT_LE(store.Stats()->eviction_count - before, HoldBudget::kHoldEntries);
  EXPECT_EQ(store.Probe("small"), core::HotKeyPresence::kPresent);
}

}  // namespace
}  // namespace abyss::hot
