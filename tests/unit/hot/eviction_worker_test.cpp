#include "abyss/hot/eviction_worker.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;

class EvictionWorkerTest : public ::testing::Test {
 protected:
  static bool ResetMetrics() {
    abyss::metrics::testing::Reset();
    return true;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  // First: a reset frees the metrics the store registers.
  bool metrics_reset_ = ResetMetrics();
  abyss::testing::TestClock clock_;
  core::EvictionPolicy policy_{core::EvictionTTL{1}};
  core::SequenceId horizon_ = kAllDrained;
  ShardedHotStore store_{ShardedHotStoreConfig{
      .max_memory_bytes = 8UL * 1024 * 1024,
      .shard_count = 1,
      .drained = [this](core::ShardId) { return horizon_; },
      .eviction_policy = &policy_,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(EvictionWorkerTest, TickOnceEvicts) {
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}},
                         /*seq=*/core::kFirstSeq)
                  .has_value());

  EvictionWorker worker(store_, EvictionWorker::Config{.tick = 50ms}, clock_.SteadyFn());

  clock_.Advance(1100ms);
  worker.TickOnce();

  auto read = store_.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}});
  EXPECT_FALSE(read.has_value()) << "expired key should be evicted";
}

TEST_F(EvictionWorkerTest, TickAttributesEvictionVsTtl) {
  // Two keys: one driven by eviction deadline, one by absolute TTL. After a
  // single tick the counters must bump independently — kEvictedTotal for the
  // deadline path, kTtlExpiredTotal{tier=hot} for the TTL path. Without this
  // split the operator can't distinguish "moved tier" from "deleted entirely".
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  // Eviction-only entry: no TTL, eviction policy is 1s.
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::StringSet{
                             .key = "evict_only",
                             .value = "v",
                             .abs_ttl_ms = 0,
                         }},
                         /*seq=*/core::kFirstSeq)
                  .has_value());
  // TTL entry: short TTL inside the eviction window, so TTL fires first.
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::StringSet{
                             .key = "ttl_key",
                             .value = "v",
                             .abs_ttl_ms = static_cast<uint64_t>(now_ms + 500),
                         }},
                         /*seq=*/core::kFirstSeq)
                  .has_value());

  EvictionWorker worker(store_, EvictionWorker::Config{.tick = 50ms}, clock_.SteadyFn());

  // Eviction tick advances past both deadlines: wall +1100ms covers the 500ms
  // TTL, steady +1100ms covers the 1s eviction. One tick must produce one
  // increment in each counter, not two in either.
  clock_.Advance(1100ms);
  worker.TickOnce();

  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kEvictedTotal).value_or(0.0), 1.0)
      << "deadline-driven removal should bump kEvictedTotal once";
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kTtlExpiredTotal, metrics::Tier::kHot)
                .value_or(0.0),
            1.0)
      << "TTL-driven removal should bump kTtlExpiredTotal{tier=hot} once";
}

TEST_F(EvictionWorkerTest, StopJoinsCleanly) {
  EvictionWorker worker(store_, EvictionWorker::Config{.tick = 20ms}, clock_.SteadyFn());
  worker.Start();
  EXPECT_TRUE(worker.Running());
  worker.Stop();
  EXPECT_FALSE(worker.Running());
  worker.Stop();
  EXPECT_FALSE(worker.Running());
}

TEST_F(EvictionWorkerTest, DoubleStartIsNoop) {
  EvictionWorker worker(store_, EvictionWorker::Config{.tick = 20ms}, clock_.SteadyFn());
  worker.Start();
  worker.Start();
  EXPECT_TRUE(worker.Running());
  worker.Stop();
}

TEST(EvictionWorkerMemoryTest, TickEnforcesMemoryBudgetAndPublishesGauges) {
  abyss::metrics::testing::Reset();
  abyss::testing::TestClock clock;
  // Long eviction TTL so the deadline pass never fires; isolate the
  // memory-pressure pass. Seed a few entries, then size the budget below them.
  core::EvictionPolicy policy{core::EvictionTTL{86400}};

  // Measure one entry's footprint so the budget can be sized to force eviction.
  const std::string value(64, 'v');
  uint64_t per_entry = 0;
  {
    ShardedHotStore probe{ShardedHotStoreConfig{.max_memory_bytes = 0,
                                                .shard_count = 1,
                                                .eviction_policy = &policy,
                                                .steady_clock = clock.SteadyFn(),
                                                .wall_clock = clock.WallFn()}};
    ASSERT_TRUE(probe
                    .Apply(core::ops::WriteOp{core::ops::StringSet{.key = "p", .value = value}},
                           core::kFirstSeq)
                    .has_value());
    per_entry = probe.Stats()->used_bytes;
  }

  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = (per_entry * 2) + (per_entry / 2),  // holds 2, not 4
      .shard_count = 1,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  for (int i = 0; i < 4; ++i) {
    // SetReplayMode so the apply-time enforcement does not pre-trim; the tick
    // is what we are exercising here.
    store.SetReplayMode(true);
    ASSERT_TRUE(store
                    .Apply(core::ops::WriteOp{core::ops::StringSet{.key = "k" + std::to_string(i),
                                                                   .value = value}},
                           core::kFirstSeq)
                    .has_value());
    store.SetReplayMode(false);
    clock.Advance(1ms);
  }
  ASSERT_GT(store.Stats()->used_bytes, store.Stats()->max_bytes);

  EvictionWorker worker(store, EvictionWorker::Config{.tick = 50ms}, clock.SteadyFn());
  worker.TickOnce();

  EXPECT_LE(store.Stats()->used_bytes, store.Stats()->max_bytes)
      << "tick must evict down to the budget";
  EXPECT_GT(metrics::testing::GetCounterValue(metrics::names::kHotMemoryEvictedTotal).value_or(0.0),
            0.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotMemoryBytes).value_or(-1.0),
            static_cast<double>(store.Stats()->used_bytes));
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotKeys).value_or(-1.0),
            static_cast<double>(store.Stats()->key_count));
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotMaxMemoryBytes).value_or(-1.0),
            static_cast<double>(store.Stats()->max_bytes));
}

TEST_F(EvictionWorkerTest, TickReclaimsTombstonesAtOrBelowHorizon) {
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}},
                         /*seq=*/core::kFirstSeq)
                  .has_value());
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, /*seq=*/5).has_value());
  ASSERT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);

  horizon_ = 4;
  EvictionWorker worker(store_, EvictionWorker::Config{.tick = 50ms}, clock_.SteadyFn());

  // Horizon below the delete seq: cold has not caught up, tombstone is kept.
  worker.TickOnce();
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(
      metrics::testing::GetCounterValue(metrics::names::kHotTombstonesReclaimedTotal).value_or(0.0),
      0.0);

  // Horizon reaches the delete seq: cold has absorbed it, tombstone reclaimed.
  horizon_ = 5;
  worker.TickOnce();
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);
  EXPECT_EQ(
      metrics::testing::GetCounterValue(metrics::names::kHotTombstonesReclaimedTotal).value_or(0.0),
      1.0);
}

TEST(EvictionWorkerResidencyTest, TickPublishesStubAndLoadMetrics) {
  abyss::metrics::testing::Reset();
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{core::EvictionTTL{1}};
  core::SequenceId horizon = 0;
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = kStubBytes * 100,
      .shard_count = 1,
      // Room for one stub.
      .stub_memory_fraction = 0.01,
      .drained = [&horizon](core::ShardId) { return horizon; },
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto set = [&](std::string_view key, core::SequenceId seq) {
    ASSERT_TRUE(store.Apply(core::ops::WriteOp{core::ops::StringSet{.key = key, .value = "v"}}, seq)
                    .has_value());
  };
  set("a", 1);
  set("b", 2);
  const LoadToken token = store.BeginLoad("c").token_if_started().value_or(LoadToken{});
  ASSERT_NE(token.id, 0U) << "no load token";
  set("c", 3);
  ASSERT_FALSE(store.CompleteLoad("c", token, LoadedAbsent{}));

  clock.Advance(2s);
  horizon = 2;
  EvictionWorker worker(store, EvictionWorker::Config{.tick = 50ms}, clock.SteadyFn());
  worker.TickOnce();
  worker.TickOnce();

  const auto stats = store.Stats();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->stub_entries, 1U);
  EXPECT_GT(stats->unevictable_bytes, 0U) << "c is not drained";
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotStubEntries).value_or(-1.0), 1.0);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kHotUnevictableBytes).value_or(-1.0),
            static_cast<double>(stats->unevictable_bytes));
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kHotStubDropsTotal).value_or(0.0),
            1.0)
      << "counted once across two ticks";
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kHotLoadDiscardsTotal).value_or(0.0),
            1.0);
}

}  // namespace
}  // namespace abyss::hot
