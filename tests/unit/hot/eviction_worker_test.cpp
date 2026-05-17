#include "abyss/hot/eviction_worker.h"

#include <gtest/gtest.h>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;

class EvictionWorkerTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  core::EvictionPolicy policy_{core::EvictionTTL{1}};
  ShardedHotStore store_{ShardedHotStoreConfig{
      .max_memory_bytes = 8UL * 1024 * 1024,
      .shard_count = 1,
      .eviction_policy = &policy_,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(EvictionWorkerTest, TickOnceDrainsBuffersAndEvicts) {
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}}).has_value());

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
  abyss::metrics::testing::Reset();

  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  // Eviction-only entry: no TTL, eviction policy is 1s.
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::StringSet{
                      .key = "evict_only",
                      .value = "v",
                      .abs_ttl_ms = 0,
                  }})
                  .has_value());
  // TTL entry: short TTL inside the eviction window, so TTL fires first.
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::StringSet{
                      .key = "ttl_key",
                      .value = "v",
                      .abs_ttl_ms = static_cast<uint64_t>(now_ms + 500),
                  }})
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

}  // namespace
}  // namespace abyss::hot
