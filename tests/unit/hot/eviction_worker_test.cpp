#include "abyss/hot/eviction_worker.h"

#include <gtest/gtest.h>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
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
