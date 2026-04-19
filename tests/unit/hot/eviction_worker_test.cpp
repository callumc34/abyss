#include "abyss/hot/eviction_worker.h"

#include <gtest/gtest.h>

#include <thread>

#include "abyss/core/ops.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;

class EvictionWorkerTest : public ::testing::Test {
 protected:
  ShardedHotStoreConfig MakeStoreCfg() {
    return ShardedHotStoreConfig{.max_memory_bytes = 8UL * 1024 * 1024, .shard_count = 1};
  }
};

TEST_F(EvictionWorkerTest, TickOnceDrainsBuffersAndEvicts) {
  ShardedHotStore store(MakeStoreCfg());
  // Write a key with a 1s eviction, then sleep past it so EvictExpired has work.
  const core::EvictionTTL short_eviction{1};
  ASSERT_TRUE(
      store
          .Apply(core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}}, short_eviction)
          .has_value());

  EvictionWorker worker(store,
                        EvictionWorker::Config{.tick = 50ms, .default_eviction = short_eviction});

  // Simulate time passing for the eviction policy.
  std::this_thread::sleep_for(1100ms);
  worker.TickOnce();

  auto read = store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}});
  EXPECT_FALSE(read.has_value()) << "expired key should be evicted";
}

TEST_F(EvictionWorkerTest, StopJoinsCleanly) {
  ShardedHotStore store(MakeStoreCfg());
  EvictionWorker worker(store, EvictionWorker::Config{.tick = 20ms});

  worker.Start();
  EXPECT_TRUE(worker.Running());
  // Give the loop a chance to run once.
  std::this_thread::sleep_for(50ms);
  worker.Stop();
  EXPECT_FALSE(worker.Running());
  // Stop is idempotent.
  worker.Stop();
  EXPECT_FALSE(worker.Running());
}

TEST_F(EvictionWorkerTest, DoubleStartIsNoop) {
  ShardedHotStore store(MakeStoreCfg());
  EvictionWorker worker(store, EvictionWorker::Config{.tick = 20ms});
  worker.Start();
  worker.Start();
  EXPECT_TRUE(worker.Running());
  worker.Stop();
}

}  // namespace
}  // namespace abyss::hot
