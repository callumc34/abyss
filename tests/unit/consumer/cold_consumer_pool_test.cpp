#include "abyss/consumer/cold_consumer_pool.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "cold_drain_wait.h"
#include "mock_cold_store.h"
#include "mock_queue.h"
#include "test_clock.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

core::QueueEntry MakeWrite(core::SequenceId seq) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{.args = {"SET", "k", "v"}}},
  };
}

class ColdConsumerPoolWaitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ON_CALL(queue_, Read(_, _, _, _, _)).WillByDefault(Return(std::vector<core::QueueEntry>{}));
    ON_CALL(queue_, CommitOffset(_, _, _)).WillByDefault(Return(core::Result<void>{}));
    ON_CALL(cold_, ApplyBatch(_, _)).WillByDefault(Return(core::Result<void>{}));
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  NiceMock<testing::MockQueue> queue_;
  NiceMock<testing::MockColdStore> cold_;
  testing::TestClock clock_;
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  std::unique_ptr<ColdConsumerPool> MakePool(uint32_t shard_count = 2) {
    return std::make_unique<ColdConsumerPool>(
        queue_, cold_,
        ColdConsumerPool::Config{.shard_count = shard_count,
                                 .consumer = ColdConsumer::Config{.rng_seed = 42}},
        policy_, clock_.SteadyFn());
  }
};

// The pool routes a drain wait to `shard`'s consumer.
TEST_F(ColdConsumerPoolWaitTest, ReadyAtOnceWhenAlreadyDrainedThrough) {
  auto pool = MakePool(/*shard_count=*/1);
  std::vector<core::QueueEntry> first_batch{MakeWrite(5), MakeWrite(7)};
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillOnce(Return(first_batch))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));
  pool->ConsumerFor(0).Drain();
  // latest_drained_seq advanced past 7; any target <= 7 should be instant.
  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(testing::WaitDrained(*pool, 0, 5, 10s).has_value());
  EXPECT_TRUE(testing::WaitDrained(*pool, 0, 7, 10s).has_value());
  EXPECT_LT(std::chrono::steady_clock::now() - start, 5s);
}

TEST_F(ColdConsumerPoolWaitTest, TimesOutWhenConsumerDoesNotAdvance) {
  auto pool = MakePool(/*shard_count=*/1);
  // Read returns empty — consumer never advances past 0.
  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(testing::TimedOut(testing::WaitDrained(*pool, 0, /*through=*/42, 30ms)));
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_GE(elapsed, 30ms);
  EXPECT_LT(elapsed, 5s) << "the wait outlived its timeout";
  EXPECT_EQ(pool->ConsumerFor(0).RoomWaitersForTesting(), 0U);
}

TEST_F(ColdConsumerPoolWaitTest, WakesWhenConsumerAdvancesDuringWait) {
  auto pool = MakePool(/*shard_count=*/1);
  // Pre-register the Read sequence so gmock isn't mutated from another thread
  // while the main thread polls the pool. The advancer just triggers Drain.
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWrite(3)}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));
  std::thread advancer([&] {
    std::this_thread::sleep_for(20ms);
    pool->ConsumerFor(0).Drain();
  });
  EXPECT_TRUE(testing::WaitDrained(*pool, 0, 3, 500ms).has_value());
  advancer.join();
}

TEST_F(ColdConsumerPoolWaitTest, OutOfRangeShardIsInvalid) {
  auto pool = MakePool(/*shard_count=*/2);
  const auto waited = testing::WaitDrained(*pool, /*shard=*/2, /*through=*/1, 5ms);
  ASSERT_FALSE(waited.has_value());
  EXPECT_EQ(waited.error().code(), core::ErrorCode::kInvalidArgument);
}

// --- COLDC-5: oldest_unflushed_age aggregation --------------------------------

class ColdConsumerPoolMetricsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ON_CALL(queue_, Read(_, _, _, _, _)).WillByDefault(Return(std::vector<core::QueueEntry>{}));
    ON_CALL(queue_, CommitOffset(_, _, _)).WillByDefault(Return(core::Result<void>{}));
    ON_CALL(cold_, ApplyBatch(_, _)).WillByDefault(Return(core::Result<void>{}));
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  NiceMock<testing::MockQueue> queue_;
  NiceMock<testing::MockColdStore> cold_;
  testing::TestClock clock_;
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  std::unique_ptr<ColdConsumerPool> MakePool(uint32_t shard_count = 2) {
    return std::make_unique<ColdConsumerPool>(
        queue_, cold_,
        ColdConsumerPool::Config{.shard_count = shard_count,
                                 .consumer = ColdConsumer::Config{.rng_seed = 42}},
        policy_, clock_.SteadyFn());
  }
};

TEST_F(ColdConsumerPoolMetricsTest, OldestUnflushedAgeIsZeroWhenAllBuffersEmpty) {
  auto pool = MakePool();
  EXPECT_EQ(pool->Snapshot().oldest_unflushed_age, 0ms);
}

TEST_F(ColdConsumerPoolMetricsTest, OldestUnflushedAgeAggregatesMaxAcrossShards) {
  auto pool = MakePool(/*shard_count=*/2);
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWrite(1)}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));
  EXPECT_CALL(queue_, Read(1, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWrite(2)}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  pool->ConsumerFor(0).Drain();
  clock_.Advance(8s);
  pool->ConsumerFor(1).Drain();
  clock_.Advance(5s);

  ASSERT_EQ(pool->ConsumerFor(0).Snapshot().oldest_unflushed_age, 13000ms);
  ASSERT_EQ(pool->ConsumerFor(1).Snapshot().oldest_unflushed_age, 5000ms);

  // Max, not the sum (18s) and not the last shard's (5s).
  EXPECT_EQ(pool->Snapshot().oldest_unflushed_age, 13000ms);
}

}  // namespace
}  // namespace abyss::consumer
