#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "abyss/engine/bounded_thread_shard_scheduler.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

TEST(BoundedThreadShardSchedulerTest, RunsSubmittedWork) {
  BoundedThreadShardScheduler sch(4);
  std::atomic<int> count{0};
  for (int i = 0; i < 16; ++i) {
    sch.Submit(static_cast<core::ShardId>(i % 4), [&] { count.fetch_add(1); });
  }
  sch.WaitAll();
  EXPECT_EQ(count.load(), 16);
}

TEST(BoundedThreadShardSchedulerTest, WaitAllReturnsOnlyAfterAllTasksComplete) {
  BoundedThreadShardScheduler sch(2);
  std::atomic<int> finished{0};
  std::atomic<bool> release{false};
  for (int i = 0; i < 8; ++i) {
    sch.Submit(0, [&] {
      while (!release.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms);
      finished.fetch_add(1, std::memory_order_acq_rel);
    });
  }

  // Briefly let workers pick up tasks and start blocking.
  std::this_thread::sleep_for(20ms);
  EXPECT_LT(finished.load(), 8);

  release.store(true, std::memory_order_release);
  sch.WaitAll();
  EXPECT_EQ(finished.load(), 8);
}

TEST(BoundedThreadShardSchedulerTest, RespectsParallelismCap) {
  // 4 workers, 16 tasks — at any point in_flight (started but not finished)
  // should be ≤ 4.
  BoundedThreadShardScheduler sch(4);
  std::atomic<int> active{0};
  std::atomic<int> max_active{0};
  std::atomic<int> done{0};
  for (int i = 0; i < 16; ++i) {
    sch.Submit(0, [&] {
      const int now = active.fetch_add(1, std::memory_order_acq_rel) + 1;
      int prev_max = max_active.load(std::memory_order_relaxed);
      while (now > prev_max &&
             !max_active.compare_exchange_weak(prev_max, now, std::memory_order_acq_rel)) {
      }
      std::this_thread::sleep_for(5ms);
      active.fetch_sub(1, std::memory_order_acq_rel);
      done.fetch_add(1, std::memory_order_acq_rel);
    });
  }
  sch.WaitAll();
  EXPECT_EQ(done.load(), 16);
  EXPECT_LE(max_active.load(), 4);
  EXPECT_GE(max_active.load(), 1);
}

TEST(BoundedThreadShardSchedulerTest, MultipleWaitAllRoundsDontDeadlock) {
  BoundedThreadShardScheduler sch(2);
  std::atomic<int> total{0};

  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 5; ++i) {
      sch.Submit(0, [&] { total.fetch_add(1, std::memory_order_relaxed); });
    }
    sch.WaitAll();
  }
  EXPECT_EQ(total.load(), 15);
}

TEST(BoundedThreadShardSchedulerTest, SingleWorkerFloorIsEnforced) {
  // worker_count=0 must clamp to >=1 internally; otherwise submitted work
  // would wedge with no worker to drain it.
  BoundedThreadShardScheduler sch(0);
  EXPECT_GE(sch.worker_count(), 1U);
  std::atomic<int> hit{0};
  sch.Submit(0, [&] { hit.fetch_add(1); });
  sch.WaitAll();
  EXPECT_EQ(hit.load(), 1);
}

TEST(BoundedThreadShardSchedulerTest, DestructorDrainsPendingWorkSafely) {
  // Submit work, then let the destructor run. Workers should exit cleanly
  // without re-entering an already-shutting-down scheduler. The atomic
  // captures by reference need to survive past the scheduler.
  std::atomic<int> hit{0};
  {
    BoundedThreadShardScheduler sch(2);
    for (int i = 0; i < 4; ++i) {
      sch.Submit(0, [&] { hit.fetch_add(1, std::memory_order_relaxed); });
    }
    sch.WaitAll();
  }
  EXPECT_EQ(hit.load(), 4);
}

}  // namespace
}  // namespace abyss::engine
