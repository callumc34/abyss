#include "abyss/queue/group_commit.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/queue/fsync_policy.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

GroupCommitter::FsyncFn MakeCounter(std::atomic<int>& counter) {
  return [&counter] {
    counter.fetch_add(1, std::memory_order_relaxed);
    return core::Result<void>{};
  };
}

TEST(GroupCommitterTest, NonePolicyNeverFsyncs) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kNone}, MakeCounter(fsync_count));

  auto future = committer.Submit(100, 1);
  EXPECT_EQ(future.wait_for(0ms), std::future_status::ready);
  EXPECT_TRUE(future.get().has_value());
  EXPECT_EQ(fsync_count.load(), 0);
}

TEST(GroupCommitterTest, PerWritePolicyFsyncsEveryCall) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kPerWrite}, MakeCounter(fsync_count));

  for (int i = 0; i < 5; ++i) {
    auto future = committer.Submit(100, 1);
    EXPECT_TRUE(future.get().has_value());
  }

  EXPECT_EQ(fsync_count.load(), 5);
}

TEST(GroupCommitterTest, GroupCommitCoalescesConcurrentSubmits) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 50ms, .max_bytes = 1 << 30},
      MakeCounter(fsync_count));

  constexpr int kWriters = 16;
  std::vector<DurabilityFuture> futures;
  futures.reserve(kWriters);

  for (int i = 0; i < kWriters; ++i) {
    futures.push_back(committer.Submit(100, 1));
  }

  for (auto& f : futures) {
    EXPECT_TRUE(f.get().has_value());
  }

  EXPECT_EQ(fsync_count.load(), 1);
}

TEST(GroupCommitterTest, GroupCommitTripsOnByteThreshold) {
  // Verify the byte threshold trips a flush well before the configured
  // interval. Two submits totalling exactly max_bytes: the second takes
  // pending_bytes to the threshold, which wakes the commit thread out of
  // its wait_for and batches both into a single fsync. A trailing submit
  // below the threshold would start a fresh batch and wait the full
  // interval — that is correct behaviour, but unrelated to what this
  // test asserts.
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 10s, .max_bytes = 1024},
      MakeCounter(fsync_count));

  auto start = std::chrono::steady_clock::now();
  auto f1 = committer.Submit(512, 1);
  auto f2 = committer.Submit(512, 1);

  EXPECT_TRUE(f1.get().has_value());
  EXPECT_TRUE(f2.get().has_value());

  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, 1s);
  EXPECT_EQ(fsync_count.load(), 1);
}

TEST(GroupCommitterTest, GroupCommitFlushesAfterInterval) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 30ms, .max_bytes = 1 << 30},
      MakeCounter(fsync_count));

  auto future = committer.Submit(100, 1);
  auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(future.get().has_value());
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_GE(elapsed, 20ms);
  EXPECT_EQ(fsync_count.load(), 1);
}

TEST(GroupCommitterTest, DrainFlushesPending) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 10s, .max_bytes = 1 << 30},
      MakeCounter(fsync_count));

  auto f = committer.Submit(100, 1);
  auto drain = committer.Drain();

  EXPECT_TRUE(drain.has_value());
  EXPECT_TRUE(f.get().has_value());
  EXPECT_GE(fsync_count.load(), 1);
}

TEST(GroupCommitterTest, DrainNoPendingReturnsOk) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kGroupCommit}, MakeCounter(fsync_count));
  EXPECT_TRUE(committer.Drain().has_value());
}

TEST(GroupCommitterTest, StopResolvesPending) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 10s, .max_bytes = 1 << 30},
      MakeCounter(fsync_count));

  auto f = committer.Submit(100, 1);
  committer.Stop();

  EXPECT_TRUE(f.get().has_value());
}

TEST(GroupCommitterTest, SubmitAfterStopResolvesWithUnavailable) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kGroupCommit}, MakeCounter(fsync_count));
  committer.Stop();

  auto f = committer.Submit(100, 1);
  auto result = f.get();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kUnavailable);
}

TEST(GroupCommitterTest, FsyncFailurePropagates) {
  GroupCommitter committer({.policy = FsyncPolicy::kGroupCommit, .interval = 10ms}, [] {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "simulated failure"});
  });

  auto f1 = committer.Submit(100, 1);
  auto f2 = committer.Submit(100, 1);

  auto r1 = f1.get();
  auto r2 = f2.get();
  ASSERT_FALSE(r1.has_value());
  ASSERT_FALSE(r2.has_value());
  EXPECT_EQ(r1.error().code(), core::ErrorCode::kInternal);
  EXPECT_EQ(r2.error().code(), core::ErrorCode::kInternal);
}

TEST(GroupCommitterTest, PerWriteFsyncFailurePropagates) {
  GroupCommitter committer({.policy = FsyncPolicy::kPerWrite}, [] {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "boom"});
  });

  auto f = committer.Submit(100, 1);
  auto result = f.get();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
}

TEST(GroupCommitterTest, HighConcurrencyCoalescesIntoMuchFewerFsyncs) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 5ms, .max_bytes = 1 << 20},
      MakeCounter(fsync_count));

  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 100;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&committer] {
      for (int i = 0; i < kOpsPerThread; ++i) {
        auto f = committer.Submit(64, 1);
        EXPECT_TRUE(f.get().has_value());
      }
    });
  }

  for (auto& th : threads) th.join();

  EXPECT_LT(fsync_count.load(), kThreads * kOpsPerThread);
}

TEST(GroupCommitterTest, DrainNonePolicyIsNoOp) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kNone}, MakeCounter(fsync_count));
  EXPECT_TRUE(committer.Drain().has_value());
  EXPECT_EQ(fsync_count.load(), 0);
}

// A1: durable_seq starts at 0 (nothing durable) and advances ONLY after the
// group fsync completes. Before the fsync lands the watermark must not move
// past the submitted seq — the exact ack-before-fsync hazard QUEUE-2 closes.
TEST(GroupCommitterTest, DurableSeqAdvancesOnlyAfterFsync) {
  std::atomic<int> fsync_count{0};
  // A blocking fsync we release manually so we can observe the pre-fsync state.
  std::atomic<bool> release{false};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 1ms, .max_bytes = 1 << 30},
      [&fsync_count, &release]() -> core::Result<void> {
        while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
        fsync_count.fetch_add(1, std::memory_order_relaxed);
        return {};
      });

  EXPECT_EQ(committer.DurableSeq(), 0U);
  auto f = committer.Submit(100, 42);
  // The fsync is blocked, so the watermark must stay at 0.
  EXPECT_FALSE(committer.AwaitDurable(42, 20ms));
  EXPECT_EQ(committer.DurableSeq(), 0U);

  release.store(true, std::memory_order_release);
  EXPECT_TRUE(f.get().has_value());
  EXPECT_TRUE(committer.AwaitDurable(42, 1s));
  EXPECT_EQ(committer.DurableSeq(), 42U);
}

// A1: durable_seq is monotonic and tracks the highest flushed batch_last_seq.
TEST(GroupCommitterTest, DurableSeqMonotonicToHighestBatchSeq) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer(
      {.policy = FsyncPolicy::kGroupCommit, .interval = 5ms, .max_bytes = 1 << 30},
      MakeCounter(fsync_count));

  auto f1 = committer.Submit(100, 10);
  auto f2 = committer.Submit(100, 25);
  ASSERT_TRUE(f1.get().has_value());
  ASSERT_TRUE(f2.get().has_value());
  EXPECT_EQ(committer.DurableSeq(), 25U);

  // A later, smaller seq must never regress the watermark.
  auto f3 = committer.Submit(100, 5);
  ASSERT_TRUE(f3.get().has_value());
  EXPECT_GE(committer.DurableSeq(), 25U);
}

// Decision 1: under fsync_none durable_seq tracks the published seq so the
// retention-Ack gate is a correct no-op (durability is disabled, not blocked).
TEST(GroupCommitterTest, NonePolicyTracksPublishedSeq) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kNone}, MakeCounter(fsync_count));

  auto f = committer.Submit(100, 7);
  EXPECT_TRUE(f.get().has_value());
  EXPECT_EQ(committer.DurableSeq(), 7U);
  EXPECT_TRUE(committer.AwaitDurable(7, 0ms));
  EXPECT_EQ(fsync_count.load(), 0);
}

// kPerWrite advances the watermark synchronously once the fsync returns ok.
TEST(GroupCommitterTest, PerWritePolicyAdvancesDurableSeqSynchronously) {
  std::atomic<int> fsync_count{0};
  GroupCommitter committer({.policy = FsyncPolicy::kPerWrite}, MakeCounter(fsync_count));

  auto f = committer.Submit(100, 3);
  EXPECT_TRUE(f.get().has_value());
  EXPECT_EQ(committer.DurableSeq(), 3U);
}

// A failed fsync must NOT advance the watermark — a write whose fsync errored
// is not durable and must not be ackable.
TEST(GroupCommitterTest, FailedFsyncDoesNotAdvanceDurableSeq) {
  GroupCommitter committer({.policy = FsyncPolicy::kGroupCommit, .interval = 1ms}, []() {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "boom"});
  });

  auto f = committer.Submit(100, 9);
  ASSERT_FALSE(f.get().has_value());
  EXPECT_EQ(committer.DurableSeq(), 0U);
  EXPECT_FALSE(committer.AwaitDurable(9, 10ms));
}

}  // namespace
}  // namespace abyss::queue
