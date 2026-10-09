#include "abyss/core/apply_notifier.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "abyss/core/types.h"

namespace abyss::core {
namespace {

using namespace std::chrono_literals;

// ENGINE-3 / HOTC-8 / XCONC-4: a fired seq is remembered forever as a per-shard
// high-water, not for a bounded recent window. A late AwaitApplied whose seq is
// far behind the high-water (well past the old 64-entry ring) resolves
// immediately instead of registering a never-fulfilled promise.
TEST(AppliedSeqNotifierTest, LateAwaitAfterManyNotifiesStillReady) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 1});
  for (SequenceId s = 1; s <= 200; ++s) {
    notifier.NotifyApplied(0, s);
  }
  auto fut = notifier.AwaitApplied(0, 5);
  EXPECT_EQ(fut.wait_for(0ms), std::future_status::ready);
  EXPECT_EQ(notifier.PendingCount(), 0U);
}

// ENGINE-6: nothing applied reads as 0, which names no entry, so an
// await of 0 is ready at once and one of the first seq waits for its
// notify. No zero-seed false-ready.
TEST(AppliedSeqNotifierTest, TheFirstSeqIsNotReadyUntilNotified) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 1});
  EXPECT_EQ(notifier.AppliedSeq(0), 0U);
  EXPECT_EQ(notifier.AwaitApplied(0, 0).wait_for(0ms), std::future_status::ready);

  auto fut = notifier.AwaitApplied(0, kFirstSeq);
  EXPECT_EQ(fut.wait_for(0ms), std::future_status::timeout);
  EXPECT_EQ(notifier.PendingCount(), 1U);

  notifier.NotifyApplied(0, kFirstSeq);
  EXPECT_EQ(fut.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(notifier.AppliedSeq(0), kFirstSeq);

  // A later await at the first seq is now immediately ready.
  auto again = notifier.AwaitApplied(0, kFirstSeq);
  EXPECT_EQ(again.wait_for(0ms), std::future_status::ready);
}

// A waiter registered before its notify is fulfilled when the high-water reaches
// or passes its seq.
TEST(AppliedSeqNotifierTest, EarlyAwaitFulfilledByLaterNotify) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 1});
  auto fut = notifier.AwaitApplied(0, 10);
  EXPECT_EQ(fut.wait_for(0ms), std::future_status::timeout);

  notifier.NotifyApplied(0, 7);  // below target: still waiting
  EXPECT_EQ(fut.wait_for(0ms), std::future_status::timeout);

  notifier.NotifyApplied(0, 12);  // at/above target: drains the waiter
  EXPECT_EQ(fut.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(notifier.PendingCount(), 0U);
}

// The high-water is monotonic: an out-of-order lower notify never regresses it.
TEST(AppliedSeqNotifierTest, NotifyIsMonotonicCasMax) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 1});
  notifier.NotifyApplied(0, 50);
  notifier.NotifyApplied(0, 10);
  EXPECT_EQ(notifier.AppliedSeq(0), 50U);

  auto fut = notifier.AwaitApplied(0, 40);
  EXPECT_EQ(fut.wait_for(0ms), std::future_status::ready);
}

// Per-shard isolation: a notify on one shard does not satisfy a waiter on
// another that maps to a distinct stripe.
TEST(AppliedSeqNotifierTest, DistinctShardsAreIndependent) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 4});
  auto fut = notifier.AwaitApplied(1, 5);
  notifier.NotifyApplied(2, 100);
  EXPECT_EQ(fut.wait_for(0ms), std::future_status::timeout);
  notifier.NotifyApplied(1, 5);
  EXPECT_EQ(fut.wait_for(1s), std::future_status::ready);
}

TEST(AppliedSeqNotifierTest, CancelRemovesWaiterAndBreaksFuture) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 1});
  auto fut = notifier.AwaitApplied(0, 99);
  EXPECT_EQ(notifier.PendingCount(), 1U);
  EXPECT_TRUE(notifier.Cancel(0, 99));
  EXPECT_EQ(notifier.PendingCount(), 0U);
  EXPECT_THROW((void)fut.get(), std::future_error);
  EXPECT_FALSE(notifier.Cancel(0, 99));
}

// Multiple waiters at distinct seqs all drain when the high-water passes them.
TEST(AppliedSeqNotifierTest, MultipleWaitersDrainOnAdvance) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 1});
  auto a = notifier.AwaitApplied(0, 1);
  auto b = notifier.AwaitApplied(0, 2);
  auto c = notifier.AwaitApplied(0, 3);
  auto d = notifier.AwaitApplied(0, 10);

  notifier.NotifyApplied(0, 3);
  EXPECT_EQ(a.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(b.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(c.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(d.wait_for(0ms), std::future_status::timeout);  // 10 > 3
  EXPECT_EQ(notifier.PendingCount(), 1U);

  notifier.NotifyApplied(0, 10);
  EXPECT_EQ(d.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(notifier.PendingCount(), 0U);
}

// ENGINE-3 stress: producers advance the high-water monotonically while
// consumers await random seqs. Every awaited future whose seq <= the final
// high-water must become ready (no lost wakeup, no never-fulfilled promise).
// Run under TSAN in CI per repo policy; locally exercises the drain path.
TEST(AppliedSeqNotifierStress, ConcurrentNotifyAwaitNoLostWakeup) {
  AppliedSeqNotifier notifier(AppliedSeqNotifierConfig{.shard_count = 8});
  constexpr SequenceId kMax = 5000;
  constexpr int kAwaiters = 6;

  std::vector<std::thread> threads;
  threads.reserve(8 + kAwaiters);

  // One producer per shard advancing its own high-water.
  for (ShardId shard = 0; shard < 8; ++shard) {
    threads.emplace_back([&notifier, shard]() {
      for (SequenceId s = 1; s <= kMax; ++s) notifier.NotifyApplied(shard, s);
    });
  }

  std::atomic<int> ready_count{0};
  std::atomic<int> issued{0};
  for (int t = 0; t < kAwaiters; ++t) {
    threads.emplace_back([&]() {
      for (SequenceId s = 1; s <= kMax; s += 7) {
        const auto shard = static_cast<ShardId>(s % 8);
        auto fut = notifier.AwaitApplied(shard, s);
        issued.fetch_add(1, std::memory_order_relaxed);
        if (fut.wait_for(5s) == std::future_status::ready) {
          ready_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& th : threads) th.join();

  // Every issued await targeted a seq <= kMax, so after all producers finished
  // every future must have resolved.
  EXPECT_EQ(ready_count.load(), issued.load());
  EXPECT_EQ(notifier.PendingCount(), 0U);
}

}  // namespace
}  // namespace abyss::core
