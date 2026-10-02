#include "abyss/queue/group_commit.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Extent = GroupCommitter::Extent;

// Stands in for a shard: the published end it snapshots, and a gate that
// holds each flush until the test releases it.
class FakeLog {
 public:
  void Publish(core::SequenceId end) {
    const std::scoped_lock lock(mu_);
    published_ = end;
  }

  // Each flush snapshots the published end, then waits at the gate while
  // the gate is closed. Returns the snapshot.
  core::Result<Extent> Flush() {
    std::unique_lock lock(mu_);
    starts_.push_back(Clock::now());
    const Extent snapshot{.end = published_, .bytes = published_ * 10};
    cv_.notify_all();
    cv_.wait(lock, [this] { return open_ || releases_ > 0; });
    if (releases_ > 0) --releases_;
    ++completed_;
    cv_.notify_all();
    return snapshot;
  }

  void Close() {
    const std::scoped_lock lock(mu_);
    open_ = false;
  }
  void Open() {
    {
      const std::scoped_lock lock(mu_);
      open_ = true;
    }
    cv_.notify_all();
  }
  // Lets exactly one held flush through.
  void ReleaseOne() {
    {
      const std::scoped_lock lock(mu_);
      ++releases_;
    }
    cv_.notify_all();
  }

  bool AwaitStarts(size_t n, std::chrono::milliseconds timeout = 5s) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this, n] { return starts_.size() >= n; });
  }
  bool AwaitCompleted(size_t n, std::chrono::milliseconds timeout = 5s) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this, n] { return completed_ >= n; });
  }

  size_t starts() {
    const std::scoped_lock lock(mu_);
    return starts_.size();
  }
  Clock::time_point start(size_t i) {
    const std::scoped_lock lock(mu_);
    return starts_.at(i);
  }

  GroupCommitter::FlushFn Fn() {
    return [this] { return Flush(); };
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  core::SequenceId published_ = 0;
  bool open_ = true;
  int releases_ = 0;
  size_t completed_ = 0;
  std::vector<Clock::time_point> starts_;
};

bool Ready(DurabilityFuture& f) { return f.wait_for(0ms) == std::future_status::ready; }

class GroupCommitterTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  FakeLog log_;
};

TEST_F(GroupCommitterTest, LoneWriteFlushesWithoutDelay) {
  GroupCommitter committer({}, log_.Fn(), nullptr);
  // An idle committer never flushes on its own: there is no timer.
  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(log_.starts(), 0U);

  Clock::duration best = Clock::duration::max();
  for (core::SequenceId end = 1; end <= 20; ++end) {
    log_.Publish(end);
    const auto published_at = Clock::now();
    committer.Published(end);
    ASSERT_TRUE(log_.AwaitStarts(end));
    best = std::min(best, log_.start(end - 1) - published_at);
    ASSERT_TRUE(committer.AwaitDurable(end - 1, 5s));
  }
  // A 1 ms interval timer would put every start at or past 1 ms.
  EXPECT_LT(best, 500us) << "best start latency "
                         << std::chrono::duration_cast<std::chrono::microseconds>(best).count()
                         << " us";
}

TEST_F(GroupCommitterTest, PublishesDuringASlowFlushFormOneNextFlush) {
  GroupCommitter committer({}, log_.Fn(), nullptr);
  log_.Close();
  log_.Publish(1);
  committer.Published(1);
  ASSERT_TRUE(log_.AwaitStarts(1));

  constexpr core::SequenceId kLast = 50;
  for (core::SequenceId end = 2; end <= kLast; ++end) {
    log_.Publish(end);
    committer.Published(end);
  }
  log_.Open();

  ASSERT_TRUE(committer.AwaitDurable(kLast - 1, 5s));
  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(log_.starts(), 2U);
  EXPECT_EQ(committer.DurableEnd(), kLast);
  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kWalFlushDurationSeconds), 2U);
  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kWalFlushBatchEntries), 2U);
  EXPECT_EQ(metrics::testing::GetHistogramSum(metrics::names::kWalFlushBatchEntries),
            static_cast<double>(kLast));
}

TEST_F(GroupCommitterTest, WaitersResolveInSeqOrderAndNeverEarly) {
  std::vector<std::pair<Extent, Extent>> flushed;
  GroupCommitter committer({}, log_.Fn(), [&flushed](Extent previous, Extent now) {
    flushed.emplace_back(previous, now);
  });
  log_.Close();

  std::vector<DurabilityFuture> futures;
  futures.reserve(3);
  log_.Publish(3);
  for (core::SequenceId seq = 0; seq < 3; ++seq) futures.push_back(committer.WhenDurable(seq));
  committer.Published(3);
  ASSERT_TRUE(log_.AwaitStarts(1));

  // Published while the first flush is held: not covered by it.
  log_.Publish(7);
  for (core::SequenceId seq = 3; seq < 7; ++seq) futures.push_back(committer.WhenDurable(seq));
  committer.Published(7);
  for (auto& f : futures) EXPECT_FALSE(Ready(f));

  log_.ReleaseOne();
  ASSERT_TRUE(log_.AwaitStarts(2));
  for (core::SequenceId seq = 0; seq < 3; ++seq) {
    ASSERT_EQ(futures[seq].wait_for(5s), std::future_status::ready) << seq;
  }
  for (core::SequenceId seq = 3; seq < 7; ++seq) EXPECT_FALSE(Ready(futures[seq])) << seq;
  EXPECT_EQ(committer.DurableEnd(), 3U);

  log_.ReleaseOne();
  for (auto& f : futures) {
    ASSERT_EQ(f.wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(f.get().has_value());
  }
  EXPECT_EQ(committer.DurableEnd(), 7U);
  log_.Open();
  committer.Stop(/*final_flush=*/true);
  ASSERT_EQ(flushed.size(), 2U);
  EXPECT_EQ(flushed[0].first.end, 0U);
  EXPECT_EQ(flushed[0].second.end, 3U);
  EXPECT_EQ(flushed[1].first.end, 3U);
  EXPECT_EQ(flushed[1].second.end, 7U);
  EXPECT_EQ(flushed[1].second.bytes, 70U);
}

TEST_F(GroupCommitterTest, AlreadyDurableSeqResolvesAtOnce) {
  GroupCommitter committer({.end = 5, .bytes = 0}, log_.Fn(), nullptr);
  auto f = committer.WhenDurable(4);
  EXPECT_TRUE(Ready(f));
  EXPECT_TRUE(committer.AwaitDurable(4, 0ms));
  EXPECT_FALSE(committer.AwaitDurable(5, 0ms));
}

TEST_F(GroupCommitterTest, AwaitDurableTimesOut) {
  GroupCommitter committer({}, log_.Fn(), nullptr);
  log_.Close();
  log_.Publish(1);
  committer.Published(1);
  ASSERT_TRUE(log_.AwaitStarts(1));

  const auto start = Clock::now();
  EXPECT_FALSE(committer.AwaitDurable(0, 30ms));
  EXPECT_GE(Clock::now() - start, 30ms);

  log_.Open();
  EXPECT_TRUE(committer.AwaitDurable(0, 5s));
}

TEST_F(GroupCommitterTest, StopResolvesOutstandingWaitersUnavailable) {
  GroupCommitter committer({}, log_.Fn(), nullptr);
  auto pending = committer.WhenDurable(0);
  committer.Stop(/*final_flush=*/false);

  ASSERT_TRUE(Ready(pending));
  auto result = pending.get();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kUnavailable);

  auto late = committer.WhenDurable(0);
  ASSERT_TRUE(Ready(late));
  EXPECT_FALSE(late.get().has_value());
  EXPECT_FALSE(committer.AwaitDurable(0, 1s));
  committer.Stop(/*final_flush=*/false);
}

TEST_F(GroupCommitterTest, StopWakesAwaiters) {
  GroupCommitter committer({}, log_.Fn(), nullptr);
  auto awaiting = std::async(std::launch::async, [&] { return committer.AwaitDurable(0, 10s); });
  std::this_thread::sleep_for(10ms);
  const auto start = Clock::now();
  committer.Stop(/*final_flush=*/false);
  ASSERT_EQ(awaiting.wait_for(5s), std::future_status::ready);
  EXPECT_FALSE(awaiting.get());
  EXPECT_LT(Clock::now() - start, 5s);
}

TEST_F(GroupCommitterTest, FinalFlushCoversTheRemainder) {
  GroupCommitter committer({}, log_.Fn(), nullptr);
  log_.Close();
  log_.Publish(1);
  committer.Published(1);
  ASSERT_TRUE(log_.AwaitStarts(1));

  log_.Publish(4);
  std::vector<DurabilityFuture> futures;
  futures.reserve(4);
  for (core::SequenceId seq = 0; seq < 4; ++seq) futures.push_back(committer.WhenDurable(seq));
  committer.Published(4);

  auto stopped = std::async(std::launch::async, [&] { committer.Stop(/*final_flush=*/true); });
  log_.Open();
  ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(committer.DurableEnd(), 4U);
  EXPECT_EQ(log_.starts(), 2U);
  for (auto& f : futures) {
    ASSERT_TRUE(Ready(f));
    EXPECT_TRUE(f.get().has_value());
  }
}

// The commit thread cannot unwind a throwing fatal capture, so the
// failure is observed in a child process.
TEST(GroupCommitterDeathTest, FlushFailureIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        GroupCommitter committer(
            {},
            [] {
              return core::Result<Extent>(std::unexpected(
                  core::Error{core::ErrorCode::kInternal, "injected device failure"}));
            },
            nullptr);
        committer.Published(1);
        std::this_thread::sleep_for(10s);
      },
      "WAL flush failed: injected device failure");
}

}  // namespace
}  // namespace abyss::queue
