#include "abyss/queue/group_commit.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
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

// Stands in for a log: the filled end each flush snapshots, and a gate
// that holds each flush until the test releases it.
class FakeLog {
 public:
  void Publish(LogPosition end) {
    const std::scoped_lock lock(mu_);
    published_ = end;
  }

  // Each flush snapshots the published end, then waits at the gate while
  // the gate is closed. Returns the snapshot, one entry per position.
  core::Result<Extent> Flush() {
    std::unique_lock lock(mu_);
    starts_.push_back(Clock::now());
    const Extent snapshot{.end = published_, .entries = published_ - flushed_};
    flushed_ = published_;
    cv_.notify_all();
    cv_.wait(lock, [this] { return open_ || releases_ > 0; });
    if (releases_ > 0) --releases_;
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
  LogPosition published_ = 0;
  LogPosition flushed_ = 0;
  bool open_ = true;
  int releases_ = 0;
  std::vector<Clock::time_point> starts_;
};

bool AwaitDurableEnd(const GroupCommitter& committer, LogPosition end,
                     std::chrono::milliseconds timeout = 5s) {
  const auto deadline = Clock::now() + timeout;
  while (committer.DurableEnd() < end) {
    if (Clock::now() >= deadline) return false;
    std::this_thread::sleep_for(100us);
  }
  return true;
}

class GroupCommitterTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  FakeLog log_;
};

TEST_F(GroupCommitterTest, LoneWriteFlushesWithoutDelay) {
  GroupCommitter committer(0, log_.Fn(), nullptr);
  // An idle committer never flushes on its own: there is no timer.
  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(log_.starts(), 0U);

  Clock::duration best = Clock::duration::max();
  for (LogPosition end = 1; end <= 20; ++end) {
    log_.Publish(end);
    const auto published_at = Clock::now();
    committer.Published(end);
    ASSERT_TRUE(log_.AwaitStarts(end));
    best = std::min(best, log_.start(end - 1) - published_at);
    ASSERT_TRUE(AwaitDurableEnd(committer, end));
  }
  // A 1 ms interval timer would put every start at or past 1 ms.
  EXPECT_LT(best, 500us) << "best start latency "
                         << std::chrono::duration_cast<std::chrono::microseconds>(best).count()
                         << " us";
}

TEST_F(GroupCommitterTest, PublishesDuringASlowFlushFormOneNextFlush) {
  GroupCommitter committer(0, log_.Fn(), nullptr);
  log_.Close();
  log_.Publish(1);
  committer.Published(1);
  ASSERT_TRUE(log_.AwaitStarts(1));

  constexpr LogPosition kLast = 50;
  for (LogPosition end = 2; end <= kLast; ++end) {
    log_.Publish(end);
    committer.Published(end);
  }
  log_.Open();

  ASSERT_TRUE(AwaitDurableEnd(committer, kLast));
  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(log_.starts(), 2U);
  EXPECT_EQ(committer.DurableEnd(), kLast);
  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kWalFlushDurationSeconds), 2U);
  EXPECT_EQ(metrics::testing::GetHistogramCount(metrics::names::kWalFlushBatchEntries), 2U);
  EXPECT_EQ(metrics::testing::GetHistogramSum(metrics::names::kWalFlushBatchEntries),
            static_cast<double>(kLast));
}

TEST_F(GroupCommitterTest, FlushedRunsBeforeTheEndIsVisibleAndNeverEarly) {
  struct Seen {
    LogPosition previous;
    LogPosition end;
    LogPosition visible;
  };
  std::vector<Seen> seen;
  GroupCommitter* self = nullptr;
  GroupCommitter committer(0, log_.Fn(), [&](LogPosition previous, Extent flushed) {
    seen.push_back({previous, flushed.end, self->DurableEnd()});
  });
  self = &committer;
  log_.Close();

  log_.Publish(3);
  committer.Published(3);
  ASSERT_TRUE(log_.AwaitStarts(1));
  // Published while the first flush is held: not covered by it.
  log_.Publish(7);
  committer.Published(7);
  EXPECT_EQ(committer.DurableEnd(), 0U);

  log_.ReleaseOne();
  ASSERT_TRUE(log_.AwaitStarts(2));
  ASSERT_TRUE(AwaitDurableEnd(committer, 3));
  EXPECT_EQ(committer.DurableEnd(), 3U);

  log_.ReleaseOne();
  ASSERT_TRUE(AwaitDurableEnd(committer, 7));
  log_.Open();
  committer.Stop(/*final_flush=*/true);
  ASSERT_EQ(seen.size(), 2U);
  EXPECT_EQ(seen[0].previous, 0U);
  EXPECT_EQ(seen[0].end, 3U);
  EXPECT_EQ(seen[0].visible, 0U);
  EXPECT_EQ(seen[1].previous, 3U);
  EXPECT_EQ(seen[1].end, 7U);
  EXPECT_EQ(seen[1].visible, 3U);
}

TEST_F(GroupCommitterTest, StartsAtTheRecoveredEnd) {
  GroupCommitter committer(5, log_.Fn(), nullptr);
  EXPECT_EQ(committer.DurableEnd(), 5U);
  // Nothing past the recovered end is published, so nothing flushes.
  committer.Published(5);
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(log_.starts(), 0U);
}

TEST_F(GroupCommitterTest, FinalFlushCoversTheRemainder) {
  GroupCommitter committer(0, log_.Fn(), nullptr);
  log_.Close();
  log_.Publish(1);
  committer.Published(1);
  ASSERT_TRUE(log_.AwaitStarts(1));

  log_.Publish(4);
  committer.Published(4);

  auto stopped = std::async(std::launch::async, [&] { committer.Stop(/*final_flush=*/true); });
  log_.Open();
  ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(committer.DurableEnd(), 4U);
  EXPECT_EQ(log_.starts(), 2U);
}

// The commit thread cannot unwind a throwing fatal capture, so the
// failure is observed in a child process.
TEST(GroupCommitterDeathTest, FlushFailureIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        GroupCommitter committer(
            0,
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
