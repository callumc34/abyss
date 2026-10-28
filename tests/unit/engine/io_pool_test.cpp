#include "abyss/engine/io_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "latch.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using metrics::IoKind;
using metrics::IoQueue;

// What became of one job; outlives a background job, which the pool
// frees.
struct Outcome {
  std::mutex mu;
  std::condition_variable cv;
  bool ran = false;
  std::optional<core::Error> failed;
  int started_at = -1;

  bool Wait(std::chrono::milliseconds timeout = 10s) {
    std::unique_lock lock(mu);
    return cv.wait_for(lock, timeout, [this] { return ran || failed.has_value(); });
  }
  bool Done() {
    const std::scoped_lock lock(mu);
    return ran || failed.has_value();
  }
};

class Job final : public IoJob {
 public:
  explicit Job(std::shared_ptr<Outcome> outcome, std::function<void()> body = {},
               std::atomic<int>* clock = nullptr)
      : outcome_(std::move(outcome)), body_(std::move(body)), clock_(clock) {}

  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  void Run() noexcept override {
    if (clock_ != nullptr) {
      const std::scoped_lock lock(outcome_->mu);
      outcome_->started_at = clock_->fetch_add(1);
    }
    if (body_) body_();
    // Copied first: the pool may free this job once Run returns, and a
    // foreground owner may as soon as it is told.
    const std::shared_ptr<Outcome> outcome = outcome_;
    const std::scoped_lock lock(outcome->mu);
    outcome->ran = true;
    outcome->cv.notify_all();
  }

  void Fail(core::Error error) noexcept override {
    const std::shared_ptr<Outcome> outcome = outcome_;
    const std::scoped_lock lock(outcome->mu);
    outcome->failed = std::move(error);
    outcome->cv.notify_all();
  }

 private:
  std::shared_ptr<Outcome> outcome_;
  std::function<void()> body_;
  std::atomic<int>* clock_;
};

core::SteadyTime Later() { return core::SteadyClock::now() + 10s; }

// Holds a pool thread until opened.
struct Blocker {
  testing::Latch started;
  testing::Latch release;
  std::shared_ptr<Outcome> outcome = std::make_shared<Outcome>();
  Job job{outcome, [this] {
            started.Open();
            release.Wait();
          }};
};

double Gauge(IoQueue queue) {
  return metrics::testing::GetGaugeValue(metrics::names::kEngineIoQueueDepth, queue).value_or(-1);
}

uint64_t Waits() {
  return metrics::testing::GetHistogramCount(metrics::names::kEngineIoWaitSeconds).value_or(0);
}

uint64_t Runs(IoKind kind) {
  return metrics::testing::GetHistogramCount(metrics::names::kEngineIoRunSeconds, kind).value_or(0);
}

TEST(ColdIoThreadsTest, ZeroResolvesBetweenTwoAndFour) {
  EXPECT_EQ(ColdIoThreads(0, 0), 2U);
  EXPECT_EQ(ColdIoThreads(0, 1), 2U);
  EXPECT_EQ(ColdIoThreads(0, 3), 3U);
  EXPECT_EQ(ColdIoThreads(0, 64), 4U);
  EXPECT_EQ(ColdIoThreads(8, 64), 8U);
}

TEST(IoPoolTest, AJobDequeuedPastItsDeadlineNeverRuns) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker a;
  Blocker b;
  pool.Submit(a.job, IoKind::kPoint, Later());
  pool.Submit(b.job, IoKind::kPoint, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());

  auto outcome = std::make_shared<Outcome>();
  bool ran = false;
  Job late(outcome, [&ran] { ran = true; });
  pool.Submit(late, IoKind::kLoad, core::SteadyClock::now() + 20ms);
  std::this_thread::sleep_for(40ms);
  a.release.Open();
  ASSERT_TRUE(outcome->Wait());
  EXPECT_FALSE(ran);
  ASSERT_TRUE(outcome->failed.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(outcome->failed->code(), core::ErrorCode::kTimeout);
  b.release.Open();
  ASSERT_TRUE(a.outcome->Wait());
  ASSERT_TRUE(b.outcome->Wait());
}

TEST(IoPoolTest, ForegroundRunsBeforeBackground) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker held;
  Blocker freed;
  pool.Submit(held.job, IoKind::kPoint, Later());
  pool.Submit(freed.job, IoKind::kPoint, Later());
  ASSERT_TRUE(held.started.Wait());
  ASSERT_TRUE(freed.started.Wait());

  std::atomic<int> clock{0};
  auto fill = std::make_shared<Outcome>();
  pool.SubmitBackground(std::make_unique<Job>(fill, nullptr, &clock), Later());
  auto miss = std::make_shared<Outcome>();
  Job point(miss, nullptr, &clock);
  pool.Submit(point, IoKind::kPoint, Later());
  freed.release.Open();
  ASSERT_TRUE(miss->Wait());
  ASSERT_TRUE(fill->Wait());
  EXPECT_EQ(miss->started_at, 0) << "the later foreground job ran first";
  EXPECT_EQ(fill->started_at, 1);
  held.release.Open();
  ASSERT_TRUE(held.outcome->Wait());
}

TEST(IoPoolTest, BackgroundWaitsWhileAnyForegroundIsQueued) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker a;
  Blocker b;
  pool.Submit(a.job, IoKind::kScan, Later());
  pool.Submit(b.job, IoKind::kPoint, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());
  // A scan the full lane holds back still keeps the fill queued.
  auto scan = std::make_shared<Outcome>();
  Job queued_scan(scan);
  pool.Submit(queued_scan, IoKind::kScan, Later());
  auto fill = std::make_shared<Outcome>();
  pool.SubmitBackground(std::make_unique<Job>(fill), Later());
  b.release.Open();
  ASSERT_TRUE(b.outcome->Wait());
  std::this_thread::sleep_for(30ms);
  EXPECT_FALSE(fill->Done()) << "a fill ran while a scan was queued";
  EXPECT_FALSE(scan->Done());
  a.release.Open();
  ASSERT_TRUE(scan->Wait());
  ASSERT_TRUE(fill->Wait());
  EXPECT_TRUE(fill->ran);
}

TEST(IoPoolTest, TheBackgroundQueueDropsWhenFull) {
  IoPool pool(IoPoolConfig{.threads = 2, .background_capacity = 2});
  Blocker a;
  Blocker b;
  pool.Submit(a.job, IoKind::kPoint, Later());
  pool.Submit(b.job, IoKind::kPoint, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());

  std::vector<std::shared_ptr<Outcome>> fills;
  for (int i = 0; i < 3; ++i) {
    fills.push_back(std::make_shared<Outcome>());
    pool.SubmitBackground(std::make_unique<Job>(fills.back()), Later());
  }
  ASSERT_TRUE(fills[2]->Done()) << "a full queue refuses at once";
  ASSERT_TRUE(fills[2]->failed.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(fills[2]->failed->code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(pool.QueuedForTesting(IoQueue::kBackground), 2U);
  a.release.Open();
  b.release.Open();
  ASSERT_TRUE(fills[0]->Wait());
  ASSERT_TRUE(fills[1]->Wait());
  EXPECT_TRUE(fills[0]->ran);
  EXPECT_TRUE(fills[1]->ran);
}

TEST(IoPoolTest, TheScanLaneLeavesAThreadForPoints) {
  IoPool pool(IoPoolConfig{.threads = 3});
  Blocker first;
  Blocker second;
  pool.Submit(first.job, IoKind::kScan, Later());
  pool.Submit(second.job, IoKind::kScan, Later());
  ASSERT_TRUE(first.started.Wait());
  ASSERT_TRUE(second.started.Wait());
  auto third = std::make_shared<Outcome>();
  Job scan(third);
  pool.Submit(scan, IoKind::kScan, Later());

  auto point = std::make_shared<Outcome>();
  Job read(point);
  pool.Submit(read, IoKind::kPoint, Later());
  ASSERT_TRUE(point->Wait(2s)) << "two scans held every thread";
  EXPECT_FALSE(third->Done()) << "a third scan took the point's thread";
  first.release.Open();
  ASSERT_TRUE(third->Wait());
  second.release.Open();
  ASSERT_TRUE(first.outcome->Wait());
  ASSERT_TRUE(second.outcome->Wait());
}

TEST(IoPoolTest, ShutdownFailsQueuedJobsAndJoinsRunningOnes) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker a;
  Blocker b;
  pool.Submit(a.job, IoKind::kPoint, Later());
  pool.Submit(b.job, IoKind::kLoad, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());
  auto queued = std::make_shared<Outcome>();
  Job waiting(queued);
  pool.Submit(waiting, IoKind::kScan, Later());
  auto fill = std::make_shared<Outcome>();
  pool.SubmitBackground(std::make_unique<Job>(fill), Later());

  std::atomic<bool> stopped{false};
  std::thread stopper([&pool, &stopped] {
    pool.Shutdown();
    stopped = true;
  });
  ASSERT_TRUE(queued->Wait());
  ASSERT_TRUE(fill->Wait());
  ASSERT_TRUE(queued->failed.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(queued->failed->code(), core::ErrorCode::kUnavailable);
  ASSERT_TRUE(fill->failed.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(fill->failed->code(), core::ErrorCode::kUnavailable);
  std::this_thread::sleep_for(20ms);
  EXPECT_FALSE(stopped) << "shutdown returned with jobs running";
  a.release.Open();
  b.release.Open();
  stopper.join();
  EXPECT_TRUE(a.outcome->ran);
  EXPECT_TRUE(b.outcome->ran);

  auto after = std::make_shared<Outcome>();
  Job late(after);
  pool.Submit(late, IoKind::kPoint, Later());
  ASSERT_TRUE(after->Done()) << "a submit after shutdown fails at once";
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(after->failed->code(), core::ErrorCode::kUnavailable);
}

TEST(IoPoolTest, QueueDepthAndWaitMetricsCountEachJob) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker a;
  Blocker b;
  const uint64_t waits = Waits();
  const uint64_t loads = Runs(IoKind::kLoad);
  const uint64_t fills = Runs(IoKind::kFill);
  pool.Submit(a.job, IoKind::kPoint, Later());
  pool.Submit(b.job, IoKind::kPoint, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());
  EXPECT_EQ(Gauge(IoQueue::kForeground), 0);

  std::vector<std::shared_ptr<Outcome>> outcomes;
  std::vector<std::unique_ptr<Job>> jobs;
  for (int i = 0; i < 3; ++i) {
    outcomes.push_back(std::make_shared<Outcome>());
    jobs.push_back(std::make_unique<Job>(outcomes.back()));
    pool.Submit(*jobs.back(), IoKind::kLoad, Later());
  }
  for (int i = 0; i < 2; ++i) {
    outcomes.push_back(std::make_shared<Outcome>());
    pool.SubmitBackground(std::make_unique<Job>(outcomes.back()), Later());
  }
  EXPECT_EQ(Gauge(IoQueue::kForeground), 3);
  EXPECT_EQ(Gauge(IoQueue::kBackground), 2);
  a.release.Open();
  b.release.Open();
  for (const auto& outcome : outcomes) ASSERT_TRUE(outcome->Wait());
  pool.Shutdown();
  EXPECT_EQ(Gauge(IoQueue::kForeground), 0);
  EXPECT_EQ(Gauge(IoQueue::kBackground), 0);
  EXPECT_EQ(Waits() - waits, 7U) << "the two blockers and five queued jobs";
  EXPECT_EQ(Runs(IoKind::kLoad) - loads, 3U);
  EXPECT_EQ(Runs(IoKind::kFill) - fills, 2U);
}

TEST(IoPoolTest, ACancelledQueuedJobNeverRunsOrFails) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker a;
  Blocker b;
  pool.Submit(a.job, IoKind::kPoint, Later());
  pool.Submit(b.job, IoKind::kPoint, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());
  auto outcome = std::make_shared<Outcome>();
  Job queued(outcome);
  pool.Submit(queued, IoKind::kLoad, Later());
  EXPECT_EQ(Gauge(IoQueue::kForeground), 1);
  EXPECT_TRUE(pool.Cancel(queued));
  EXPECT_EQ(Gauge(IoQueue::kForeground), 0);
  EXPECT_FALSE(pool.Cancel(queued)) << "taken back once";
  a.release.Open();
  b.release.Open();
  pool.WaitIdleForTesting();
  EXPECT_FALSE(outcome->Done()) << "a cancelled job ran or failed";
  // The owner may submit it again.
  pool.Submit(queued, IoKind::kLoad, Later());
  ASSERT_TRUE(outcome->Wait());
  EXPECT_TRUE(outcome->ran);
}

TEST(IoPoolTest, ACancelOfARunningJobLosesAndTheJobLands) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker running;
  pool.Submit(running.job, IoKind::kScan, Later());
  ASSERT_TRUE(running.started.Wait());
  EXPECT_FALSE(pool.Cancel(running.job));
  running.release.Open();
  ASSERT_TRUE(running.outcome->Wait());
  EXPECT_TRUE(running.outcome->ran);
}

// The answer is decided in the hold a dequeue takes a job in: exactly
// one of a true Cancel, or a run that lands, ever happens.
TEST(IoPoolTest, ACancelRacingTheDequeueHasOneWinner) {
  IoPool pool(IoPoolConfig{.threads = 2});
  int cancelled = 0;
  int ran = 0;
  for (int i = 0; i < 2000; ++i) {
    auto outcome = std::make_shared<Outcome>();
    Job job(outcome);
    pool.Submit(job, IoKind::kPoint, Later());
    if (i % 2 == 1) std::this_thread::yield();
    const bool taken = pool.Cancel(job);
    if (taken) {
      pool.WaitIdleForTesting();
      ASSERT_FALSE(outcome->Done()) << "cancelled, then run, at " << i;
      ++cancelled;
    } else {
      ASSERT_TRUE(outcome->Wait()) << "neither cancelled nor run, at " << i;
      ASSERT_TRUE(outcome->ran);
      ++ran;
    }
  }
  pool.Shutdown();
  EXPECT_EQ(cancelled + ran, 2000);
  RecordProperty("cancelled", cancelled);
  RecordProperty("ran", ran);
}

TEST(IoPoolTest, TheWaitHistogramMeasuresTimeQueued) {
  IoPool pool(IoPoolConfig{.threads = 2});
  Blocker a;
  Blocker b;
  pool.Submit(a.job, IoKind::kPoint, Later());
  pool.Submit(b.job, IoKind::kPoint, Later());
  ASSERT_TRUE(a.started.Wait());
  ASSERT_TRUE(b.started.Wait());
  const double before =
      metrics::testing::GetHistogramSum(metrics::names::kEngineIoWaitSeconds).value_or(0);
  auto outcome = std::make_shared<Outcome>();
  Job queued(outcome);
  pool.Submit(queued, IoKind::kPoint, Later());
  std::this_thread::sleep_for(50ms);
  a.release.Open();
  ASSERT_TRUE(outcome->Wait());
  b.release.Open();
  pool.Shutdown();
  const double waited =
      metrics::testing::GetHistogramSum(metrics::names::kEngineIoWaitSeconds).value_or(0) - before;
  EXPECT_GE(waited, 0.045) << "the queued job's wait went unmeasured";
}

}  // namespace
}  // namespace abyss::engine
