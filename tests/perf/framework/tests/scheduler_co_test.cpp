#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>

#include "histogram.h"
#include "scheduler.h"

namespace abyss::perf {
namespace {

// Pins the coordinated-omission contract: when a worker recording at a
// scheduled interval observes a stall of duration D >> interval, the
// corrected histogram must contain synthetic samples populating the tail up
// to D. A naive histogram observing the same events will not.
//
// If this test ever weakens or is disabled, the perf harness is silently
// reporting lies about tail latency. See ADP-013 §Coordinated omission.
TEST(CoordinatedOmissionTest, StallInjectsSyntheticTailSamples) {
  constexpr int64_t kIntervalNs = 100'000;          // 10K ops/s → 100us schedule
  constexpr int64_t kNormalLatencyNs = 10'000;      // 10us normal service time
  constexpr int64_t kStallLatencyNs = 100'000'000;  // 100ms stall
  constexpr int64_t kNormalOps = 999;

  Histogram corrected;
  Histogram naive;

  for (int64_t i = 0; i < kNormalOps; ++i) {
    corrected.RecordCorrected(kNormalLatencyNs, kIntervalNs);
    naive.Record(kNormalLatencyNs);
  }
  corrected.RecordCorrected(kStallLatencyNs, kIntervalNs);
  naive.Record(kStallLatencyNs);

  // Corrected: stall produced ~1000 synthetic samples populating the tail.
  // p99 should land deep in the stall band.
  const int64_t corrected_p99 = corrected.PercentileNs(99.0);
  EXPECT_GT(corrected_p99, 50'000'000)
      << "corrected p99 = " << corrected_p99 << " ns; expected deep stall tail";

  // Naive: only one outlier sample at 100ms in ~1000 total. The 990th-ranked
  // sample is a normal one, so p99 is approximately the normal latency. This
  // is exactly the dishonesty CO correction prevents.
  const int64_t naive_p99 = naive.PercentileNs(99.0);
  EXPECT_LT(naive_p99, 100'000) << "naive p99 = " << naive_p99 << " ns; expected normal-band value";

  EXPECT_GT(corrected.Count(), naive.Count()) << "synthetic samples should inflate corrected count";
}

TEST(CoordinatedOmissionTest, NormalRecordingMatchesBetweenCorrectedAndNaive) {
  constexpr int64_t kIntervalNs = 100'000;
  constexpr int64_t kLatencyNs = 50'000;
  constexpr int kSamples = 10'000;

  Histogram corrected;
  Histogram naive;
  for (int i = 0; i < kSamples; ++i) {
    corrected.RecordCorrected(kLatencyNs, kIntervalNs);
    naive.Record(kLatencyNs);
  }
  // Without any stall (latency <= interval), corrected behaves identically.
  EXPECT_EQ(corrected.Count(), naive.Count());
  EXPECT_EQ(corrected.PercentileNs(99.0), naive.PercentileNs(99.0));
}

TEST(CoSchedulerTest, OpenLoopComputesIntendedSendTimes) {
  using namespace std::chrono;
  const auto start = CoScheduler::Clock::now();
  CoScheduler sched{10'000, start};
  EXPECT_TRUE(sched.IsOpenLoop());
  EXPECT_EQ(sched.ExpectedIntervalNs(), 100'000);

  EXPECT_EQ(sched.IntendedSendTime(0), start);
  EXPECT_EQ(sched.IntendedSendTime(1) - start, nanoseconds{100'000});
  EXPECT_EQ(sched.IntendedSendTime(10) - start, nanoseconds{1'000'000});
}

TEST(CoSchedulerTest, ClosedLoopReportsZeroInterval) {
  const auto start = CoScheduler::Clock::now();
  CoScheduler sched{0, start};
  EXPECT_FALSE(sched.IsOpenLoop());
  EXPECT_EQ(sched.ExpectedIntervalNs(), 0);
}

TEST(CoSchedulerTest, SleepUntilReturnsImmediatelyForPastTimes) {
  const auto start = CoScheduler::Clock::now() - std::chrono::seconds{1};
  const auto wake = CoScheduler::SleepUntil(start);
  EXPECT_GE(wake, start);
}

}  // namespace
}  // namespace abyss::perf
