#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>

#include "scheduler.h"

namespace abyss::perf {
namespace {

// Coordinated-omission accounting through RunLoop is pinned by
// CoordinatedOmissionTest in run_loop_test.cpp; this file owns the
// schedule itself.
TEST(CoSchedulerTest, OpenLoopSpacesSendsByTheInverseRate) {
  using namespace std::chrono;
  const auto start = CoScheduler::Clock::now();
  const CoScheduler sched{10'000, start};
  EXPECT_TRUE(sched.IsOpenLoop());

  EXPECT_EQ(sched.IntendedSendTime(0), start);
  EXPECT_EQ(sched.IntendedSendTime(1) - start, nanoseconds{100'000});
  EXPECT_EQ(sched.IntendedSendTime(10) - start, nanoseconds{1'000'000});
}

TEST(CoSchedulerTest, ThreadsInterleaveAcrossTheSlot) {
  using namespace std::chrono;
  const auto start = CoScheduler::Clock::now();
  const CoScheduler first{1000, start, 1, 4, 0};
  const CoScheduler third{1000, start, 1, 4, 2};
  const CoScheduler bursty{1000, start, 16, 4, 1};
  EXPECT_EQ(first.IntendedSendTime(0), start);
  EXPECT_EQ(third.IntendedSendTime(0) - start, microseconds{500});
  EXPECT_EQ(third.IntendedSendTime(1) - start, microseconds{1500});
  EXPECT_EQ(bursty.IntendedSendTime(0) - start, microseconds{4000});
}

TEST(CoSchedulerTest, ZeroRateIsClosedLoop) {
  const CoScheduler sched{0, CoScheduler::Clock::now()};
  EXPECT_FALSE(sched.IsOpenLoop());
}

// 80 connections at 1250 req/s each: 1/20 of an 800us slot is 40us,
// but half a core over 80 threads allows only 5us.
TEST(CoSchedulerTest, SpinWindowIsBoundedPerSlotAndPerRun) {
  using namespace std::chrono_literals;
  EXPECT_EQ(CoScheduler::SpinWindowFor(800us, 80), 5us);
  EXPECT_EQ(CoScheduler::SpinWindowFor(2ms, 1),
            std::min<std::chrono::nanoseconds>(CoScheduler::kMaxSpinWindow, 100us));
  EXPECT_EQ(CoScheduler::SpinWindowFor(70ms, 14),
            std::chrono::nanoseconds{CoScheduler::kMaxSpinWindow});
  const CoScheduler burst{1250, CoScheduler::Clock::now(), 16, 64};
  EXPECT_EQ(burst.SpinWindow(), CoScheduler::SpinWindowFor(12800us, 64));
}

TEST(CoSchedulerTest, SleepStepsStopShortOfTheSpinWindow) {
  using namespace std::chrono_literals;
  const auto now = CoScheduler::Clock::now();
  const CoScheduler sched{100, now};  // 10ms slots
  const auto spin = sched.SpinWindow();
  ASSERT_GT(spin, 0ns);

  const auto wake = sched.SleepStepEnd(now, now + 10ms);
  ASSERT_TRUE(wake.has_value());
  EXPECT_GT(wake.value_or(now), now);
  EXPECT_LE(wake.value_or(now), now + 10ms - spin);
  EXPECT_FALSE(sched.SleepStepEnd(now, now + spin).has_value());
  EXPECT_FALSE(sched.SleepStepEnd(now, now - 1ms).has_value());

  // 20us before the spin window: macOS spins rather than half-step.
  const auto close = now + spin + 20us;
#ifdef __APPLE__
  EXPECT_FALSE(sched.SleepStepEnd(now, close).has_value());
  EXPECT_TRUE(sched.SleepStepEnd(now, now + spin + CoScheduler::kMinSleepStep).has_value());
#else
  EXPECT_EQ(sched.SleepStepEnd(now, close), now + 20us);
#endif
}

TEST(CoSchedulerTest, SleepUntilNeverReturnsEarly) {
  using namespace std::chrono_literals;
  const CoScheduler sched{1000, CoScheduler::Clock::now()};
  for (const auto ahead : {50us, 500us, 3000us}) {
    const auto target = CoScheduler::Clock::now() + ahead;
    EXPECT_GE(sched.SleepUntil(target), target);
    EXPECT_GE(CoScheduler::Clock::now(), target);
  }
}

TEST(CoSchedulerTest, SleepUntilReturnsImmediatelyForPastTimes) {
  const CoScheduler sched{1000, CoScheduler::Clock::now()};
  const auto start = CoScheduler::Clock::now() - std::chrono::seconds{1};
  const auto wake = sched.SleepUntil(start);
  EXPECT_GE(wake, start);
}

}  // namespace
}  // namespace abyss::perf
