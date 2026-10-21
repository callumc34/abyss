#include "abyss/queue/durability_window.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;
using Clock = DurabilityWindow::Clock;

constexpr uint64_t kMaxBytes = 1000;
constexpr auto kMaxAge = 100ms;

// A flush that left frames unflushed: the age runs from `since`.
void SetAge(DurabilityWindow::LogAge& age, Clock::time_point since) {
  age.Flushed(since, 0, [] { return uint64_t{1}; });
}

class DurabilityWindowTest : public ::testing::Test {
 protected:
  // After the reset, so the window's counters land in the fresh registry.
  void SetUp() override {
    metrics::testing::Reset();
    window_ = std::make_unique<DurabilityWindow>(kMaxBytes, kMaxAge);
  }
  void TearDown() override {
    window_.reset();
    metrics::testing::Reset();
  }

  std::future<core::Result<void>> AdmitAsync(Clock::duration wait = 5s) {
    return std::async(std::launch::async,
                      [this, wait] { return window_->Admit(age_, Clock::now() + wait); });
  }

  static bool Pending(std::future<core::Result<void>>& f) {
    return f.wait_for(30ms) == std::future_status::timeout;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<DurabilityWindow> window_;
  DurabilityWindow::LogAge age_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(DurabilityWindowTest, EmptyWindowAlwaysAdmits) {
  // Even with a stale age: nothing is unflushed, so nothing can drain.
  SetAge(age_, Clock::now() - 1h);
  EXPECT_TRUE(window_->Admit(age_, Clock::now()).has_value());
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureWaitsTotal), 0.0);
}

TEST_F(DurabilityWindowTest, AdmitsBelowBothBounds) {
  window_->Add(kMaxBytes - 1);
  age_.Start(Clock::now());
  EXPECT_TRUE(window_->Admit(age_, Clock::now()).has_value());
}

TEST_F(DurabilityWindowTest, BlocksOverTheByteBoundUntilARelease) {
  window_->Add(kMaxBytes);
  auto admitted = AdmitAsync();
  ASSERT_TRUE(Pending(admitted));

  window_->Release(kMaxBytes);
  ASSERT_EQ(admitted.wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(admitted.get().has_value());
  EXPECT_EQ(window_->UnflushedBytes(), 0U);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureWaitsTotal), 1.0);
}

TEST_F(DurabilityWindowTest, BlocksOverTheAgeBoundUntilAFlush) {
  window_->Add(1);
  SetAge(age_, Clock::now() - (2 * kMaxAge));
  auto admitted = AdmitAsync();
  ASSERT_TRUE(Pending(admitted));

  // A flush that leaves entries behind restarts the clock at its snapshot.
  window_->Add(1);
  SetAge(age_, Clock::now());
  window_->Release(1);
  ASSERT_EQ(admitted.wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(admitted.get().has_value());
}

TEST_F(DurabilityWindowTest, OtherLogsAgeDoesNotBlock) {
  DurabilityWindow::LogAge stale;
  SetAge(stale, Clock::now() - (2 * kMaxAge));
  window_->Add(1);
  EXPECT_TRUE(window_->Admit(age_, Clock::now()).has_value());
  EXPECT_FALSE(window_->Admit(stale, Clock::now()).has_value());
}

TEST_F(DurabilityWindowTest, RejectsAtTheDeadline) {
  window_->Add(kMaxBytes);
  const auto start = Clock::now();
  auto rejected = window_->Admit(age_, start + 30ms);
  EXPECT_GE(Clock::now() - start, 30ms);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), core::ErrorCode::kResourceExhausted);
  const std::string& message = rejected.error().message();
  EXPECT_NE(message.find("WAL durability window full: the device is not keeping up with writes"),
            std::string::npos)
      << message;
  EXPECT_NE(message.find("1000 of 1000 bytes"), std::string::npos) << message;
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureRejectionsTotal),
            1.0);
}

// A best-effort append passes a deadline already past: it must neither
// wait nor count as client backpressure.
TEST_F(DurabilityWindowTest, PastDeadlineChecksWithoutWaitingOrCounting) {
  window_->Add(kMaxBytes);
  const auto start = Clock::now();
  auto admitted = window_->Admit(age_, start);
  EXPECT_LT(Clock::now() - start, 5ms);
  ASSERT_FALSE(admitted.has_value());
  EXPECT_EQ(admitted.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureWaitsTotal), 0.0);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureRejectionsTotal),
            0.0);
}

TEST_F(DurabilityWindowTest, ShutdownWakesWaitersUnavailable) {
  window_->Add(kMaxBytes);
  auto admitted = AdmitAsync(10s);
  ASSERT_TRUE(Pending(admitted));

  window_->Shutdown();
  ASSERT_EQ(admitted.wait_for(5s), std::future_status::ready);
  auto result = admitted.get();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kUnavailable);
}

TEST(LogAgeTest, StartsOnlyWhenClear) {
  DurabilityWindow::LogAge age;
  const auto t0 = Clock::now();
  EXPECT_EQ(age.Age(t0), Clock::duration::zero());

  age.Start(t0);
  age.Start(t0 + 1s);
  EXPECT_EQ(age.Age(t0 + 2s), std::chrono::duration_cast<Clock::duration>(2s));
}

// The A4 protocol's interleavings, each driven through the flusher's
// tail reads. P_s is 10; a frame reserved past it moves the tail to 20.
class LogAgeFlushTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kSnapshot = 10;
  static constexpr uint64_t kPast = 20;

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  DurabilityWindow::LogAge age_;
  const Clock::time_point t0_ = Clock::now();
  // The flush's snapshot time, and a later append's.
  const Clock::time_point snapshot_at_ = t0_ + 1s;
  const Clock::time_point appended_at_ = t0_ + 2s;
  const Clock::time_point now_ = t0_ + 5s;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  Clock::duration Since(Clock::time_point t) const { return now_ - t; }
};

TEST_F(LogAgeFlushTest, AFrameAlreadyPastTheSnapshotRestartsTheClockAtIt) {
  age_.Start(t0_);
  age_.Flushed(snapshot_at_, kSnapshot, [] { return kPast; });
  EXPECT_EQ(age_.Age(now_), Since(snapshot_at_));
}

TEST_F(LogAgeFlushTest, NothingPastTheSnapshotClears) {
  age_.Start(t0_);
  int reads = 0;
  age_.Flushed(snapshot_at_, kSnapshot, [&reads] {
    ++reads;
    return kSnapshot;
  });
  EXPECT_EQ(reads, 2);
  EXPECT_EQ(age_.Age(now_), Clock::duration::zero());
}

// The appender reserves and starts the clock before the clear: its
// Start finds the clock running and leaves it, and the re-read sees it.
TEST_F(LogAgeFlushTest, AnAppendBeforeTheClearIsSeenByTheReRead) {
  age_.Start(t0_);
  uint64_t tail = kSnapshot;
  int reads = 0;
  age_.Flushed(snapshot_at_, kSnapshot, [&] {
    if (++reads == 1) {
      const uint64_t seen = tail;
      tail = kPast;
      age_.Start(appended_at_);
      return seen;
    }
    return tail;
  });
  EXPECT_EQ(age_.Age(now_), Since(snapshot_at_));
}

// The appender reserves before the clear but starts the clock after it:
// its Start wins the CAS, and the re-read then sets the older snapshot.
TEST_F(LogAgeFlushTest, AStartAfterTheClearIsKeptOrMadeOlder) {
  age_.Start(t0_);
  int reads = 0;
  age_.Flushed(snapshot_at_, kSnapshot, [&] {
    if (++reads == 1) return kSnapshot;
    age_.Start(appended_at_);
    EXPECT_EQ(age_.Age(now_), Since(appended_at_));
    return kPast;
  });
  EXPECT_EQ(age_.Age(now_), Since(snapshot_at_));
}

// The appender reserves after the re-read: the flusher leaves the clock
// clear, and the appender's Start finds it so and starts it.
TEST_F(LogAgeFlushTest, AnAppendAfterTheReReadStartsTheClock) {
  age_.Start(t0_);
  age_.Flushed(snapshot_at_, kSnapshot, [] { return kSnapshot; });
  EXPECT_EQ(age_.Age(now_), Clock::duration::zero());
  age_.Start(appended_at_);
  EXPECT_EQ(age_.Age(now_), Since(appended_at_));
}

}  // namespace
}  // namespace abyss::queue
