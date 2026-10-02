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
                      [this, wait] { return window_->Admit(shard_, Clock::now() + wait); });
  }

  static bool Pending(std::future<core::Result<void>>& f) {
    return f.wait_for(30ms) == std::future_status::timeout;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<DurabilityWindow> window_;
  DurabilityWindow::ShardAge shard_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(DurabilityWindowTest, EmptyWindowAlwaysAdmits) {
  // Even with a stale age: nothing is unflushed, so nothing can drain.
  shard_.Set(Clock::now() - 1h);
  EXPECT_TRUE(window_->Admit(shard_, Clock::now()).has_value());
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureWaitsTotal), 0.0);
}

TEST_F(DurabilityWindowTest, AdmitsBelowBothBounds) {
  window_->Add(kMaxBytes - 1);
  shard_.Start(Clock::now());
  EXPECT_TRUE(window_->Admit(shard_, Clock::now()).has_value());
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
  shard_.Set(Clock::now() - (2 * kMaxAge));
  auto admitted = AdmitAsync();
  ASSERT_TRUE(Pending(admitted));

  // A flush that leaves entries behind restarts the clock at its snapshot.
  window_->Add(1);
  shard_.Set(Clock::now());
  window_->Release(1);
  ASSERT_EQ(admitted.wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(admitted.get().has_value());
}

TEST_F(DurabilityWindowTest, OtherShardsAgeDoesNotBlock) {
  DurabilityWindow::ShardAge stale;
  stale.Set(Clock::now() - (2 * kMaxAge));
  window_->Add(1);
  EXPECT_TRUE(window_->Admit(shard_, Clock::now()).has_value());
  EXPECT_FALSE(window_->Admit(stale, Clock::now()).has_value());
}

TEST_F(DurabilityWindowTest, RejectsAtTheDeadline) {
  window_->Add(kMaxBytes);
  const auto start = Clock::now();
  auto rejected = window_->Admit(shard_, start + 30ms);
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
  auto admitted = window_->Admit(shard_, start);
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

TEST(DurabilityWindowShardAgeTest, StartsOnlyWhenClear) {
  DurabilityWindow::ShardAge age;
  const auto t0 = Clock::now();
  EXPECT_EQ(age.Age(t0), Clock::duration::zero());

  age.Start(t0);
  age.Start(t0 + 1s);
  EXPECT_EQ(age.Age(t0 + 2s), std::chrono::duration_cast<Clock::duration>(2s));

  age.Clear();
  EXPECT_EQ(age.Age(t0 + 2s), Clock::duration::zero());
  age.Start(t0 + 1s);
  EXPECT_EQ(age.Age(t0 + 2s), std::chrono::duration_cast<Clock::duration>(1s));
}

}  // namespace
}  // namespace abyss::queue
