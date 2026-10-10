#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::metrics {
namespace {

constexpr int kThreads = 16;
constexpr int kOpsPerThread = 10000;

class ThreadedMetricsTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(ThreadedMetricsTest, ConcurrentCounterIncrements) {
  auto& reg = Registry::Instance();
  auto handle = reg.Counter(names::kMissesTotal);

  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&handle, &go]() {
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      for (int op = 0; op < kOpsPerThread; ++op) handle.Increment();
    });
  }
  go.store(true, std::memory_order_release);
  for (auto& t : threads) t.join();

  const auto value = testing::GetCounterValue(names::kMissesTotal);
  EXPECT_EQ(value, static_cast<double>(kThreads * kOpsPerThread));
}

TEST_F(ThreadedMetricsTest, ConcurrentRegistrationOfSameSeriesIsSafe) {
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&go]() {
      while (!go.load()) std::this_thread::yield();
      for (int op = 0; op < 500; ++op) {
        auto h = Registry::Instance().Counter(names::kHitsTotal, Tier::kHot);
        h.Increment();
      }
    });
  }
  go.store(true);
  for (auto& t : threads) t.join();

  const auto value = testing::GetCounterValue(names::kHitsTotal, Tier::kHot);
  EXPECT_EQ(value, static_cast<double>(kThreads * 500));
}

TEST_F(ThreadedMetricsTest, ConcurrentHistogramObservations) {
  auto& reg = Registry::Instance();
  auto h = reg.Histogram(names::kWalFlushDurationSeconds);

  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&h, &go]() {
      while (!go.load()) std::this_thread::yield();
      for (int op = 0; op < kOpsPerThread; ++op) h.Observe(0.001);
    });
  }
  go.store(true);
  for (auto& t : threads) t.join();

  const auto count = testing::GetHistogramCount(names::kWalFlushDurationSeconds);
  EXPECT_EQ(count, static_cast<uint64_t>(kThreads * kOpsPerThread));
}

}  // namespace
}  // namespace abyss::metrics
