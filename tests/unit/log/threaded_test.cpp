#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "abyss/config/config.h"
#include "abyss/log/log.h"
#include "abyss/log/testing.h"

namespace abyss::log {
namespace {

constexpr int kThreads = 16;
constexpr int kOpsPerThread = 1000;

class ThreadedLogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    testing::Reset();
    config::LogConfig cfg;
    cfg.default_level = Level::kDebug;
    cfg.format = "json";
    cfg.sink = "stdout";
    Init(cfg);
  }
  void TearDown() override { testing::Reset(); }
};

TEST_F(ThreadedLogTest, ConcurrentEmissionCapturesAllRecords) {
  testing::CapturingSink capture;

  std::atomic<int> started{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([i, &started, &go]() {
      const Logger l = Get("threaded.emitter");
      ++started;
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      for (int op = 0; op < kOpsPerThread; ++op) {
        ABYSS_LOG_INFO(l, "tick", {"thread", static_cast<int64_t>(i)},
                       {"op", static_cast<int64_t>(op)});
      }
    });
  }
  while (started.load() < kThreads) std::this_thread::yield();
  go.store(true, std::memory_order_release);
  for (auto& t : threads) t.join();

  EXPECT_EQ(capture.Size(), static_cast<size_t>(kThreads * kOpsPerThread));
}

TEST_F(ThreadedLogTest, ConcurrentGetReturnsConsistentLoggers) {
  std::vector<std::thread> threads;
  std::atomic<bool> go{false};
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&go]() {
      while (!go.load()) std::this_thread::yield();
      for (int k = 0; k < 1000; ++k) {
        const Logger l = Get("shared");
        ASSERT_EQ(l.Name(), "shared");
      }
    });
  }
  go.store(true);
  for (auto& t : threads) t.join();
}

}  // namespace
}  // namespace abyss::log
