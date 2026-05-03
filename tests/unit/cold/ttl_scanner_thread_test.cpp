#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "abyss/cold/ttl_scanner.h"
#include "abyss/metrics/testing.h"

namespace abyss::cold {
namespace {

using namespace std::chrono_literals;

class CountingBackend : public TtlScannerBackend {
 public:
  core::Result<SweepReport> SampleAndExpire(SweepRequest /*req*/) override {
    calls_.fetch_add(1, std::memory_order_relaxed);
    return SweepReport{};
  }
  size_t calls() const { return calls_.load(std::memory_order_relaxed); }

 private:
  std::atomic<size_t> calls_{0};
};

TtlScanner::Config FastConfig() {
  TtlScanner::Config cfg;
  cfg.base_interval = 5ms;
  cfg.min_interval = 5ms;
  cfg.max_interval = 5ms;
  cfg.cpu_ewma_window = 1s;
  return cfg;
}

TtlScanner::Hooks RealHooksWithFakeDisk() {
  TtlScanner::Hooks hooks;
  hooks.disk_usage = []() -> core::Result<double> { return 0.0; };
  return hooks;
}

TEST(TtlScannerThreadTest, StartTicksThroughBackend) {
  metrics::testing::Reset();
  CountingBackend backend;
  auto scanner = TtlScanner::Create(backend, FastConfig(), TtlScanner::ExecutionMode::kOwnedThread,
                                    RealHooksWithFakeDisk());
  ASSERT_TRUE(scanner.has_value());
  (*scanner)->Start();
  std::this_thread::sleep_for(80ms);
  (*scanner)->Stop();
  EXPECT_GE(backend.calls(), 2U);
  metrics::testing::Reset();
}

TEST(TtlScannerThreadTest, StopJoinsPromptly) {
  metrics::testing::Reset();
  CountingBackend backend;
  auto scanner = TtlScanner::Create(backend, FastConfig(), TtlScanner::ExecutionMode::kOwnedThread,
                                    RealHooksWithFakeDisk());
  ASSERT_TRUE(scanner.has_value());
  (*scanner)->Start();
  std::this_thread::sleep_for(20ms);

  const auto t0 = std::chrono::steady_clock::now();
  (*scanner)->Stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;
  // Stop signals via cv.notify_all; the loop must wake within one tick.
  EXPECT_LT(elapsed, 200ms);
  metrics::testing::Reset();
}

TEST(TtlScannerThreadTest, StartIsIdempotent) {
  metrics::testing::Reset();
  CountingBackend backend;
  auto scanner = TtlScanner::Create(backend, FastConfig(), TtlScanner::ExecutionMode::kOwnedThread,
                                    RealHooksWithFakeDisk());
  ASSERT_TRUE(scanner.has_value());
  (*scanner)->Start();
  (*scanner)->Start();  // Second call must be a no-op.
  std::this_thread::sleep_for(40ms);
  (*scanner)->Stop();
  metrics::testing::Reset();
}

TEST(TtlScannerThreadTest, DestructorJoinsRunningThread) {
  metrics::testing::Reset();
  CountingBackend backend;
  {
    auto scanner = TtlScanner::Create(
        backend, FastConfig(), TtlScanner::ExecutionMode::kOwnedThread, RealHooksWithFakeDisk());
    ASSERT_TRUE(scanner.has_value());
    (*scanner)->Start();
    std::this_thread::sleep_for(20ms);
    // Drop without explicit Stop — destructor must clean up.
  }
  metrics::testing::Reset();
}

TEST(TtlScannerThreadTest, DisabledConfigDoesNotSpawnThread) {
  metrics::testing::Reset();
  CountingBackend backend;
  auto cfg = FastConfig();
  cfg.enabled = false;
  auto scanner = TtlScanner::Create(backend, cfg, TtlScanner::ExecutionMode::kOwnedThread,
                                    RealHooksWithFakeDisk());
  ASSERT_TRUE(scanner.has_value());
  (*scanner)->Start();
  std::this_thread::sleep_for(40ms);
  (*scanner)->Stop();
  EXPECT_EQ(backend.calls(), 0U);
  metrics::testing::Reset();
}

}  // namespace
}  // namespace abyss::cold
