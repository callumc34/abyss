#include "abyss/cold/ttl_scanner.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <vector>

#include "abyss/metrics/testing.h"

namespace abyss::cold {
namespace {

using namespace std::chrono_literals;

// Backend that hands back a queue of canned reports and records the sample
// sizes it was asked for. Used by all tests in this file — the scanner under
// test never touches real storage. Optionally bumps a caller-supplied CPU
// counter inside SampleAndExpire so the test can simulate scanner CPU cost.
class CannedBackend : public TtlScannerBackend {
 public:
  void EnqueueReport(SweepReport r) { reports_.push_back(r); }
  size_t calls() const { return calls_; }
  size_t last_sample_size() const { return last_sample_size_; }
  void SetCpuPerCall(std::chrono::nanoseconds delta, std::atomic<int64_t>* counter) {
    cpu_per_call_ = delta;
    cpu_counter_ = counter;
  }

  core::Result<SweepReport> SampleAndExpire(SweepRequest req) override {
    if (cpu_counter_ != nullptr) {
      cpu_counter_->fetch_add(cpu_per_call_.count(), std::memory_order_relaxed);
    }
    last_sample_size_ = req.sample_size;
    ++calls_;
    if (reports_.empty()) return SweepReport{};
    auto out = reports_.front();
    reports_.erase(reports_.begin());
    return out;
  }

 private:
  std::vector<SweepReport> reports_;
  size_t calls_ = 0;
  size_t last_sample_size_ = 0;
  std::chrono::nanoseconds cpu_per_call_{0};
  std::atomic<int64_t>* cpu_counter_ = nullptr;
};

// Test harness: synthetic monotonic + cpu clocks the test advances by hand,
// plus an adjustable disk-usage source. Manual-tick mode means no threads.
struct Harness {
  Harness(const Harness&) = delete;
  Harness& operator=(const Harness&) = delete;
  Harness(Harness&&) = delete;
  Harness& operator=(Harness&&) = delete;

  Harness() {
    metrics::testing::Reset();
    cfg.base_sample_size = 20;
    cfg.min_sample_size = 5;
    cfg.max_sample_size = 200;
    cfg.base_interval = 1000ms;
    cfg.min_interval = 100ms;
    cfg.max_interval = 60'000ms;
    cfg.high_threshold = 0.25;
    cfg.low_threshold = 0.05;
    cfg.rate_increase_factor = 1.5;
    cfg.rate_decrease_factor = 0.7;
    cfg.disk_pressure_threshold = 0.9;
    cfg.disk_pressure_release_threshold = 0.855;
    cfg.max_cpu_fraction = 0.10;
    cfg.cpu_ewma_window = 30s;

    hooks.steady_clock = [this] {
      return core::SteadyTime{} + std::chrono::nanoseconds{wall_ns.load()};
    };
    hooks.cpu_clock = [this] { return std::chrono::nanoseconds{cpu_ns.load()}; };
    hooks.disk_usage = [this]() -> core::Result<double> { return disk_fraction.load(); };
  }
  ~Harness() { metrics::testing::Reset(); }

  std::unique_ptr<TtlScanner> Build() {
    auto r = TtlScanner::Create(backend, cfg, TtlScanner::ExecutionMode::kManualTick, hooks);
    EXPECT_TRUE(r.has_value()) << (r.has_value() ? "" : r.error().message());
    return std::move(*r);
  }

  void AdvanceWall(std::chrono::milliseconds delta) {
    wall_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(delta).count());
  }
  void AdvanceCpu(std::chrono::nanoseconds delta) { cpu_ns.fetch_add(delta.count()); }

  CannedBackend backend;
  TtlScanner::Config cfg;
  TtlScanner::Hooks hooks;
  std::atomic<int64_t> wall_ns{0};
  std::atomic<int64_t> cpu_ns{0};
  std::atomic<double> disk_fraction{0.0};
};

TEST(TtlScannerTest, InitialDerivedValuesMatchConfig) {
  Harness h;
  auto scanner = h.Build();
  auto snap = scanner->CurrentSnapshot();
  EXPECT_EQ(snap.sample_size, h.cfg.base_sample_size);
  EXPECT_EQ(snap.interval, h.cfg.base_interval);
  EXPECT_DOUBLE_EQ(snap.rate_multiplier, 1.0);
  EXPECT_FALSE(snap.disk_pressure_active);
}

TEST(TtlScannerTest, RateIncreasesWhenExpiredRatioAboveHigh) {
  Harness h;
  auto scanner = h.Build();
  // Five consecutive ticks with 50% expired ratio (above 0.25 high threshold).
  for (int i = 0; i < 5; ++i) {
    SweepReport r;
    r.sampled_strings = 10;
    r.sampled_collections = 10;
    r.expired_strings = 5;
    r.expired_collections = 5;
    h.backend.EnqueueReport(r);
  }
  for (int i = 0; i < 5; ++i) {
    h.AdvanceWall(1000ms);
    auto t = scanner->Tick();
    ASSERT_TRUE(t.has_value());
  }
  auto snap = scanner->CurrentSnapshot();
  EXPECT_GT(snap.rate_multiplier, 1.0);
  EXPECT_GT(snap.sample_size, h.cfg.base_sample_size);
  EXPECT_LT(snap.interval, h.cfg.base_interval);
}

TEST(TtlScannerTest, RateDecreasesWhenExpiredRatioBelowLow) {
  Harness h;
  auto scanner = h.Build();
  for (int i = 0; i < 5; ++i) {
    SweepReport r;
    r.sampled_strings = 100;
    r.sampled_collections = 100;
    // No expired records at all → ratio 0, well below low_threshold (0.05).
    h.backend.EnqueueReport(r);
  }
  for (int i = 0; i < 5; ++i) {
    h.AdvanceWall(1000ms);
    auto t = scanner->Tick();
    ASSERT_TRUE(t.has_value());
  }
  auto snap = scanner->CurrentSnapshot();
  EXPECT_LT(snap.rate_multiplier, 1.0);
  EXPECT_LT(snap.sample_size, h.cfg.base_sample_size);
  EXPECT_GT(snap.interval, h.cfg.base_interval);
}

TEST(TtlScannerTest, RateStableInDeadband) {
  Harness h;
  auto scanner = h.Build();
  // Ratio 0.10 is between low (0.05) and high (0.25) — no change.
  for (int i = 0; i < 5; ++i) {
    SweepReport r;
    r.sampled_strings = 100;
    r.expired_strings = 10;
    h.backend.EnqueueReport(r);
  }
  for (int i = 0; i < 5; ++i) {
    h.AdvanceWall(1000ms);
    auto t = scanner->Tick();
    ASSERT_TRUE(t.has_value());
  }
  auto snap = scanner->CurrentSnapshot();
  EXPECT_DOUBLE_EQ(snap.rate_multiplier, 1.0);
}

TEST(TtlScannerTest, DiskPressureForcesMaxRate) {
  Harness h;
  auto scanner = h.Build();
  // Even with 0 expired ratio, disk pressure forces max rate.
  h.disk_fraction.store(0.95);
  SweepReport r;
  r.sampled_strings = 10;
  h.backend.EnqueueReport(r);

  h.AdvanceWall(1000ms);
  auto t = scanner->Tick();
  ASSERT_TRUE(t.has_value());

  auto snap = scanner->CurrentSnapshot();
  EXPECT_TRUE(snap.disk_pressure_active);
  EXPECT_EQ(snap.sample_size, h.cfg.max_sample_size);
  EXPECT_EQ(snap.interval, h.cfg.min_interval);
}

TEST(TtlScannerTest, DiskPressureReleasesWithHysteresis) {
  Harness h;
  auto scanner = h.Build();

  // Tick 1: enter pressure at 0.92.
  h.disk_fraction.store(0.92);
  h.backend.EnqueueReport(SweepReport{});
  h.AdvanceWall(1000ms);
  ASSERT_TRUE(scanner->Tick().has_value());
  EXPECT_TRUE(scanner->CurrentSnapshot().disk_pressure_active);

  // Tick 2: 0.88 — above release threshold (0.855), still pressure.
  h.disk_fraction.store(0.88);
  h.backend.EnqueueReport(SweepReport{});
  h.AdvanceWall(1000ms);
  ASSERT_TRUE(scanner->Tick().has_value());
  EXPECT_TRUE(scanner->CurrentSnapshot().disk_pressure_active);

  // Tick 3: 0.80 — below release threshold, exits pressure.
  h.disk_fraction.store(0.80);
  h.backend.EnqueueReport(SweepReport{});
  h.AdvanceWall(1000ms);
  ASSERT_TRUE(scanner->Tick().has_value());
  EXPECT_FALSE(scanner->CurrentSnapshot().disk_pressure_active);
}

TEST(TtlScannerTest, CpuBudgetExtendsIntervalAboveCap) {
  Harness h;
  h.cfg.cpu_ewma_window = 1s;
  h.cfg.max_cpu_fraction = 0.05;
  auto scanner = h.Build();
  // Backend burns 500ms of CPU per call inside Tick, simulating a heavy
  // scanner workload. Combined with 1000ms wall between ticks that is a 50%
  // CPU fraction, ten times the cap.
  h.backend.SetCpuPerCall(500ms, &h.cpu_ns);
  for (int i = 0; i < 3; ++i) {
    h.backend.EnqueueReport(SweepReport{});
    h.AdvanceWall(1000ms);
    ASSERT_TRUE(scanner->Tick().has_value());
  }

  auto snap = scanner->CurrentSnapshot();
  EXPECT_GT(snap.cpu_fraction, h.cfg.max_cpu_fraction);
  EXPECT_GT(snap.interval, h.cfg.base_interval);
}

TEST(TtlScannerTest, DiskPressureBypassesCpuBudget) {
  Harness h;
  h.cfg.cpu_ewma_window = 1s;
  h.cfg.max_cpu_fraction = 0.05;
  auto scanner = h.Build();
  h.disk_fraction.store(0.95);
  h.backend.SetCpuPerCall(500ms, &h.cpu_ns);

  for (int i = 0; i < 3; ++i) {
    h.backend.EnqueueReport(SweepReport{});
    h.AdvanceWall(1000ms);
    ASSERT_TRUE(scanner->Tick().has_value());
  }

  auto snap = scanner->CurrentSnapshot();
  EXPECT_TRUE(snap.disk_pressure_active);
  EXPECT_EQ(snap.interval, h.cfg.min_interval);
}

TEST(TtlScannerTest, ManualTickWithoutStartDoesNotSpawnThread) {
  Harness h;
  auto scanner = h.Build();
  // No Start(); Tick() should still work in manual mode.
  h.backend.EnqueueReport(SweepReport{});
  h.AdvanceWall(1000ms);
  auto t = scanner->Tick();
  ASSERT_TRUE(t.has_value());
  EXPECT_EQ(h.backend.calls(), 1U);
}

TEST(TtlScannerTest, StopIsIdempotent) {
  Harness h;
  auto scanner = h.Build();
  scanner->Stop();
  scanner->Stop();  // Must not deadlock or assert.
}

TEST(TtlScannerTest, DisabledScannerSkipsTicks) {
  Harness h;
  h.cfg.enabled = false;
  auto scanner = h.Build();
  h.backend.EnqueueReport(SweepReport{});
  auto t = scanner->Tick();
  ASSERT_TRUE(t.has_value());
  EXPECT_EQ(h.backend.calls(), 0U);
}

TEST(TtlScannerTest, TotalCountersAccumulate) {
  Harness h;
  auto scanner = h.Build();
  for (int i = 0; i < 3; ++i) {
    SweepReport r;
    r.sampled_strings = 10;
    r.with_ttl_strings = 8;
    r.expired_strings = 4;
    r.deleted_strings = 3;
    r.conflicts_strings = 1;
    h.backend.EnqueueReport(r);
    h.AdvanceWall(1000ms);
    ASSERT_TRUE(scanner->Tick().has_value());
  }
  auto snap = scanner->CurrentSnapshot();
  EXPECT_EQ(snap.total_ticks, 3U);
  EXPECT_EQ(snap.total_sampled, 30U);
  EXPECT_EQ(snap.total_with_ttl, 24U);
  EXPECT_EQ(snap.total_expired, 12U);
  EXPECT_EQ(snap.total_deleted, 9U);
  EXPECT_EQ(snap.total_conflicts, 3U);
}

TEST(TtlScannerTest, MetricsAreEmitted) {
  Harness h;
  auto scanner = h.Build();
  SweepReport r;
  r.sampled_strings = 5;
  r.expired_strings = 2;
  r.deleted_strings = 2;
  h.backend.EnqueueReport(r);
  h.AdvanceWall(1000ms);
  ASSERT_TRUE(scanner->Tick().has_value());

  auto samples = metrics::testing::GetCounterValue(metrics::names::kColdTtlSamplesTotal,
                                                   metrics::TtlSubject::kString);
  ASSERT_TRUE(samples.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_DOUBLE_EQ(samples.value(), 5.0);

  auto deleted = metrics::testing::GetCounterValue(metrics::names::kColdTtlDeletedTotal,
                                                   metrics::TtlSubject::kString);
  ASSERT_TRUE(deleted.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_DOUBLE_EQ(deleted.value(), 2.0);
}

TEST(TtlScannerTest, RateIsClampedToMax) {
  Harness h;
  auto scanner = h.Build();
  for (int i = 0; i < 100; ++i) {
    SweepReport r;
    r.sampled_strings = 10;
    r.expired_strings = 10;  // 100% expired — way above high threshold
    h.backend.EnqueueReport(r);
    h.AdvanceWall(1000ms);
    ASSERT_TRUE(scanner->Tick().has_value());
  }
  auto snap = scanner->CurrentSnapshot();
  EXPECT_LE(snap.sample_size, h.cfg.max_sample_size);
  EXPECT_GE(snap.interval, h.cfg.min_interval);
}

TEST(TtlScannerTest, RateIsClampedToMin) {
  Harness h;
  auto scanner = h.Build();
  for (int i = 0; i < 100; ++i) {
    SweepReport r;
    r.sampled_strings = 100;
    r.expired_strings = 0;
    h.backend.EnqueueReport(r);
    h.AdvanceWall(1000ms);
    ASSERT_TRUE(scanner->Tick().has_value());
  }
  auto snap = scanner->CurrentSnapshot();
  EXPECT_GE(snap.sample_size, h.cfg.min_sample_size);
  EXPECT_LE(snap.interval, h.cfg.max_interval);
}

}  // namespace
}  // namespace abyss::cold
