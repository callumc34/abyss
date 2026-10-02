#include "run_loop.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <thread>

namespace abyss::perf {
namespace {

TEST(RunLoopTest, ClosedLoopExercisesAllOpsAcrossWorkers) {
  RunLoopConfig cfg;
  cfg.workers = 2;
  cfg.duration = std::chrono::seconds{1};
  cfg.warmup = std::chrono::seconds{0};
  cfg.target_rate_ops = 0;
  cfg.key_count = 100;
  cfg.value_size_bytes = 16;
  cfg.mix.weights["A"] = 0.5;
  cfg.mix.weights["B"] = 0.5;

  std::atomic<int> worker_seen_a{0};
  std::atomic<int> worker_seen_b{0};
  OpFn op = [&](int /*worker*/, std::string_view name, uint64_t /*key*/) {
    if (name == "A") {
      worker_seen_a.fetch_add(1, std::memory_order_relaxed);
    } else if (name == "B") {
      worker_seen_b.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  };

  const auto result = RunLoop(cfg, op);
  EXPECT_GT(result.per_op_counts.at("A"), 0U);
  EXPECT_GT(result.per_op_counts.at("B"), 0U);
  EXPECT_GT(result.per_op_histograms.at("A").Count(), 0);
  EXPECT_GT(result.per_op_histograms.at("B").Count(), 0);
}

// Pins coordinated-omission accounting (ADP-013): open loop issues
// every scheduled op, late if necessary, and measures each from its own
// intended send time, so one stall is charged once to every op it
// delayed. The lower bounds can only loosen under machine load.
TEST(CoordinatedOmissionTest, StallIsChargedToEveryDelayedOp) {
  constexpr int64_t kIntervalNs = 1'000'000;  // 1000 ops/s/worker → 1ms schedule
  constexpr int kStallIntervals = 50;
  constexpr auto kStall = std::chrono::milliseconds{kStallIntervals};
  constexpr int64_t kStallNs = kStallIntervals * kIntervalNs;

  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.target_rate_ops = 1000;
  cfg.key_count = 10;
  cfg.mix.weights["X"] = 1.0;

  std::atomic<bool> stalled{false};
  OpFn op = [&](int /*w*/, std::string_view /*n*/, uint64_t /*k*/) {
    // One deterministic stall spanning kStallIntervals scheduled send slots.
    if (!stalled.exchange(true, std::memory_order_relaxed)) {
      std::this_thread::sleep_for(kStall);
    }
    return true;
  };
  const auto result = RunLoop(cfg, op);

  const uint64_t ops = result.per_op_counts.at("X");
  const auto& hist = result.per_op_histograms.at("X");

  // Every slot of the second is issued, and sampled once.
  EXPECT_EQ(ops, 1000U);
  EXPECT_EQ(hist.Count(), static_cast<int64_t>(ops));
  EXPECT_EQ(result.send_lag.Count(), static_cast<int64_t>(ops));
  EXPECT_GE(hist.MaxNs(), kStallNs);

  // The ~49 ops behind the stall drain the backlog with waits falling
  // from ~49ms to ~1ms. Measured from the actual send, only the stalled
  // op would exceed an interval.
  EXPECT_GE(hist.PercentileNs(99.0), kStallNs * 3 / 5);
  EXPECT_GE(hist.PercentileNs(97.0), kStallNs / 5);
  EXPECT_LT(hist.PercentileNs(97.0), hist.PercentileNs(99.0));

  // The rest of the schedule keeps its unstalled latency.
  EXPECT_LT(hist.PercentileNs(50.0), kIntervalNs);
}

TEST(RunLoopTest, ResultExposesMeasuredDuration) {
  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.warmup = std::chrono::seconds{1};
  cfg.target_rate_ops = 0;
  cfg.key_count = 10;
  cfg.mix.weights["GET"] = 1.0;

  OpFn op = [&](int, std::string_view, uint64_t) { return true; };
  const auto result = RunLoop(cfg, op);
  EXPECT_NEAR(result.measured_duration.count(), 1'000'000'000, 200'000'000);
  EXPECT_FALSE(result.open_loop);
  EXPECT_EQ(result.send_lag.Count(), 0);
}

TEST(RunLoopTest, FailedOpsAreCountedNotTimed) {
  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.key_count = 10;
  cfg.mix.weights["fail"] = 0.5;
  cfg.mix.weights["ok"] = 0.5;

  OpFn op = [&](int, std::string_view name, uint64_t) { return name == "ok"; };
  const auto result = RunLoop(cfg, op);

  EXPECT_GT(result.per_op_errors.at("fail"), 0U);
  EXPECT_EQ(result.per_op_counts.at("fail"), 0U);
  EXPECT_EQ(result.per_op_histograms.at("fail").Count(), 0);
  EXPECT_EQ(result.per_op_errors.at("ok"), 0U);
  EXPECT_EQ(result.per_op_histograms.at("ok").Count(),
            static_cast<int64_t>(result.per_op_counts.at("ok")));
}

// An op scheduled inside the warmup stays out of the measurement even
// when a stall makes it complete after the warmup ends.
TEST(RunLoopTest, WarmupClassifiesByIntendedSendTime) {
  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.warmup = std::chrono::seconds{1};
  cfg.duration = std::chrono::seconds{1};
  cfg.target_rate_ops = 1000;
  cfg.key_count = 10;
  cfg.mix.weights["X"] = 1.0;

  // Slot 995 is 5ms before the warmup ends; it stalls 50ms past it.
  std::atomic<int> calls{0};
  OpFn op = [&](int, std::string_view, uint64_t) {
    if (calls.fetch_add(1, std::memory_order_relaxed) == 995) {
      std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    return true;
  };
  const auto result = RunLoop(cfg, op);

  EXPECT_EQ(result.per_op_counts.at("X"), 1000U);
  EXPECT_EQ(result.per_op_histograms.at("X").Count(), 1000);
}

// 120 req/s over 80 connections: 40 take 2/s and 40 take 1/s.
TEST(WorkerRateTest, SplitsTheTotalExactly) {
  uint64_t total = 0;
  for (size_t w = 0; w < 80; ++w) {
    EXPECT_EQ(WorkerRate(120, 80, w), w < 40 ? 2U : 1U);
    total += WorkerRate(120, 80, w);
  }
  EXPECT_EQ(total, 120U);
  EXPECT_EQ(WorkerRate(100'000, 80, 79), 1250U);
  EXPECT_EQ(WorkerRate(3, 80, 2), 1U);
  EXPECT_EQ(WorkerRate(3, 80, 3), 0U);
}

TEST(RunLoopConfigFromWorkloadTest, CarriesTheTotalTargetRate) {
  WorkloadConfig wl;
  wl.workers = 4;
  wl.duration = std::chrono::seconds{30};
  wl.warmup = std::chrono::seconds{5};
  wl.target_rate_ops = 100'000;
  wl.key_count = 1000;
  wl.value_size_bytes = 64;
  wl.mix.weights["GET"] = 1.0;

  const auto rl = RunLoopConfigFromWorkload(wl);
  EXPECT_EQ(rl.workers, 4);
  EXPECT_EQ(rl.target_rate_ops, 100'000U);
  EXPECT_EQ(rl.duration.count(), 30);
  EXPECT_EQ(rl.warmup.count(), 5);
}

}  // namespace
}  // namespace abyss::perf
