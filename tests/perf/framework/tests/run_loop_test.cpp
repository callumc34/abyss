#include "run_loop.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <string_view>

namespace abyss::perf {
namespace {

TEST(RunLoopTest, ClosedLoopExercisesAllOpsAcrossWorkers) {
  RunLoopConfig cfg;
  cfg.workers = 2;
  cfg.duration = std::chrono::seconds{1};
  cfg.warmup = std::chrono::seconds{0};
  cfg.target_rate_ops_per_worker = 0;
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
  };

  const auto result = RunLoop(cfg, op);
  EXPECT_GT(result.per_op_counts.at("A"), 0U);
  EXPECT_GT(result.per_op_counts.at("B"), 0U);
  EXPECT_GT(result.per_op_histograms.at("A").Count(), 0);
  EXPECT_GT(result.per_op_histograms.at("B").Count(), 0);
}

TEST(RunLoopTest, OpenLoopRecordsCorrectedLatency) {
  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.target_rate_ops_per_worker = 1000;  // 1ms interval
  cfg.key_count = 10;
  cfg.mix.weights["X"] = 1.0;

  OpFn op = [&](int /*w*/, std::string_view /*n*/, uint64_t /*k*/) {
    // No-op: stay well below the 1ms scheduled interval.
  };
  const auto result = RunLoop(cfg, op);
  EXPECT_GT(result.per_op_counts.at("X"), 100U);
  // With no stall, p99 stays below ~10x the scheduled interval. A tighter
  // bound flakes under parallel-test load; the load-bearing CO behaviour is
  // covered by CoordinatedOmissionTest.StallInjectsSyntheticTailSamples.
  EXPECT_LT(result.per_op_histograms.at("X").PercentileNs(99.0), 10'000'000);
}

TEST(RunLoopTest, ResultExposesMeasuredDuration) {
  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.warmup = std::chrono::seconds{1};
  cfg.target_rate_ops_per_worker = 0;
  cfg.key_count = 10;
  cfg.mix.weights["GET"] = 1.0;

  OpFn op = [&](int, std::string_view, uint64_t) {};
  const auto result = RunLoop(cfg, op);
  EXPECT_NEAR(result.measured_duration.count(), 1'000'000'000, 200'000'000);
}

TEST(RunLoopConfigFromWorkloadTest, DividesTargetRateAcrossWorkers) {
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
  EXPECT_EQ(rl.target_rate_ops_per_worker, 25'000U);
  EXPECT_EQ(rl.duration.count(), 30);
  EXPECT_EQ(rl.warmup.count(), 5);
}

}  // namespace
}  // namespace abyss::perf
