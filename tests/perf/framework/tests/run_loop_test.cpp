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

// Pins the wiring between the open-loop scheduler and the coordinated-omission
// corrector: RunLoop must measure from the INTENDED send time and must feed the
// scheduler's expected interval to Histogram::RecordCorrected. (The corrector
// itself is pinned by CoordinatedOmissionTest.StallInjectsSyntheticTailSamples;
// this test owns the wiring, per ADP-013 §Coordinated omission.)
//
// Every assertion below is a LOWER bound on a quantity that a stall can only
// increase, so machine load cannot make the test fail — only a regression in
// the harness can. An upper bound on a wall-clock p99 cannot have that
// property: under parallel-test load the scheduler overshoot is unbounded, and
// the corrector faithfully amplifies it. A no-stall p99 bound was also blind to
// the thing this test is named for, since with no missed slot RecordCorrected
// and Record are identical.
TEST(RunLoopTest, OpenLoopRecordsCorrectedLatency) {
  constexpr int64_t kIntervalNs = 1'000'000;  // 1000 ops/s/worker → 1ms schedule
  constexpr int kStallIntervals = 50;
  constexpr auto kStall = std::chrono::milliseconds{kStallIntervals};
  constexpr int64_t kStallNs = kStallIntervals * kIntervalNs;

  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.target_rate_ops_per_worker = 1000;
  cfg.key_count = 10;
  cfg.mix.weights["X"] = 1.0;

  std::atomic<bool> stalled{false};
  OpFn op = [&](int /*w*/, std::string_view /*n*/, uint64_t /*k*/) {
    // One deterministic stall spanning kStallIntervals scheduled send slots.
    if (!stalled.exchange(true, std::memory_order_relaxed)) {
      std::this_thread::sleep_for(kStall);
    }
  };
  const auto result = RunLoop(cfg, op);

  const uint64_t ops = result.per_op_counts.at("X");
  const auto& hist = result.per_op_histograms.at("X");
  EXPECT_GT(ops, 100U);

  // The stalled op alone must contribute kStallIntervals-1 synthetic samples
  // (one per send slot it missed). Record() would leave Count() == ops exactly,
  // so this fires if the open-loop path stops correcting or is handed an
  // interval other than the scheduler's.
  EXPECT_GE(hist.Count(), static_cast<int64_t>(ops) + kStallIntervals - 1)
      << "count = " << hist.Count() << " for " << ops
      << " ops; coordinated-omission correction is missing or mis-parameterised";

  // Measuring from the intended send time preserves the full stall; measuring
  // from the actual send time would clip it to the ops that were in flight.
  EXPECT_GE(hist.MaxNs(), kStallNs);

  // ADP-013: the reported tail must reflect the stall.
  EXPECT_GT(hist.PercentileNs(99.0), kIntervalNs);
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
