#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <string_view>
#include <thread>

#include "run_loop.h"

namespace abyss::perf {
namespace {

using Clock = std::chrono::steady_clock;
using Await = PipelinedConnection::Await;

// Answers in send order, each reply `service` after its flush; the
// reply to request `stall_at` is held back by `stall`.
class FakeConnection final : public PipelinedConnection {
 public:
  FakeConnection(std::chrono::nanoseconds service, uint64_t stall_at,
                 std::chrono::nanoseconds stall)
      : service_(service), stall_at_(stall_at), stall_(stall) {}

  bool Send(std::string_view /*op_name*/, uint64_t /*key_index*/) override {
    ++queued_;
    return true;
  }

  bool Flush() override {
    const auto now = Clock::now();
    for (; queued_ > 0; --queued_) {
      auto ready = now + (sent_ == stall_at_ ? stall_ : service_);
      if (!ready_.empty()) ready = std::max(ready, ready_.back());
      ready_.push_back(ready);
      ++sent_;
      max_in_flight_ = std::max(max_in_flight_, ready_.size());
    }
    return true;
  }

  Await AwaitReply(Clock::time_point deadline) override {
    if (ready_.empty() || completed_ == fail_after_) return Await::kFailed;
    const auto ready = ready_.front();
    if (deadline < ready) {
      std::this_thread::sleep_until(deadline);
      return Await::kTimeout;
    }
    std::this_thread::sleep_until(ready);
    ready_.pop_front();
    const bool error = error_odd_ && completed_ % 2 == 1;
    ++completed_;
    return error ? Await::kError : Await::kReply;
  }

  void FailAfter(uint64_t replies) { fail_after_ = replies; }
  void ErrorOnOddReplies() { error_odd_ = true; }
  uint64_t sent() const { return sent_; }
  uint64_t completed() const { return completed_; }
  size_t max_in_flight() const { return max_in_flight_; }

 private:
  std::chrono::nanoseconds service_;
  uint64_t stall_at_;
  std::chrono::nanoseconds stall_;
  uint64_t fail_after_ = UINT64_MAX;
  bool error_odd_ = false;
  uint64_t queued_ = 0;
  uint64_t sent_ = 0;
  uint64_t completed_ = 0;
  size_t max_in_flight_ = 0;
  std::deque<Clock::time_point> ready_;
};

RunLoopResult RunOn(const RunLoopConfig& cfg, int depth, FakeConnection& conn,
                    Arrival arrival = Arrival::kSteady) {
  const std::array<PipelinedConnection*, 1> conns{&conn};
  return RunPipelinedLoop(cfg, depth, arrival, conns);
}

RunLoopConfig OneSecondConfig(uint64_t rate_per_worker) {
  RunLoopConfig cfg;
  cfg.workers = 1;
  cfg.duration = std::chrono::seconds{1};
  cfg.target_rate_ops = rate_per_worker;
  cfg.key_count = 10;
  cfg.mix.weights["X"] = 1.0;
  return cfg;
}

// Pins coordinated-omission accounting for depth > 1: during a stall
// the window fills, slots falling due meanwhile are sent late, and each
// is measured from its own intended send time. Every bound below can
// only loosen under machine load.
TEST(PipelinedRunLoopTest, OpenLoopStallIsChargedToEverySlotItHeldBack) {
  constexpr int kDepth = 4;
  constexpr uint64_t kRate = 1000;  // 1ms schedule
  constexpr int64_t kSlots = 1000;  // one second of schedule
  constexpr auto kStall = std::chrono::milliseconds{50};
  constexpr int64_t kStallNs = 50'000'000;

  FakeConnection conn{std::chrono::microseconds{10}, 10, kStall};
  const auto result = RunOn(OneSecondConfig(kRate), kDepth, conn);
  const auto& hist = result.per_op_histograms.at("X");

  // One sample per scheduled slot: none omitted, none synthesised twice.
  EXPECT_EQ(conn.sent(), static_cast<uint64_t>(kSlots));
  EXPECT_EQ(conn.completed(), conn.sent());
  EXPECT_EQ(hist.Count(), kSlots);
  EXPECT_EQ(result.per_op_counts.at("X"), static_cast<uint64_t>(kSlots));

  EXPECT_EQ(conn.max_in_flight(), static_cast<size_t>(kDepth));
  EXPECT_GE(hist.MaxNs(), kStallNs);

  // ~30 held-back slots waited >= 20ms. From the actual send only the 4
  // in-window requests would, leaving p98 near the service time.
  EXPECT_GE(hist.PercentileNs(98.0), kStallNs * 2 / 5);
}

TEST(PipelinedRunLoopTest, ClosedLoopKeepsTheWindowFull) {
  constexpr int kDepth = 8;
  FakeConnection conn{std::chrono::microseconds{200}, UINT64_MAX, {}};
  const auto result = RunOn(OneSecondConfig(0), kDepth, conn);

  EXPECT_EQ(conn.max_in_flight(), static_cast<size_t>(kDepth));
  EXPECT_EQ(conn.completed(), conn.sent());
  EXPECT_GT(result.per_op_counts.at("X"), 0U);
  EXPECT_LE(result.per_op_counts.at("X"), conn.completed());
}

TEST(PipelinedRunLoopTest, FailedConnectionEndsItsWorker) {
  FakeConnection conn{std::chrono::microseconds{10}, UINT64_MAX, {}};
  conn.FailAfter(5);
  const auto result = RunOn(OneSecondConfig(0), 4, conn);

  EXPECT_EQ(conn.completed(), 5U);
  EXPECT_LE(result.per_op_counts.at("X"), 5U);
}

TEST(PipelinedRunLoopTest, ErrorRepliesAreCountedNotTimed) {
  FakeConnection conn{std::chrono::microseconds{50}, UINT64_MAX, {}};
  conn.ErrorOnOddReplies();
  const auto result = RunOn(OneSecondConfig(0), 4, conn);

  const auto ok = result.per_op_counts.at("X");
  const auto errors = result.per_op_errors.at("X");
  EXPECT_GT(errors, 0U);
  EXPECT_EQ(result.per_op_histograms.at("X").Count(), static_cast<int64_t>(ok));
  EXPECT_LE(ok > errors ? ok - errors : errors - ok, 1U);
}

// A request scheduled inside the warmup stays out of the measurement
// even when a stall makes it complete after the warmup ends.
TEST(PipelinedRunLoopTest, WarmupClassifiesByIntendedSendTime) {
  auto cfg = OneSecondConfig(1000);
  cfg.warmup = std::chrono::seconds{1};
  FakeConnection conn{std::chrono::microseconds{10}, 995, std::chrono::milliseconds{50}};
  const auto result = RunOn(cfg, 4, conn);

  EXPECT_EQ(conn.sent(), 2000U);
  EXPECT_EQ(result.per_op_counts.at("X"), 1000U);
  EXPECT_EQ(result.per_op_histograms.at("X").Count(), 1000);
}

// target_rate_ops counts requests, so depth-16 bursts come every 10ms
// at 1600 requests/s; send lag is sampled once per burst.
TEST(PipelinedRunLoopTest, BurstArrivalOffersTheTargetRequestRate) {
  constexpr int kDepth = 16;
  FakeConnection conn{std::chrono::microseconds{10}, UINT64_MAX, {}};
  const auto result = RunOn(OneSecondConfig(1600), kDepth, conn, Arrival::kBurst);

  EXPECT_EQ(conn.sent(), 1600U);
  EXPECT_EQ(result.per_op_counts.at("X"), 1600U);
  EXPECT_EQ(conn.max_in_flight(), static_cast<size_t>(kDepth));
  EXPECT_EQ(result.send_lag.Count(), 100);
}

}  // namespace
}  // namespace abyss::perf
