#include "queue_append.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"
#include "latch.h"
#include "shard_stream.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;
using abyss::testing::Append;
using abyss::testing::TimedOut;
using abyss::testing::WaitDurable;

constexpr auto kProcess = core::Durability::kProcessCrash;
constexpr auto kPower = core::Durability::kPowerLoss;

core::QueueEntry Write(const std::string& key) {
  return core::QueueEntry{.payload =
                              core::entry::Write{.cmd = core::RespCommand{{"SET", key, "v"}}}};
}

class QueueAppendTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override {
    release_.Open();
    queue_.reset();
    metrics::testing::Reset();
  }

  void Open(core::Durability durability, uint64_t window_bytes = uint64_t{64} << 20) {
    auto opened = WalQueue::Open(WalConfig{
        .wal_path = dir_.String(),
        .segment_size_bytes = std::size_t{1} << 20,
        .shard_count = 1,
        .durability = durability,
        .durability_window_bytes = window_bytes,
        .durability_window = 60s,
        .min_retention = 0s,
    });
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  // Until release_ opens, no flush syncs, so neither the power end
  // moves nor the window drains.
  void HoldFlushes() {
    queue_->SetFlushHookForTesting([this](uint32_t) {
      release_.Wait(30s);
      return core::Result<void>{};
    });
  }

  std::size_t Waiters(core::Durability durability) const {
    return queue_->StreamForTesting(0).DurableWaitersForTesting(durability);
  }

  // Bounded: until `count` waiters are linked at `durability`.
  bool WaitersReach(core::Durability durability, std::size_t count) const {
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (Waiters(durability) < count) {
      if (std::chrono::steady_clock::now() >= until) return false;
      std::this_thread::sleep_for(1ms);
    }
    return true;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"queue_append"};
  abyss::testing::Latch release_;
  std::unique_ptr<WalQueue> queue_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(QueueAppendTest, AWaitCompletesOnceItsClassEndPassesTheSeq) {
  Open(kPower);
  HoldFlushes();
  const auto seq = Append(*queue_, 0, Write("k"));
  ASSERT_TRUE(seq.has_value()) << seq.error().message();
  EXPECT_TRUE(WaitDurable(*queue_, 0, *seq, kProcess, 0ms).has_value()) << "published by Complete";

  WalQueue& queue = *queue_;
  auto waiting = std::async(std::launch::async, [&queue, seq = *seq] {
    auto waited = WaitDurable(queue, 0, seq, kPower, 10s);
    return std::pair{std::move(waited), queue.DurableEnd(0, kPower).value()};
  });
  ASSERT_TRUE(WaitersReach(kPower, 1));
  EXPECT_EQ(waiting.wait_for(20ms), std::future_status::timeout);
  EXPECT_EQ(queue_->DurableEnd(0, kPower).value(), *seq);

  release_.Open();
  ASSERT_EQ(waiting.wait_for(10s), std::future_status::ready);
  const auto [waited, end] = waiting.get();
  ASSERT_TRUE(waited.has_value()) << waited.error().message();
  EXPECT_GT(end, *seq);
  EXPECT_EQ(Waiters(kPower), 0U);
}

TEST_F(QueueAppendTest, ATimedOutWaitLeavesNoWaiterRegistered) {
  Open(kPower);
  HoldFlushes();
  const auto seq = Append(*queue_, 0, Write("k"));
  ASSERT_TRUE(seq.has_value()) << seq.error().message();

  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(TimedOut(WaitDurable(*queue_, 0, *seq, kPower, 20ms)));
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
  EXPECT_EQ(Waiters(kPower), 0U);
  EXPECT_TRUE(TimedOut(WaitDurable(*queue_, 0, *seq + 1, kProcess, 0ms)));
  EXPECT_EQ(Waiters(kProcess), 0U);

  release_.Open();
  const auto waited = WaitDurable(*queue_, 0, *seq, kPower);
  EXPECT_TRUE(waited.has_value()) << waited.error().message();
}

TEST_F(QueueAppendTest, StoppingFailsAWaitUnavailable) {
  Open(kPower);
  WalQueue& queue = *queue_;
  const auto wait_at = [&queue](core::Durability durability) {
    return std::async(std::launch::async, [&queue, durability] {
      return WaitDurable(queue, 0, core::kFirstSeq + 5, durability, 30s);
    });
  };
  auto published = wait_at(kProcess);
  auto power = wait_at(kPower);
  ASSERT_TRUE(WaitersReach(kProcess, 1));
  ASSERT_TRUE(WaitersReach(kPower, 1));

  queue_.reset();
  for (auto* waiting : {&published, &power}) {
    ASSERT_EQ(waiting->wait_for(10s), std::future_status::ready);
    const auto waited = waiting->get();
    ASSERT_FALSE(waited.has_value());
    EXPECT_EQ(waited.error().code(), core::ErrorCode::kUnavailable);
  }
}

// A full window holds an append until a flush frees it; at its
// deadline it is rejected and counted, and a deadline already past
// waits for nothing.
TEST_F(QueueAppendTest, AnAppendWaitsForTheWindow) {
  Open(kProcess, /*window_bytes=*/1);
  HoldFlushes();
  ASSERT_TRUE(Append(*queue_, 0, Write("first")).has_value());

  const auto refused = Append(*queue_, 0, Write("now"), core::SteadyClock::now());
  ASSERT_FALSE(refused.has_value());
  EXPECT_EQ(refused.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureRejectionsTotal)
                .value_or(0),
            0.0);

  const auto start = std::chrono::steady_clock::now();
  const auto rejected = Append(*queue_, 0, Write("late"), start + 30ms);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 30ms);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureRejectionsTotal),
            1.0);

  WalQueue& queue = *queue_;
  auto admitted = std::async(std::launch::async, [&queue] {
    return Append(queue, 0, Write("second"), core::SteadyClock::now() + 10s);
  });
  EXPECT_EQ(admitted.wait_for(20ms), std::future_status::timeout);
  release_.Open();
  ASSERT_EQ(admitted.wait_for(10s), std::future_status::ready);
  const auto second = admitted.get();
  ASSERT_TRUE(second.has_value()) << second.error().message();
  EXPECT_EQ(*second, core::kFirstSeq + 1);
}

}  // namespace
}  // namespace abyss::queue
