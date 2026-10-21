#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <span>

#include "abyss/core/completion.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"
#include "thread_allocs.h"

namespace abyss::core {
namespace {

class CountingSink final : public CompletionSink {
 public:
  void Post(std::span<const CompletionEvent> events) noexcept override {
    posts_.fetch_add(1);
    events_.fetch_add(events.size());
  }
  size_t posts() const { return posts_.load(); }
  size_t events() const { return events_.load(); }

 private:
  std::atomic<size_t> posts_{0};
  std::atomic<size_t> events_{0};
};

class CountingWaiter final : public Waiter {
 public:
  void Wake(Result<void> /*result*/) noexcept override { ++wakes_; }
  int wakes() const { return wakes_; }

 private:
  int wakes_ = 0;
};

constexpr SequenceId kPerRound = 48;

// The notifier groups by sink in reused per-thread scratch: once warm,
// a round of Register and Advance allocates nothing.
TEST(WaitAllocTest, WarmRegisterAndAdvanceAllocateNothing) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  std::array<CountingSink, 3> sinks;
  for (SequenceId round = 0; round < 4; ++round) {
    std::deque<SeqWaiterNode> nodes;
    const SequenceId base = round * kPerRound;
    for (SequenceId i = 0; i < kPerRound; ++i) {
      nodes.emplace_back(base + i,
                         Completion{.sink = &sinks.at(i % 3), .client_id = 1, .slot_seq = i});
    }
    size_t linked = 0;
    const std::size_t before = abyss::testing::ThreadAllocs();
    for (auto& node : nodes) linked += waiters.Register(node, end).has_value() ? 0 : 1;
    end.store(base + kPerRound, std::memory_order_seq_cst);
    waiters.Advance(base + kPerRound);
    const std::size_t allocs = abyss::testing::ThreadAllocs() - before;
    ASSERT_EQ(linked, kPerRound);
    if (round > 0) {
      EXPECT_EQ(allocs, 0U) << "round " << round;
    }
    ASSERT_EQ(waiters.size(), 0U);
  }
  for (const auto& sink : sinks) {
    EXPECT_EQ(sink.posts(), 4U);
    EXPECT_EQ(sink.events(), 4 * kPerRound / 3);
  }
}

TEST(WaitAllocTest, RegisterAndNotifyAllocateNothing) {
  WaitList list;
  std::array<CountingWaiter, WaitList::kWakeBatch + 6> waiters;
  const auto never = [] { return false; };
  size_t linked = 0;
  const std::size_t before = abyss::testing::ThreadAllocs();
  for (auto& waiter : waiters) linked += list.Register(waiter, never).has_value() ? 0 : 1;
  list.Notify();
  while (list.size() > 0) list.HandOff();
  const std::size_t allocs = abyss::testing::ThreadAllocs() - before;
  ASSERT_EQ(linked, waiters.size());
  EXPECT_EQ(allocs, 0U);
  EXPECT_EQ(list.size(), 0U);
  for (const auto& waiter : waiters) EXPECT_EQ(waiter.wakes(), 1);
}

}  // namespace
}  // namespace abyss::core
