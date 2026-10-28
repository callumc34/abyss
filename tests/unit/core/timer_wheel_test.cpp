#include "abyss/core/timer_wheel.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "abyss/core/types.h"

namespace abyss::core {
namespace {

using namespace std::chrono_literals;

class TimerWheelTest : public ::testing::Test {
 protected:
  std::vector<uint64_t> Expire(std::chrono::microseconds at) {
    std::vector<uint64_t> fired;
    wheel_.Expire(origin_ + at, fired);
    return fired;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  SteadyTime origin_ = SteadyClock::now();
  // Eight one-millisecond buckets: a round every 8 ms.
  TimerWheel wheel_{1ms, 8, origin_};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(TimerWheelTest, ATimerNeverFiresBeforeItsDeadline) {
  TimerNode node(1);
  wheel_.Schedule(node, origin_ + 5500us);
  EXPECT_TRUE(Expire(5ms).empty());
  EXPECT_TRUE(Expire(5999us).empty());
  EXPECT_EQ(Expire(6ms), std::vector<uint64_t>{1});
  EXPECT_FALSE(node.armed());
  EXPECT_EQ(wheel_.size(), 0U);
}

TEST_F(TimerWheelTest, ATimerOfALaterRoundStaysInItsBucket) {
  TimerNode near(1);
  TimerNode far(2);
  wheel_.Schedule(near, origin_ + 4ms);
  wheel_.Schedule(far, origin_ + 20ms);
  EXPECT_EQ(Expire(12ms), std::vector<uint64_t>{1});
  EXPECT_TRUE(far.armed()) << "its bucket came round once already";
  EXPECT_TRUE(Expire(19ms).empty());
  EXPECT_EQ(Expire(20ms), std::vector<uint64_t>{2});
}

TEST_F(TimerWheelTest, AJumpPastAWholeRoundFiresEverythingDue) {
  std::vector<std::unique_ptr<TimerNode>> nodes;
  for (uint64_t id = 0; id < 20; ++id) {
    nodes.push_back(std::make_unique<TimerNode>(id));
    wheel_.Schedule(*nodes.back(), origin_ + std::chrono::milliseconds(id + 1));
  }
  EXPECT_EQ(Expire(100ms).size(), 20U);
  EXPECT_EQ(wheel_.size(), 0U);
}

TEST_F(TimerWheelTest, CancelAndFreeingUnlink) {
  TimerNode cancelled(1);
  wheel_.Schedule(cancelled, origin_ + 3ms);
  {
    TimerNode freed(2);
    wheel_.Schedule(freed, origin_ + 3ms);
    EXPECT_EQ(wheel_.size(), 2U);
  }
  EXPECT_EQ(wheel_.size(), 1U);
  wheel_.Cancel(cancelled);
  EXPECT_FALSE(cancelled.armed());
  EXPECT_TRUE(Expire(10ms).empty());
}

TEST_F(TimerWheelTest, RescheduleMovesATimer) {
  TimerNode node(1);
  wheel_.Schedule(node, origin_ + 3ms);
  wheel_.Schedule(node, origin_ + 6ms);
  EXPECT_EQ(wheel_.size(), 1U);
  EXPECT_TRUE(Expire(5ms).empty());
  EXPECT_EQ(Expire(6ms), std::vector<uint64_t>{1});
}

TEST_F(TimerWheelTest, NextDueIsTheNextOccupiedBucket) {
  EXPECT_FALSE(wheel_.NextDue().has_value()) << "an idle wheel never wakes its poller";
  TimerNode node(1);
  wheel_.Schedule(node, origin_ + 2500us);
  EXPECT_EQ(wheel_.NextDue(), std::optional<SteadyTime>(origin_ + 3ms));
  (void)Expire(1ms);
  EXPECT_EQ(wheel_.NextDue(), std::optional<SteadyTime>(origin_ + 3ms));
  // A deadline already past fires at the next tick.
  TimerNode late(2);
  wheel_.Schedule(late, origin_);
  EXPECT_EQ(wheel_.NextDue(), std::optional<SteadyTime>(origin_ + 2ms));
}

TEST_F(TimerWheelTest, NextDueWrapsRoundTheWheel) {
  (void)Expire(6ms);
  TimerNode node(1);
  // Tick 9 is bucket 1, behind the current position.
  wheel_.Schedule(node, origin_ + 9ms);
  EXPECT_EQ(wheel_.NextDue(), std::optional<SteadyTime>(origin_ + 9ms));
}

}  // namespace
}  // namespace abyss::core
