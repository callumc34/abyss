#include "abyss/consumer/flush_strategy.h"

#include <gtest/gtest.h>

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;

class FlushStrategyTest : public ::testing::Test {
 protected:
  FlushStrategy strategy_;

  BufferEntry MakeEntry(core::SteadyTime first_seen, core::SteadyTime last_modified) {
    return BufferEntry{
        .key = "k",
        .state = {},
        .first_seen = first_seen,
        .last_modified = last_modified,
    };
  }
};

TEST_F(FlushStrategyTest, QuietWindowDominatesWhenEvictionIsLong) {
  auto t0 = core::SteadyTime{100s};
  auto entry = MakeEntry(t0, t0 + 10s);

  auto flush = strategy_.NextFlushTime(entry, 3600s);
  auto quiet_deadline = entry.last_modified + 30s;
  EXPECT_EQ(flush, quiet_deadline);
}

TEST_F(FlushStrategyTest, EvictionDeadlineDominatesWhenQuietIsLong) {
  auto t0 = core::SteadyTime{100s};
  auto entry = MakeEntry(t0, t0 + 50s);

  auto flush = strategy_.NextFlushTime(entry, 60s);
  auto eviction_deadline = entry.first_seen + 60s - 300s;
  EXPECT_EQ(flush, eviction_deadline);
}

TEST_F(FlushStrategyTest, VeryShortEvictionDeadlineInPast) {
  auto t0 = core::SteadyTime{100s};
  auto entry = MakeEntry(t0, t0);

  auto flush = strategy_.NextFlushTime(entry, 10s);
  auto eviction_deadline = entry.first_seen + 10s - 300s;
  EXPECT_EQ(flush, eviction_deadline);
  EXPECT_LT(flush, t0);
}

TEST_F(FlushStrategyTest, FreshEntryFlushesAtLastModifiedPlusQuiet) {
  auto t0 = core::SteadyTime{100s};
  auto entry = MakeEntry(t0, t0);

  auto flush = strategy_.NextFlushTime(entry, 86400s);
  EXPECT_EQ(flush, t0 + 30s);
}

TEST_F(FlushStrategyTest, EqualQuietAndDeadlineReturnsSharedTime) {
  auto t0 = core::SteadyTime{100s};
  auto eviction = 330s;
  auto entry = MakeEntry(t0, t0);

  auto quiet_deadline = t0 + 30s;
  auto eviction_deadline = t0 + eviction - 300s;
  EXPECT_EQ(quiet_deadline, eviction_deadline);

  auto flush = strategy_.NextFlushTime(entry, eviction);
  EXPECT_EQ(flush, quiet_deadline);
}

}  // namespace
}  // namespace abyss::consumer
