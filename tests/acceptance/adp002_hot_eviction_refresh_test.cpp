#include <gtest/gtest.h>

#include "abyss/hot/eviction_worker.h"
#include "integration_harness.h"

namespace abyss::acceptance {
namespace {

using namespace std::chrono_literals;

class Adp002HotEvictionRefreshTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;
};

TEST_F(Adp002HotEvictionRefreshTest, RepeatedReadsExtendLifetimePastDeadline) {
  hot::EvictionWorker worker(
      harness_.ShardedHot(),
      hot::EvictionWorker::Config{.tick = 10ms, .default_eviction = core::EvictionTTL{5}},
      harness_.Clock().SteadyFn());

  ASSERT_TRUE(harness_.SeedHot({"SET", "k", "v"}).has_value());

  harness_.Clock().Advance(3s);
  auto read = harness_.Engine().DispatchRead("GET", core::RespCommand{.args = {"GET", "k"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "v");

  worker.TickOnce();

  harness_.Clock().Advance(3s);
  auto still_present =
      harness_.Engine().DispatchRead("GET", core::RespCommand{.args = {"GET", "k"}});
  ASSERT_TRUE(still_present.has_value());
  EXPECT_EQ(still_present->AsString(), "v");
}

}  // namespace
}  // namespace abyss::acceptance
