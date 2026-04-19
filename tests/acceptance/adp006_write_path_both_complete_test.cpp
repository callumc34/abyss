#include <gtest/gtest.h>

#include "abyss/core/ops.h"
#include "integration_harness.h"

namespace abyss::acceptance {
namespace {

class Adp006WritePathTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;
};

TEST_F(Adp006WritePathTest, DispatchWriteReturnsOnlyAfterDurableAndApply) {
  auto before = harness_.Rpc().PendingCount();
  auto result =
      harness_.Engine().DispatchWrite("SET", core::RespCommand{.args = {"SET", "k", "v"}});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(harness_.Rpc().PendingCount(), before);

  auto read = harness_.Hot().Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "v");
}

TEST_F(Adp006WritePathTest, ConsumerStalledTimeoutErrors) {
  harness_.HotPool().Stop();

  auto result =
      harness_.Engine().DispatchWrite("SET", core::RespCommand{.args = {"SET", "k", "v"}});
  ASSERT_FALSE(result.has_value() && !result->IsError());
}

}  // namespace
}  // namespace abyss::acceptance
