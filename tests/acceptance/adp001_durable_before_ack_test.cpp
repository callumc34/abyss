#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <future>
#include <memory>

#include "abyss/queue/append_result.h"
#include "integration_harness.h"

namespace abyss::acceptance {
namespace {

using ::testing::_;

class Adp001DurableBeforeAckTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;
};

TEST_F(Adp001DurableBeforeAckTest, FsyncFailureSurfacesErrorToClient) {
  harness_.HotPool().Stop();

  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  EXPECT_CALL(harness_.Queue(), BeginAppend(_, _))
      .WillOnce([](core::ShardId, core::QueueEntry entry) {
        std::promise<core::Result<void>> p;
        p.set_value(std::unexpected(core::Error{core::ErrorCode::kInternal, "fsync failed"}));
        return queue::PendingAppend{entry.seq, p.get_future(),
                                    std::make_unique<testing::NoopAppendPublisher>()};
      });

  core::RespCommand cmd{.args = {"SET", "k", "v"}};
  auto result = harness_.Engine().DispatchWrite("SET", std::move(cmd));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
}

}  // namespace
}  // namespace abyss::acceptance
