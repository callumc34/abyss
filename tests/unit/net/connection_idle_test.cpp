#include <gtest/gtest.h>

#include <chrono>

#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/net.h"
#include "abyss/resp/command_registry.h"
#include "fake_poller.h"
#include "socket_pair.h"
#include "stub_dispatcher.h"

namespace abyss::net {
namespace {

namespace pnet = abyss::platform::net;
using abyss::testing::SocketPair;

class ConnectionIdleTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_TRUE(pnet::Init().has_value()); }
  void TearDown() override { pnet::Shutdown(); }
};

TEST_F(ConnectionIdleTest, IdleAfterTimeout) {
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  auto fake_now = core::SteadyTime{std::chrono::seconds{1000}};
  ConnectionConfig config{
      .idle_timeout = std::chrono::seconds{30},
  };

  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, /*client_id=*/42, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics,
                  [&] { return fake_now; });

  EXPECT_FALSE(conn.IsIdle(fake_now));
  EXPECT_FALSE(conn.IsIdle(fake_now + std::chrono::seconds{29}));
  EXPECT_TRUE(conn.IsIdle(fake_now + std::chrono::seconds{31}));
}

// Strict greater-than: alive at the deadline boundary is still alive.
TEST_F(ConnectionIdleTest, NotIdleAtExactTimeout) {
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  auto fake_now = core::SteadyTime{std::chrono::seconds{1000}};
  ConnectionConfig config{.idle_timeout = std::chrono::seconds{30}};

  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics,
                  [&] { return fake_now; });

  EXPECT_FALSE(conn.IsIdle(fake_now + std::chrono::seconds{30}));
}

}  // namespace
}  // namespace abyss::net
