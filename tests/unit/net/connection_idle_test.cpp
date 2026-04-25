#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <utility>

#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/resp/command_registry.h"
#include "fake_poller.h"
#include "stub_dispatcher.h"

namespace abyss::net {
namespace {

Fd MakePeerFd() {
  int sv[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
  ::close(sv[1]);
  return Fd(sv[0]);
}

TEST(ConnectionIdleTest, IdleAfterTimeout) {
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  auto fake_now = core::SteadyTime{std::chrono::seconds{1000}};
  ConnectionConfig config{
      .idle_timeout = std::chrono::seconds{30},
  };

  Connection conn(MakePeerFd(), 0, 0, /*client_id=*/42, poller, resp::GlobalRegistry(), dispatcher,
                  config, metrics, [&] { return fake_now; });

  EXPECT_FALSE(conn.IsIdle(fake_now));
  EXPECT_FALSE(conn.IsIdle(fake_now + std::chrono::seconds{29}));
  EXPECT_TRUE(conn.IsIdle(fake_now + std::chrono::seconds{31}));
}

// Strict greater-than: alive at the deadline boundary is still alive.
TEST(ConnectionIdleTest, NotIdleAtExactTimeout) {
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  auto fake_now = core::SteadyTime{std::chrono::seconds{1000}};
  ConnectionConfig config{.idle_timeout = std::chrono::seconds{30}};

  Connection conn(MakePeerFd(), 0, 0, 1, poller, resp::GlobalRegistry(), dispatcher, config,
                  metrics, [&] { return fake_now; });

  EXPECT_FALSE(conn.IsIdle(fake_now + std::chrono::seconds{30}));
}

}  // namespace
}  // namespace abyss::net
