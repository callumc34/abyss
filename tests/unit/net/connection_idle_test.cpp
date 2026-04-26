#include <gtest/gtest.h>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <chrono>
#include <utility>

#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/net.h"
#include "abyss/platform/types.h"
#include "abyss/resp/command_registry.h"
#include "fake_poller.h"
#include "stub_dispatcher.h"

namespace abyss::net {
namespace {

using namespace abyss::platform::net;

#ifdef _WIN32
Fd MakePeerFd() {
  auto listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == kInvalidSocket) return Fd{kInvalidSocket};

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    CloseSocket(listener);
    return Fd{kInvalidSocket};
  }

  int len = sizeof(addr);
  ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len);
  if (::listen(listener, 1) != 0) {
    CloseSocket(listener);
    return Fd{kInvalidSocket};
  }

  auto writer = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (writer == kInvalidSocket) {
    CloseSocket(listener);
    return Fd{kInvalidSocket};
  }
  if (::connect(writer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    CloseSocket(listener);
    CloseSocket(writer);
    return Fd{kInvalidSocket};
  }

  auto reader = ::accept(listener, nullptr, nullptr);
  CloseSocket(listener);
  if (reader == kInvalidSocket) {
    CloseSocket(writer);
    return Fd{kInvalidSocket};
  }
  CloseSocket(writer);
  return Fd{reader};
}
#else
Fd MakePeerFd() {
  int sv[2] = {-1, -1};
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
  ::close(sv[1]);
  return Fd(sv[0]);
}
#endif

TEST(ConnectionIdleTest, IdleAfterTimeout) {
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  auto fake_now = core::SteadyTime{std::chrono::seconds{1000}};
  ConnectionConfig config{
      .idle_timeout = std::chrono::seconds{30},
  };

  Connection conn(MakePeerFd(), 0, 0, /*client_id=*/42, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics,
                  [&] { return fake_now; });

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

  Connection conn(MakePeerFd(), 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics,
                  [&] { return fake_now; });

  EXPECT_FALSE(conn.IsIdle(fake_now + std::chrono::seconds{30}));
}

}  // namespace
}  // namespace abyss::net
