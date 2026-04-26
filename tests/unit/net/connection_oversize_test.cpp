#include <fcntl.h>
#include <gtest/gtest.h>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
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
namespace {
static struct WSAInitializer {
  WSAInitializer() { (void)Init(); }
} g_wsa_init;
}  // namespace
#endif

struct ConnectedPair {
  Fd server;
#ifdef _WIN32
  Socket client = INVALID_SOCKET;
  ~ConnectedPair() {
    if (client != INVALID_SOCKET) CloseSocket(client);
  }
#else
  int client = -1;
  ~ConnectedPair() {
    if (client >= 0) ::close(client);
  }
#endif
  ConnectedPair() = default;
  ConnectedPair(const ConnectedPair&) = delete;
  ConnectedPair& operator=(const ConnectedPair&) = delete;
  ConnectedPair(ConnectedPair&& o) noexcept : server(std::move(o.server)), client(o.client) {
#ifdef _WIN32
    o.client = INVALID_SOCKET;
#else
    o.client = -1;
#endif
  }
  ConnectedPair& operator=(ConnectedPair&&) = delete;
};

ConnectedPair MakePair() {
#ifdef _WIN32
  Socket listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == INVALID_SOCKET) return {};
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    CloseSocket(listener);
    return {};
  }
  int addr_len = sizeof(addr);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &addr_len) != 0) {
    CloseSocket(listener);
    return {};
  }
  if (::listen(listener, 1) != 0) {
    CloseSocket(listener);
    return {};
  }
  Socket writer = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (writer == INVALID_SOCKET) {
    CloseSocket(listener);
    return {};
  }
  if (::connect(writer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    CloseSocket(listener);
    CloseSocket(writer);
    return {};
  }
  Socket reader = ::accept(listener, nullptr, nullptr);
  CloseSocket(listener);
  if (reader == INVALID_SOCKET) {
    CloseSocket(writer);
    return {};
  }
  unsigned long nonblock = 1;
  ::ioctlsocket(reader, FIONBIO, &nonblock);
  ::ioctlsocket(writer, FIONBIO, &nonblock);
  ConnectedPair p;
  p.server = Fd(reader);
  p.client = writer;
  return p;
#else
  int sv[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
  for (int i = 0; i < 2; ++i) {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
    const int flags = ::fcntl(sv[i], F_GETFL, 0);
    EXPECT_NE(::fcntl(sv[i], F_SETFL, flags | O_NONBLOCK), -1);
    // NOLINTEND(cppcoreguidelines-pro-type-vararg)
  }
  ConnectedPair p;
  p.server = Fd(sv[0]);
  p.client = sv[1];
  return p;
#endif
}

TEST(ConnectionOversizeTest, ClosesOnReadBufferOverflow) {
  auto pair = MakePair();
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  ConnectionConfig config{
      .max_read_buffer_bytes = 4096,
      .write_backpressure_bytes = 1024,
      .write_resume_bytes = 256,
      .write_hard_limit_bytes = 65536,
      .idle_timeout = std::chrono::seconds{60},
  };
  Connection conn(std::move(pair.server), 0, 0, /*client_id=*/1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Bytes that don't form a complete RESP command — parser can't drain.
  std::string blob(16 * 1024, 'X');
  size_t off = 0;
  while (off < blob.size()) {
    const auto n = platform::net::Send(pair.client, blob.data() + off, blob.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && platform::net::IsWouldBlock(platform::net::LastError())) break;
    FAIL() << "send failed: " << platform::net::LastErrorString();
  }

  for (int i = 0; i < 8 && !conn.IsClosed(); ++i) conn.OnReadable();

  ASSERT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kOversize);
}

TEST(ConnectionOversizeTest, RecordsHighWaterAcrossReads) {
  auto pair = MakePair();
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  ConnectionConfig config{
      .max_read_buffer_bytes = 65536,
      .write_backpressure_bytes = 4096,
      .write_resume_bytes = 1024,
      .write_hard_limit_bytes = 16384,
      .idle_timeout = std::chrono::seconds{60},
  };
  Connection conn(std::move(pair.server), 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Truncated frame: bytes stay in read_buf_ so high-water lifts.
  ::send(pair.client, "*2\r\n$3\r\nGET\r\n$5\r\nh", 17, 0);
  conn.OnReadable();
  EXPECT_GE(conn.ReadBufferHighWater(), 17U);
}

}  // namespace
}  // namespace abyss::net
