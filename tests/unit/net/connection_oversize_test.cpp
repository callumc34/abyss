#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <utility>

#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/resp/command_registry.h"
#include "fake_poller.h"
#include "stub_dispatcher.h"

namespace abyss::net {
namespace {

struct ConnectedPair {
  Fd server;
  int client = -1;
  ~ConnectedPair() {
    if (client >= 0) ::close(client);
  }
  ConnectedPair() = default;
  ConnectedPair(const ConnectedPair&) = delete;
  ConnectedPair& operator=(const ConnectedPair&) = delete;
  ConnectedPair(ConnectedPair&& o) noexcept : server(std::move(o.server)), client(o.client) {
    o.client = -1;
  }
  ConnectedPair& operator=(ConnectedPair&&) = delete;
};

ConnectedPair MakePair() {
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
                  dispatcher, config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Bytes that don't form a complete RESP command — parser can't drain.
  std::string blob(16 * 1024, 'X');
  size_t off = 0;
  while (off < blob.size()) {
    const ssize_t n = ::send(pair.client, blob.data() + off, blob.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      conn.OnReadable();
      if (conn.IsClosed()) break;
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      conn.OnReadable();
      if (conn.IsClosed()) break;
      continue;
    }
    FAIL() << "send failed: " << std::strerror(errno);
  }

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
  Connection conn(std::move(pair.server), 0, 0, 1, poller, resp::GlobalRegistry(), dispatcher,
                  config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Truncated frame: bytes stay in read_buf_ so high-water lifts.
  ::send(pair.client, "*2\r\n$3\r\nGET\r\n$5\r\nh", 17, 0);
  conn.OnReadable();
  EXPECT_GE(conn.ReadBufferHighWater(), 17U);
}

}  // namespace
}  // namespace abyss::net
