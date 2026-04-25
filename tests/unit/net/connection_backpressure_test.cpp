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

// Tight kernel send buffer to force quick EAGAIN on send() so write_buf_ holds
// the rest of the response. Chosen above the platform minimum (~2 KiB on
// macOS) but well below the response sizes used in these tests.
constexpr int kSndBufBytes = 4096;

struct ConnectedPair {
  Fd server;
  int client = -1;

  ~ConnectedPair() {
    if (client >= 0) ::close(client);
  }
  ConnectedPair() = default;
  ConnectedPair(const ConnectedPair&) = delete;
  ConnectedPair& operator=(const ConnectedPair&) = delete;
  ConnectedPair(ConnectedPair&& other) noexcept
      : server(std::move(other.server)), client(other.client) {
    other.client = -1;
  }
  ConnectedPair& operator=(ConnectedPair&& other) noexcept {
    if (this != &other) {
      if (client >= 0) ::close(client);
      server = std::move(other.server);
      client = other.client;
      other.client = -1;
    }
    return *this;
  }
};

ConnectedPair MakePair(int sndbuf_bytes = kSndBufBytes) {
  int sv[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0) << std::strerror(errno);

  for (int i = 0; i < 2; ++i) {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
    const int flags = ::fcntl(sv[i], F_GETFL, 0);
    EXPECT_NE(::fcntl(sv[i], F_SETFL, flags | O_NONBLOCK), -1);
    // NOLINTEND(cppcoreguidelines-pro-type-vararg)
  }

  // Tight kernel buffers so back-pressure fires within the configured limits.
  EXPECT_EQ(::setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &sndbuf_bytes, sizeof(sndbuf_bytes)), 0);
  EXPECT_EQ(::setsockopt(sv[1], SOL_SOCKET, SO_RCVBUF, &sndbuf_bytes, sizeof(sndbuf_bytes)), 0);

  ConnectedPair p;
  p.server = Fd(sv[0]);
  p.client = sv[1];
  return p;
}

void WriteAll(int fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    const ssize_t n = ::send(fd, data.data() + off, data.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
    FAIL() << "WriteAll send failed: " << std::strerror(errno);
  }
}

ConnectionConfig SmallBuffersConfig() {
  return ConnectionConfig{
      .max_read_buffer_bytes = 65536,
      .write_backpressure_bytes = 8192,
      .write_resume_bytes = 1024,
      .write_hard_limit_bytes = 65536,
      .idle_timeout = std::chrono::seconds{60},
  };
}

class ConnectionBackpressureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pair_ = MakePair();
    metrics_ = NetMetrics{};
  }

  ConnectedPair pair_;
  testing::FakePoller poller_;
  testing::StubDispatcher dispatcher_;
  NetMetrics metrics_;
};

TEST_F(ConnectionBackpressureTest, PauseEntered) {
  dispatcher_.read_payload.assign(32 * 1024, 'a');
  Connection conn(std::move(pair_.server), 0, 0, /*client_id=*/1, poller_, resp::GlobalRegistry(),
                  dispatcher_, SmallBuffersConfig(), metrics_);
  ASSERT_TRUE(conn.Arm().has_value());
  ASSERT_FALSE(conn.ReadingPaused());

  WriteAll(pair_.client, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();

  EXPECT_TRUE(conn.ReadingPaused());
  EXPECT_EQ(poller_.LastInterest(), EventKind::kWritable);
  EXPECT_GT(conn.WriteBufferBytes(), 8192U);
  EXPECT_FALSE(conn.IsClosed());
}

TEST_F(ConnectionBackpressureTest, ResumeAfterDrain) {
  dispatcher_.read_payload.assign(32 * 1024, 'b');
  Connection conn(std::move(pair_.server), 0, 0, 1, poller_, resp::GlobalRegistry(), dispatcher_,
                  SmallBuffersConfig(), metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair_.client, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();
  ASSERT_TRUE(conn.ReadingPaused());

  std::string sink(64 * 1024, '\0');
  while (true) {
    const ssize_t n = ::recv(pair_.client, sink.data(), sink.size(), 0);
    if (n <= 0) break;
    conn.OnWritable();
    if (!conn.ReadingPaused()) break;
  }

  EXPECT_FALSE(conn.ReadingPaused());
  EXPECT_LT(conn.WriteBufferBytes(), 1024U);
  EXPECT_EQ(poller_.LastInterest(), EventKind::kReadable | EventKind::kWritable);
}

TEST_F(ConnectionBackpressureTest, HardLimitClosesConnection) {
  dispatcher_.read_payload.assign(96 * 1024, 'c');
  Connection conn(std::move(pair_.server), 0, 0, 1, poller_, resp::GlobalRegistry(), dispatcher_,
                  SmallBuffersConfig(), metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair_.client, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();

  EXPECT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kBackpressure);
}

}  // namespace
}  // namespace abyss::net
