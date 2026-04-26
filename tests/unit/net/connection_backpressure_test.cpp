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

struct ConnectedPair {
  Fd server;
#ifdef _WIN32
  Socket client = INVALID_SOCKET;
#else
  int client = -1;
#endif

  ~ConnectedPair() {
#ifdef _WIN32
    if (client != INVALID_SOCKET) platform::net::CloseSocket(client);
#else
    if (client >= 0) ::close(client);
#endif
  }
  ConnectedPair() = default;
  ConnectedPair(const ConnectedPair&) = delete;
  ConnectedPair& operator=(const ConnectedPair&) = delete;
  ConnectedPair(ConnectedPair&& other) noexcept
      : server(std::move(other.server)), client(other.client) {
#ifdef _WIN32
    other.client = INVALID_SOCKET;
#else
    other.client = -1;
#endif
  }
  ConnectedPair& operator=(ConnectedPair&& other) noexcept {
    if (this != &other) {
#ifdef _WIN32
      if (client != INVALID_SOCKET) platform::net::CloseSocket(client);
#else
      if (client >= 0) ::close(client);
#endif
      server = std::move(other.server);
      client = other.client;
#ifdef _WIN32
      other.client = INVALID_SOCKET;
#else
      other.client = -1;
#endif
    }
    return *this;
  }
};

ConnectedPair MakePair() {
#ifdef _WIN32
  using namespace platform::net;
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
  int sv[2] = {-1, -1};
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0) << std::strerror(errno);

  for (int i = 0; i < 2; ++i) {
    const int flags = ::fcntl(sv[i], F_GETFL, 0);
    EXPECT_NE(::fcntl(sv[i], F_SETFL, flags | O_NONBLOCK), -1);
  }

  ConnectedPair p;
  p.server = Fd(sv[0]);
  p.client = sv[1];
  return p;
#endif
}

void WriteAll(Socket fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    const auto n = platform::net::Send(fd, data.data() + off, data.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && platform::net::IsWouldBlock(platform::net::LastError())) continue;
    FAIL() << "WriteAll send failed: " << platform::net::LastErrorString();
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
    ASSERT_TRUE(platform::net::Init().has_value());
    pair_ = MakePair();
    metrics_ = NetMetrics{};
  }

  ConnectedPair pair_;
  testing::FakePoller poller_;
  testing::StubDispatcher dispatcher_;
  NetMetrics metrics_;
};

TEST_F(ConnectionBackpressureTest, PauseEntered) {
#ifdef _WIN32
  GTEST_SKIP()
      << "Kernel socket buffer backpressure differs: POSIX uses socketpair (small buffers causing "
         "EAGAIN), "
      << "Windows uses TCP loopback with different flow semantics - fundamental OS difference";
#endif

  dispatcher_.read_payload.assign(32 * 1024, 'a');
  Connection conn(std::move(pair_.server), 0, 0, /*client_id=*/1, poller_, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher_}, SmallBuffersConfig(),
                  metrics_);
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
#ifdef _WIN32
  GTEST_SKIP()
      << "Kernel socket buffer backpressure differs: POSIX uses socketpair (small buffers causing "
         "EAGAIN), "
      << "Windows uses TCP loopback with different flow semantics - fundamental OS difference";
#endif

  dispatcher_.read_payload.assign(32 * 1024, 'b');
  Connection conn(std::move(pair_.server), 0, 0, 1, poller_, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher_}, SmallBuffersConfig(),
                  metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair_.client, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();
  ASSERT_TRUE(conn.ReadingPaused());

  std::string sink(64 * 1024, '\0');
  while (true) {
    const auto n = platform::net::Recv(pair_.client, sink.data(), sink.size(), 0);
    if (n <= 0) break;
    conn.OnWritable();
    if (!conn.ReadingPaused()) break;
  }

  EXPECT_FALSE(conn.ReadingPaused());
  EXPECT_LT(conn.WriteBufferBytes(), 1024U);
  EXPECT_EQ(poller_.LastInterest(), EventKind::kReadable | EventKind::kWritable);
}

TEST_F(ConnectionBackpressureTest, HardLimitClosesConnection) {
#ifdef _WIN32
  GTEST_SKIP()
      << "Kernel socket buffer backpressure differs: POSIX uses socketpair (small buffers causing "
         "EAGAIN), "
      << "Windows uses TCP loopback with different flow semantics - fundamental OS difference";
#endif

  dispatcher_.read_payload.assign(96 * 1024, 'c');
  Connection conn(std::move(pair_.server), 0, 0, 1, poller_, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher_}, SmallBuffersConfig(),
                  metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair_.client, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();

  EXPECT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kBackpressure);
}

}  // namespace
}  // namespace abyss::net
