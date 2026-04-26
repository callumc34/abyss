#include <gtest/gtest.h>

#include <chrono>
#include <string>

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

void WriteAll(platform::Socket fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    const auto n = pnet::Send(fd, data.data() + off, data.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && pnet::IsWouldBlock(pnet::LastError())) continue;
    FAIL() << "WriteAll send failed: " << pnet::LastErrorString();
  }
}

// Pause/resume thresholds tight; hard limit set well above the test payload
// so the pause path fires before the hard close path can.
ConnectionConfig PauseConfig() {
  return ConnectionConfig{
      .max_read_buffer_bytes = 1024 * 1024,
      .write_backpressure_bytes = 8192,
      .write_resume_bytes = 1024,
      .write_hard_limit_bytes = 4 * 1024 * 1024,
      .idle_timeout = std::chrono::seconds{60},
  };
}

// Hard limit deliberately tight so a single response trips it.
ConnectionConfig HardLimitConfig() {
  return ConnectionConfig{
      .max_read_buffer_bytes = 1024 * 1024,
      .write_backpressure_bytes = 8192,
      .write_resume_bytes = 1024,
      .write_hard_limit_bytes = 65536,
      .idle_timeout = std::chrono::seconds{60},
  };
}

class ConnectionBackpressureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(pnet::Init().has_value());
    auto p = SocketPair::Make(/*small_buffers=*/true);
    ASSERT_TRUE(p.has_value()) << p.error().message();
    pair_ = std::move(*p);
    metrics_ = NetMetrics{};
  }

  void TearDown() override { pnet::Shutdown(); }

  SocketPair pair_;
  testing::FakePoller poller_;
  testing::StubDispatcher dispatcher_;
  NetMetrics metrics_;
};

TEST_F(ConnectionBackpressureTest, PauseEntered) {
  // 256 KiB overflows the kernel send buffer on POSIX (SO_SNDBUF=2 KiB) and
  // Windows (which floors SO_SNDBUF at ~4-64 KiB). PauseConfig keeps
  // write_hard_limit_bytes well above the payload so the pause path runs,
  // not the hard-close path.
  dispatcher_.read_payload.assign(256 * 1024, 'a');
  Connection conn(Fd{pair_.ReleaseRead()}, 0, 0, /*client_id=*/1, poller_, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher_}, PauseConfig(), metrics_);
  ASSERT_TRUE(conn.Arm().has_value());
  ASSERT_FALSE(conn.ReadingPaused());

  WriteAll(pair_.Write(), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();

  EXPECT_TRUE(conn.ReadingPaused());
  EXPECT_EQ(poller_.LastInterest(), EventKind::kWritable);
  EXPECT_GT(conn.WriteBufferBytes(), 8192U);
  EXPECT_FALSE(conn.IsClosed());
}

TEST_F(ConnectionBackpressureTest, ResumeAfterDrain) {
  // 256 KiB payload (see PauseEntered for sizing rationale), well over the
  // 8 KiB write_backpressure_bytes / 1 KiB write_resume_bytes thresholds.
  dispatcher_.read_payload.assign(256 * 1024, 'b');
  Connection conn(Fd{pair_.ReleaseRead()}, 0, 0, 1, poller_, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher_}, PauseConfig(), metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair_.Write(), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();
  ASSERT_TRUE(conn.ReadingPaused());

  std::string sink(64 * 1024, '\0');
  while (true) {
    const auto n = pnet::Recv(pair_.Write(), sink.data(), sink.size(), 0);
    if (n <= 0) break;
    conn.OnWritable();
    if (!conn.ReadingPaused()) break;
  }

  EXPECT_FALSE(conn.ReadingPaused());
  EXPECT_LT(conn.WriteBufferBytes(), 1024U);
}

TEST_F(ConnectionBackpressureTest, HardLimitClosesConnection) {
  // 1 MiB exceeds Windows TCP autotuned send buffer + HardLimitConfig's
  // 64 KiB hard limit, so the close path triggers regardless of how much the
  // kernel decides to absorb.
  dispatcher_.read_payload.assign(1024 * 1024, 'c');
  Connection conn(Fd{pair_.ReleaseRead()}, 0, 0, 1, poller_, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher_}, HardLimitConfig(),
                  metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair_.Write(), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  conn.OnReadable();

  EXPECT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kBackpressure);
}

}  // namespace
}  // namespace abyss::net
