#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <string_view>

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

class ConnectionOversizeTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_TRUE(pnet::Init().has_value()); }
  void TearDown() override { pnet::Shutdown(); }
};

TEST_F(ConnectionOversizeTest, ClosesOnReadBufferOverflow) {
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
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
  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, /*client_id=*/1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Bytes that don't form a complete RESP command — parser can't drain.
  std::string blob(16 * 1024, 'X');
  size_t off = 0;
  while (off < blob.size()) {
    const auto n = pnet::Send(pair->Write(), blob.data() + off, blob.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && pnet::IsWouldBlock(pnet::LastError())) break;
    FAIL() << "send failed: " << pnet::LastErrorString();
  }

  for (int i = 0; i < 8 && !conn.IsClosed(); ++i) conn.OnReadable();

  ASSERT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kOversize);
}

TEST_F(ConnectionOversizeTest, RecordsHighWaterAcrossReads) {
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
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
  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Truncated frame: bytes stay in read_buf_ so high-water lifts.
  static constexpr std::string_view kFrame{"*2\r\n$3\r\nGET\r\n$5\r\nh"};
  ASSERT_GT(pnet::Send(pair->Write(), kFrame.data(), kFrame.size(), 0), 0);
  conn.OnReadable();
  EXPECT_GE(conn.ReadBufferHighWater(), kFrame.size());
}

}  // namespace
}  // namespace abyss::net
