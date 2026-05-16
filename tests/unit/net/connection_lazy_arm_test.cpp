#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <string_view>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/net.h"
#include "abyss/resp/command_registry.h"
#include "fake_poller.h"
#include "socket_pair.h"

namespace abyss::net {
namespace {

namespace pnet = abyss::platform::net;
using abyss::testing::SocketPair;

class EchoDispatcher : public core::CommandDispatcher {
 public:
  std::string read_payload = "OK";
  core::Result<core::RespValue> DispatchRead(std::string_view /*name*/,
                                             const core::RespCommand& /*cmd*/) override {
    return core::RespValue::BulkString(read_payload);
  }
  core::Result<core::RespValue> DispatchWrite(std::string_view /*name*/,
                                              core::RespCommand /*cmd*/) override {
    return core::RespValue::SimpleString("OK");
  }
  core::Result<core::RespValue> DispatchConditional(std::string_view /*name*/,
                                                    core::RespCommand /*cmd*/,
                                                    core::PredicateFlags /*flags*/) override {
    return core::RespValue::SimpleString("OK");
  }
  core::Result<core::RespValue> DispatchFanOut(core::MultiKeyKind /*kind*/,
                                               core::RespCommand /*cmd*/) override {
    return core::RespValue::SimpleString("OK");
  }
  core::Result<core::RespValue> DispatchFlush(core::FlushTarget /*target*/) override {
    return core::RespValue::SimpleString("OK");
  }
};

class ConnectionLazyArmTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_TRUE(pnet::Init().has_value()); }
  void TearDown() override { pnet::Shutdown(); }

  // Backpressure thresholds set high so these tests exercise lazy arming
  // without triggering the pause/resume path (covered separately).
  static ConnectionConfig WideBackpressureConfig() {
    return ConnectionConfig{
        .max_read_buffer_bytes = 1024 * 1024,
        .write_backpressure_bytes = 1024 * 1024,
        .write_resume_bytes = 512 * 1024,
        .write_hard_limit_bytes = 4 * 1024 * 1024,
        .idle_timeout = std::chrono::seconds{60},
    };
  }
};

// Arm() establishes interest only in kReadable when the write buffer is empty
// and the connection is not paused. This guards against POLLOUT storms on
// level-triggered backends (WSAPoll) for idle connections.
TEST_F(ConnectionLazyArmTest, ArmReadableOnlyOnIdle) {
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
  testing::FakePoller poller;
  EchoDispatcher dispatcher;
  NetMetrics metrics;

  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, WideBackpressureConfig(),
                  metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  EXPECT_EQ(poller.LastInterest(), EventKind::kReadable);
  EXPECT_EQ(poller.CountOf(testing::FakePoller::Op::kAdd), 1U);
  EXPECT_EQ(poller.CountOf(testing::FakePoller::Op::kModify), 0U);
}

// When the pipeline produces output that doesn't fully drain into the kernel
// send buffer, the connection arms kWritable so the reactor wakes it again.
TEST_F(ConnectionLazyArmTest, ArmsWritableWhenOutputPending) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; can't leave bytes pending.";
#endif
  auto pair = SocketPair::Make(/*small_buffers=*/true);
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
  testing::FakePoller poller;
  EchoDispatcher dispatcher;
  // 256 KiB stays well under WideBackpressureConfig::write_backpressure_bytes
  // (1 MiB) so the pause path is not exercised here.
  dispatcher.read_payload.assign(256 * 1024, 'x');
  NetMetrics metrics;

  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, WideBackpressureConfig(),
                  metrics);
  ASSERT_TRUE(conn.Arm().has_value());
  ASSERT_EQ(poller.LastInterest(), EventKind::kReadable);

  static constexpr std::string_view kReq{"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"};
  ASSERT_GT(pnet::Send(pair->Write(), kReq.data(), kReq.size(), 0), 0);
  conn.OnReadable();

  ASSERT_FALSE(conn.ReadingPaused());
  EXPECT_TRUE(conn.HasPendingWrites());
  EXPECT_EQ(poller.LastInterest(), EventKind::kReadable | EventKind::kWritable);
}

// After the write buffer fully drains, kWritable is dropped — back to lazy.
TEST_F(ConnectionLazyArmTest, DropsWritableAfterFullDrain) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; can't leave bytes pending.";
#endif
  auto pair = SocketPair::Make(/*small_buffers=*/true);
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
  testing::FakePoller poller;
  EchoDispatcher dispatcher;
  dispatcher.read_payload.assign(256 * 1024, 'y');
  NetMetrics metrics;

  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, 1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, WideBackpressureConfig(),
                  metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  static constexpr std::string_view kReq{"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"};
  ASSERT_GT(pnet::Send(pair->Write(), kReq.data(), kReq.size(), 0), 0);
  conn.OnReadable();
  ASSERT_TRUE(conn.HasPendingWrites());
  ASSERT_EQ(poller.LastInterest(), EventKind::kReadable | EventKind::kWritable);

  // Drain everything from the peer side so each OnWritable can make progress
  // until the connection's write buffer is empty.
  std::string sink(64 * 1024, '\0');
  while (conn.HasPendingWrites()) {
    const auto n = pnet::Recv(pair->Write(), sink.data(), sink.size(), 0);
    if (n <= 0) break;
    conn.OnWritable();
  }

  EXPECT_FALSE(conn.HasPendingWrites());
  EXPECT_EQ(poller.LastInterest(), EventKind::kReadable);
}

}  // namespace
}  // namespace abyss::net
