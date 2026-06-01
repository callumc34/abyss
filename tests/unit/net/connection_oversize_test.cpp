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

// NET-2: a single oversize/unconsumable command must close with kOversize
// DETERMINISTICALLY in one OnReadable — not after a hypothetical second edge
// (which edge-triggered pollers never deliver once the buffer is capped).
TEST_F(ConnectionOversizeTest, SingleOversizedCommandClosesOversizeInOneCall) {
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

  // Exactly ONE call: deterministic in-call close, no second-edge dependency.
  conn.OnReadable();

  ASSERT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kOversize);
}

// NET-1: a read that hits the buffer cap with consumable (pipelined) data must
// make progress within the same OnReadable by self-draining as the pipeline
// frees buffer space — it must NOT strand bytes waiting for an edge that an ET
// poller will never deliver. After one OnReadable the buffer fully drains.
TEST_F(ConnectionOversizeTest, CappedReadDrainsConsumablePipelineInOneCall) {
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  NetMetrics metrics;
  // Small read cap so a pipelined stream of commands exceeds it and forces the
  // cap break, but each command is individually framable and consumable.
  ConnectionConfig config{
      .max_read_buffer_bytes = 4096,
      .write_backpressure_bytes = 16 * 1024 * 1024,
      .write_resume_bytes = 8 * 1024 * 1024,
      .write_hard_limit_bytes = 64 * 1024 * 1024,
      .idle_timeout = std::chrono::seconds{60},
  };
  Connection conn(Fd{pair->ReleaseRead()}, 0, 0, /*client_id=*/1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics);
  ASSERT_TRUE(conn.Arm().has_value());

  // Many small framable PING commands; total far exceeds the 4096 read cap.
  static constexpr std::string_view kPing{"*1\r\n$4\r\nPING\r\n"};
  static constexpr std::string_view kPong{"+PONG\r\n"};
  std::string stream;
  while (stream.size() < 32 * 1024) stream.append(kPing);

  size_t sent = 0;
  while (sent < stream.size()) {
    const auto n = pnet::Send(pair->Write(), stream.data() + sent, stream.size() - sent, 0);
    if (n > 0) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && pnet::IsWouldBlock(pnet::LastError())) break;
    FAIL() << "send failed: " << pnet::LastErrorString();
  }
  // The kernel accepted more than the read cap, so a single non-draining pass
  // (pre-fix) could process at most one cap's worth before stranding the rest.
  ASSERT_GT(sent, config.max_read_buffer_bytes);
  const size_t pings_sent = sent / kPing.size();

  conn.OnReadable();

  // Drain the connection's write side and count the PONGs the server produced.
  // Self-rescheduling after the cap break means ALL accepted PINGs were
  // processed in this one OnReadable — far more than a single 4096-cap pass.
  EXPECT_FALSE(conn.IsClosed());
  std::string replies;
  std::string sink(64 * 1024, '\0');
  conn.OnWritable();  // flush server -> client
  while (true) {
    const auto n = pnet::Recv(pair->Write(), sink.data(), sink.size(), 0);
    if (n <= 0) break;
    replies.append(sink.data(), static_cast<size_t>(n));
    conn.OnWritable();
  }
  size_t pongs = 0;
  for (size_t p = replies.find(kPong); p != std::string::npos; p = replies.find(kPong, p + 1)) {
    ++pongs;
  }
  // A non-draining pass would yield ~ (cap / ping_size) PONGs; we expect all.
  const size_t single_pass_ceiling = config.max_read_buffer_bytes / kPing.size();
  EXPECT_GT(pongs, single_pass_ceiling);
  EXPECT_EQ(pongs, pings_sent);
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

// RESP-3: an unframable protocol error must emit the -ERR reply and then close
// the connection (kClient). Pre-fix the pipeline never wired close on a
// protocol error, so the connection lingered.
TEST_F(ConnectionOversizeTest, MalformedFrameDrainsErrorThenCloses) {
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

  // Negative multibulk count: fully-delimited, unframable -> protocol error.
  static constexpr std::string_view kBad{"*-9\r\njunk\r\n"};
  ASSERT_GT(pnet::Send(pair->Write(), kBad.data(), kBad.size(), 0), 0);
  conn.OnReadable();

  // The -ERR reply fits the socket buffer, so the close fires in this call.
  EXPECT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kClient);
}

}  // namespace
}  // namespace abyss::net
