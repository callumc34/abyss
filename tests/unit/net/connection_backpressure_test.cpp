#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <utility>

#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
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

void WriteAll(platform::Socket fd, std::string_view data) {
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

// Windows TCP loopback buffers data without honouring SO_SNDBUF / SO_RCVBUF,
// so EWOULDBLOCK can't be triggered to exercise the backpressure paths.
// Connection's state machine being tested here is platform-independent and
// is verified by these same tests on POSIX (Linux CI, macOS).

TEST_F(ConnectionBackpressureTest, PauseEntered) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
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
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
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
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
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

// Reply large enough that the pause threshold is crossed no matter how much of
// it the kernel socket buffer swallows on the first drain.
constexpr size_t kBigReplyBytes = 1024 * 1024;
constexpr size_t kRecvChunkBytes = 64 * 1024;
constexpr std::string_view kGetRequest = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";

// Wide hysteresis band so pause and resume are each reached deterministically.
ConnectionConfig HysteresisConfig() {
  return ConnectionConfig{
      .max_read_buffer_bytes = 1024 * 1024,
      .write_backpressure_bytes = 256 * 1024,
      .write_resume_bytes = 128 * 1024,
      .write_hard_limit_bytes = 8 * 1024 * 1024,
      .idle_timeout = std::chrono::seconds{60},
  };
}

// Unlike ConnectionBackpressureTest these need real registry-backed handles:
// the invariant under test is the value of the backpressure gauge itself.
class BackpressureGaugeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(pnet::Init().has_value());
    metrics::testing::Reset();
    metrics_ = NetMetrics::Register();
  }

  void TearDown() override { pnet::Shutdown(); }

  static double Active() {
    return metrics::testing::GetGaugeValue(metrics::names::kNetBackpressureActive).value_or(-1.0);
  }
  static double Entered() {
    return metrics::testing::GetCounterValue(metrics::names::kNetBackpressureEnteredTotal)
        .value_or(-1.0);
  }
  static double Exited() {
    return metrics::testing::GetCounterValue(metrics::names::kNetBackpressureExitedTotal)
        .value_or(-1.0);
  }

  // Feeds the socket peer and pumps writability until the connection resumes or
  // closes. Returns false if it never settled.
  static bool DrainUntilSettled(Connection& conn, platform::Socket peer) {
    std::string sink(kRecvChunkBytes, '\0');
    for (int i = 0; i < 2000; ++i) {
      if (conn.IsClosed() || !conn.ReadingPaused()) return true;
      (void)pnet::Recv(peer, sink.data(), sink.size(), 0);
      conn.OnWritable();
    }
    return false;
  }

  NetMetrics metrics_;
};

// XRES-4/NET-3: a failed pause must not decrement an increment that never ran.
TEST_F(BackpressureGaugeTest, BackpressureGaugeStableOnPollerModifyFailure) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
  auto p = SocketPair::Make(/*small_buffers=*/true);
  ASSERT_TRUE(p.has_value()) << p.error().message();
  SocketPair pair = std::move(*p);
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  dispatcher.read_payload.assign(kBigReplyBytes, 'a');

  Connection conn(Fd{pair.ReleaseRead()}, 0, 0, /*client_id=*/1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, HysteresisConfig(),
                  metrics_);
  ASSERT_TRUE(conn.Arm().has_value());
  ASSERT_DOUBLE_EQ(Active(), 0.0);

  poller.fail_modify = true;
  WriteAll(pair.Write(), kGetRequest);
  conn.OnReadable();

  EXPECT_TRUE(conn.IsClosed());
  ASSERT_TRUE(conn.CloseReasonValue().has_value());
  EXPECT_EQ(*conn.CloseReasonValue(), metrics::CloseReason::kClient);
  EXPECT_DOUBLE_EQ(Active(), 0.0);
  EXPECT_DOUBLE_EQ(Entered(), 0.0);
  EXPECT_DOUBLE_EQ(Exited(), 0.0);
}

// NET-3: a failed resume must not leave the increment stranded on the gauge.
TEST_F(BackpressureGaugeTest, BackpressureGaugeNoLeakOnResumeModifyFailure) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
  auto p = SocketPair::Make(/*small_buffers=*/true);
  ASSERT_TRUE(p.has_value()) << p.error().message();
  SocketPair pair = std::move(*p);
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;
  dispatcher.read_payload.assign(kBigReplyBytes, 'b');

  Connection conn(Fd{pair.ReleaseRead()}, 0, 0, /*client_id=*/1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, HysteresisConfig(),
                  metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair.Write(), kGetRequest);
  conn.OnReadable();
  ASSERT_TRUE(conn.ReadingPaused());
  ASSERT_FALSE(conn.IsClosed());
  ASSERT_DOUBLE_EQ(Active(), 1.0);
  ASSERT_DOUBLE_EQ(Entered(), 1.0);

  poller.fail_modify = true;
  ASSERT_TRUE(DrainUntilSettled(conn, pair.Write()));

  EXPECT_TRUE(conn.IsClosed());
  EXPECT_DOUBLE_EQ(Active(), 0.0);
  EXPECT_DOUBLE_EQ(Exited(), 0.0);
}

// XRES-4: over randomised pause/resume/close cycles with random poller failures
// the gauge never dips below zero and always returns to zero once closed.
TEST_F(BackpressureGaugeTest, BackpressureGaugeExactlyOncePerCycle_Property) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
  // Fixed seed: a failing sequence has to be reproducible.
  std::mt19937 rng(0x5EEDU);  // NOLINT(cert-msc32-c,cert-msc51-cpp,bugprone-random-generator-seed)
  std::bernoulli_distribution coin(0.5);

  for (uint64_t iter = 1; iter <= 24; ++iter) {
    auto p = SocketPair::Make(/*small_buffers=*/true);
    ASSERT_TRUE(p.has_value()) << p.error().message();
    SocketPair pair = std::move(*p);
    testing::FakePoller poller;
    testing::StubDispatcher dispatcher;
    dispatcher.read_payload.assign(kBigReplyBytes, 'p');

    Connection conn(Fd{pair.ReleaseRead()}, 0, 0, iter, poller, resp::GlobalRegistry(),
                    resp::PipelineDependencies{.dispatcher = &dispatcher}, HysteresisConfig(),
                    metrics_);
    ASSERT_TRUE(conn.Arm().has_value());

    poller.fail_modify = coin(rng);
    WriteAll(pair.Write(), kGetRequest);
    conn.OnReadable();
    ASSERT_GE(Active(), 0.0) << "negative after pause, iter " << iter;

    poller.fail_modify = coin(rng);
    ASSERT_TRUE(DrainUntilSettled(conn, pair.Write())) << "iter " << iter;
    ASSERT_GE(Active(), 0.0) << "negative after resume, iter " << iter;

    conn.Close(metrics::CloseReason::kServerShutdown);
    EXPECT_DOUBLE_EQ(Active(), 0.0) << "iter " << iter;
  }

  EXPECT_DOUBLE_EQ(Active(), 0.0);
}

// NET-4: the shared gauge has no per-connection writer any more; the value a
// connection contributes is its own high-water, folded elsewhere.
TEST_F(BackpressureGaugeTest, ReadBufferHighWaterNotPublishedPerConnection) {
  auto p = SocketPair::Make();
  ASSERT_TRUE(p.has_value()) << p.error().message();
  SocketPair pair = std::move(*p);
  testing::FakePoller poller;
  testing::StubDispatcher dispatcher;

  Connection conn(Fd{pair.ReleaseRead()}, 0, 0, /*client_id=*/1, poller, resp::GlobalRegistry(),
                  resp::PipelineDependencies{.dispatcher = &dispatcher}, HysteresisConfig(),
                  metrics_);
  ASSERT_TRUE(conn.Arm().has_value());

  WriteAll(pair.Write(), kGetRequest);
  conn.OnReadable();

  EXPECT_GE(conn.ReadBufferHighWater(), kGetRequest.size());
  EXPECT_FALSE(
      metrics::testing::GetGaugeValue(metrics::names::kNetReadBufferHighWaterBytes).has_value());
}

}  // namespace
}  // namespace abyss::net
