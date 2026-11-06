#include <gtest/gtest.h>

#ifndef _WIN32
#include <sys/socket.h>
#endif

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/types.h"
#include "abyss/net/connection.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/net.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/serializer.h"
#include "event_inbox.h"
#include "fake_poller.h"
#include "mock_queue.h"
#include "scripted_dispatcher.h"
#include "socket_pair.h"

namespace abyss::net {
namespace {

using namespace std::chrono_literals;
namespace pnet = abyss::platform::net;
using abyss::testing::SocketPair;

std::string Command(std::initializer_list<std::string> args) {
  const auto bytes = resp::Serializer::SerializeCommand(core::RespCommand{.args = args});
  return {bytes.begin(), bytes.end()};
}
std::string Bulk(const std::string& value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

// A connection over a socket pair, its engine scripted and its reactor
// played by hand: events land in the inbox and are routed as the
// reactor routes them.
class ConnectionAsyncTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(pnet::Init().has_value());
    auto pair = SocketPair::Make();
    ASSERT_TRUE(pair.has_value()) << pair.error().message();
    pair_ = std::move(*pair);
  }
  void TearDown() override {
    conn_.reset();
    pnet::Shutdown();
  }

  Connection& Open(ConnectionConfig config = {}) {
    conn_ = std::make_unique<Connection>(
        Fd{pair_.ReleaseRead()}, 0, 0, kClient, poller_, resp::GlobalRegistry(),
        resp::PipelineDependencies{.dispatcher = &dispatcher_}, config, metrics_,
        core::DefaultSteadyClock, resp::PipelineHost{.sink = &inbox_});
    EXPECT_TRUE(conn_->Arm().has_value());
    return *conn_;
  }
  void Send(std::string_view data) {
    size_t off = 0;
    while (off < data.size()) {
      const auto n = pnet::Send(pair_.Write(), data.data() + off, data.size() - off, 0);
      ASSERT_GT(n, 0) << pnet::LastErrorString();
      off += static_cast<size_t>(n);
    }
  }
  // What the peer has received, waiting up to a second for `want` bytes.
  std::string Received(size_t want) {
    std::string got;
    const auto until = std::chrono::steady_clock::now() + 1s;
    char buf[4096];  // NOLINT(modernize-avoid-c-arrays)
    while (got.size() < want && std::chrono::steady_clock::now() < until) {
      const auto n = pnet::Recv(pair_.Write(), buf, sizeof(buf), 0);
      if (n > 0) {
        got.append(buf, static_cast<size_t>(n));
      } else {
        std::this_thread::sleep_for(1ms);
      }
    }
    return got;
  }
  // The engine's events so far, routed, then the connection pumped as
  // the reactor does for a connection its events touched.
  void Deliver() {
    while (inbox_.Pending() > 0) {
      const std::optional<core::CompletionEvent> event = inbox_.Take(std::nullopt);
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access): Pending says one is there.
      EXPECT_TRUE(orphans_.Route(*event, conn_ != nullptr ? conn_->Pipeline() : nullptr));
    }
    if (conn_ != nullptr && !conn_->IsClosed()) conn_->Pump();
  }
  bool ReadArmed() const { return Has(poller_.LastInterest(), EventKind::kReadable); }
  bool WriteArmed() const { return Has(poller_.LastInterest(), EventKind::kWritable); }

  // The peer's: sends what the socket takes, and reads what has come.
  size_t SendSome(std::string_view data) {
    if (data.empty()) return 0;
    const auto n = pnet::Send(pair_.Write(), data.data(), data.size(), 0);
    if (n > 0) return static_cast<size_t>(n);
    EXPECT_TRUE(pnet::IsWouldBlock(pnet::LastError())) << pnet::LastErrorString();
    return 0;
  }
  std::string ReceiveSome() {
    std::string got;
    char buf[4096];  // NOLINT(modernize-avoid-c-arrays)
    for (;;) {
      const auto n = pnet::Recv(pair_.Write(), buf, sizeof(buf), 0);
      if (n <= 0) return got;
      got.append(buf, static_cast<size_t>(n));
    }
  }
  // The reactor's part for a connection whose engine replies at once:
  // an event only for the interest it has armed. Armed for nothing
  // with replies still owed, it has stalled, as nothing would run it.
  std::string Serve(Connection& conn, const std::string& input, size_t want) {
    size_t sent = 0;
    std::string got;
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < until) {
      sent += SendSome(std::string_view(input).substr(sent));
      got += ReceiveSome();
      if (got.size() >= want || conn.IsClosed()) break;
      const bool read = ReadArmed();
      const bool write = WriteArmed();
      if (!read && !write) {
        ADD_FAILURE() << "armed for nothing at " << got.size() << " of " << want << " bytes";
        break;
      }
      if (read) conn.OnReadable();
      if (write) conn.OnWritable();
    }
    return got;
  }

  static constexpr uint64_t kClient = 9;

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  SocketPair pair_;
  testing::FakePoller poller_;
  ::testing::NiceMock<abyss::testing::MockQueue> queue_;
  abyss::testing::ScriptedDispatcher dispatcher_{queue_};
  abyss::testing::EventInbox inbox_;
  resp::Orphans orphans_;
  NetMetrics metrics_;
  std::unique_ptr<Connection> conn_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// Edge-triggered pollers say nothing more about bytes already read in:
// once the pending command lets the rest through, they run with no new
// readable event, and reading is re-armed only after.
TEST_F(ConnectionAsyncTest, ResumingRunsBufferedInputWithNoNewReadableEvent) {
  Connection& conn = Open();
  Send(Command({"GET", "p1"}) + Command({"GET", "r1"}) + Command({"GET", "r2"}));
  conn.OnReadable();
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"p1"}));
  EXPECT_FALSE(ReadArmed()) << "nothing more is read behind the gate";

  dispatcher_.WakeLast();
  Deliver();
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"p1", "r1", "r2"}));
  EXPECT_EQ(Received(24), Bulk("p1") + Bulk("r1") + Bulk("r2"));
  EXPECT_TRUE(ReadArmed());
}

TEST_F(ConnectionAsyncTest, TheSlotCapPausesReadingUntilHalfIsFree) {
  Connection& conn = Open({.max_inflight_commands = 2});
  Send(Command({"SET", "s1", "v"}) + Command({"SET", "s2", "v"}) + Command({"SET", "s3", "v"}));
  conn.OnReadable();
  EXPECT_EQ(dispatcher_.dispatched.size(), 2U);
  EXPECT_TRUE(conn.ReadingPaused());
  EXPECT_FALSE(ReadArmed());

  queue_.ReleaseParked(1);
  Deliver();
  EXPECT_EQ(Received(8), Bulk("s1"));
  EXPECT_TRUE(conn.ReadingPaused()) << "one of two slots is still in use";

  queue_.ReleaseParked(2);
  Deliver();
  EXPECT_EQ(dispatcher_.dispatched.size(), 3U) << "a free slot takes the buffered command";
  queue_.ReleaseParked(3);
  Deliver();
  EXPECT_EQ(Received(16), Bulk("s2") + Bulk("s3"));
  EXPECT_FALSE(conn.ReadingPaused());
  EXPECT_TRUE(ReadArmed());
}

// The cap stops the feed with input left. The pass that writes the
// replies filling it feeds the rest, as no event comes for bytes
// already read.
TEST_F(ConnectionAsyncTest, InputLeftAtTheSlotCapRunsOnceItsPassFreesTheSlots) {
  Connection& conn = Open({.max_inflight_commands = 2});
  const std::string pong = "+PONG\r\n";
  EXPECT_EQ(Serve(conn, Command({"PING"}) + Command({"PING"}) + Command({"PING"}), 3 * pong.size()),
            pong + pong + pong);
  EXPECT_TRUE(ReadArmed());
}

// With room in the socket for all of them, the 5000 arrive in one
// read: the cap stops the feed at 1024 with the rest buffered.
TEST_F(ConnectionAsyncTest, FiveThousandSetsWrittenAtOnceAllReplyAtTheDefaultCap) {
  constexpr size_t kCommands = 5000;
#ifndef _WIN32
  for (const Socket end : {pair_.Read(), pair_.Write()}) {
    constexpr int kBytes = 1024 * 1024;
    ASSERT_EQ(::setsockopt(end, SOL_SOCKET, SO_RCVBUF, &kBytes, sizeof(kBytes)), 0);
    ASSERT_EQ(::setsockopt(end, SOL_SOCKET, SO_SNDBUF, &kBytes, sizeof(kBytes)), 0);
  }
#endif
  Connection& conn = Open();
  std::string input;
  std::string expected;
  for (size_t i = 0; i < kCommands; ++i) {
    const std::string key = "r" + std::to_string(i);
    input += Command({"SET", key, "v"});
    expected += Bulk(key);
  }
#ifndef _WIN32
  ASSERT_EQ(SendSome(input), input.size()) << "the socket takes them all at once";
  input.clear();
#endif
  EXPECT_EQ(Serve(conn, input, expected.size()), expected);
  EXPECT_EQ(dispatcher_.dispatched.size(), kCommands);
}

TEST_F(ConnectionAsyncTest, InputLeftAtTheReplyByteLimitRunsOnceItsPassWritesThem) {
  constexpr int kPings = 20;
  Connection& conn = Open({.write_backpressure_bytes = 64, .write_resume_bytes = 32});
  std::string input;
  std::string expected;
  for (int i = 0; i < kPings; ++i) {
    input += Command({"PING"});
    expected += "+PONG\r\n";
  }
  EXPECT_EQ(Serve(conn, input, expected.size()), expected);
  EXPECT_TRUE(ReadArmed());
}

TEST_F(ConnectionAsyncTest, AFullReadBufferBehindTheGateIsNotOversize) {
  Connection& conn = Open({.max_read_buffer_bytes = 64});
  std::string input = Command({"GET", "p1"});
  for (int i = 0; i < 8; ++i) input += Command({"GET", "r" + std::to_string(i)});
  Send(input);
  conn.OnReadable();
  EXPECT_FALSE(conn.IsClosed()) << "the gate, not an unframable command, holds it";
  dispatcher_.WakeLast();
  Deliver();
  EXPECT_FALSE(conn.IsClosed());
  EXPECT_EQ(dispatcher_.dispatched.size(), 9U);
}

TEST_F(ConnectionAsyncTest, QuitBehindAPendingCommandClosesOnceTheRepliesAreWritten) {
  Connection& conn = Open();
  Send(Command({"GET", "p1"}) + Command({"QUIT"}));
  conn.OnReadable();
  EXPECT_FALSE(conn.IsClosed());
  dispatcher_.WakeLast();
  Deliver();
  EXPECT_TRUE(conn.IsClosed());
  EXPECT_EQ(Received(13), Bulk("p1") + "+OK\r\n");
}

TEST_F(ConnectionAsyncTest, TheReaperSparesAConnectionWithACommandInFlight) {
  Connection& conn = Open({.idle_timeout = 1s});
  Send(Command({"GET", "p1"}));
  conn.OnReadable();
  const auto later = core::SteadyClock::now() + 1h;
  EXPECT_FALSE(conn.IsIdle(later));
  dispatcher_.WakeLast();
  Deliver();
  EXPECT_EQ(Received(8), Bulk("p1"));
  EXPECT_TRUE(conn.IsIdle(later));
}

// Closing never waits for the engine: the socket and the connection go
// at once, and its pipeline is kept with the orphans until the engine's
// last event for it lands.
TEST_F(ConnectionAsyncTest, ClosingWithCommandsOutstandingLeavesThemToTheOrphans) {
  Connection& conn = Open();
  Send(Command({"SET", "s1", "v"}) + Command({"SET", "p1", "v"}));
  conn.OnReadable();
  conn.Close(metrics::CloseReason::kClient);
  orphans_.Adopt(conn.ReleasePipeline());
  conn_.reset();
  EXPECT_EQ(orphans_.size(), 1U);

  queue_.ReleaseParked(1);
  dispatcher_.WakeLast();
  Deliver();
  EXPECT_TRUE(orphans_.empty());
  EXPECT_EQ(dispatcher_.applied["p1"], 1) << "dispatched, so it runs to its outcome";
}

}  // namespace
}  // namespace abyss::net
