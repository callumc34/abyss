#include "abyss/net/tcp_server.h"

#include <gtest/gtest.h>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif

#include <atomic>
#include <chrono>
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/platform/net.h"
#include "abyss/resp/command_registry.h"
#include "latch.h"
#include "sync_redis_client.h"

namespace abyss::net {
namespace {

class TcpServerComponentTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef _WIN32
    ASSERT_TRUE(platform::net::Init().has_value());
#endif
  }
};

class StubDispatcher : public core::CommandDispatcher {
 public:
  std::atomic<int> read_calls{0};
  std::atomic<int> write_calls{0};
  std::string read_payload = "value";

  core::Outcome Dispatch(core::Request request, core::Slot& /*slot*/,
                         std::unique_ptr<core::Task>& /*task*/) override {
    if (request.kind == core::Request::Kind::kRead) {
      read_calls.fetch_add(1);
      return {.reply = core::RespValue::BulkString(read_payload)};
    }
    write_calls.fetch_add(1);
    return {.reply = core::RespValue::SimpleString("OK")};
  }
};

// "again" pends until its kResume, and its first resume posts one
// more from the thread running it, as a stopped pool fails at once the
// job a resumed task submits. "hold" keeps its reactor until released.
// Anything else replies at once.
class ResumeAgainDispatcher : public core::CommandDispatcher {
 public:
  core::Outcome Dispatch(core::Request request, core::Slot& slot,
                         std::unique_ptr<core::Task>& task) override {
    const std::string key = request.cmd.args.size() > 1 ? request.cmd.args[1] : std::string();
    slot.deadline = core::SteadyClock::now() + std::chrono::hours{1};
    if (key == "hold") {
      holding.Open();
      (void)release.Wait(std::chrono::seconds{30});
    } else if (key == "again") {
      if (task == nullptr) task = std::make_unique<Again>(*this);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): made here.
      static_cast<Again&>(*task).Begin();
      {
        const std::scoped_lock lock(mu_);
        pending_ = slot.completion;
      }
      pended.Open();
      return {.kind = core::Outcome::Kind::kPending};
    }
    return {.reply = core::RespValue::BulkString(key)};
  }

  // The pending command's kResume, from the caller's thread.
  void Wake() {
    core::Completion completion;
    {
      const std::scoped_lock lock(mu_);
      completion = pending_;
    }
    Resume(completion);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch holding;
  abyss::testing::Latch release;
  abyss::testing::Latch pended;
  std::atomic<int> runs{0};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  class Again final : public core::Task {
   public:
    explicit Again(ResumeAgainDispatcher& owner) : owner_(owner) {}

    void Begin() {
      busy_ = true;
      posted_ = false;
    }
    core::Outcome Run(core::Slot& slot, core::Result<void> /*wake*/) override {
      owner_.runs.fetch_add(1);
      if (!std::exchange(posted_, true)) {
        Resume(slot.completion);
        return {.kind = core::Outcome::Kind::kPending};
      }
      busy_ = false;
      return {.reply = core::RespValue::BulkString("again")};
    }
    core::Result<core::RespValue> Abandon(core::Slot& /*slot*/,
                                          core::AbandonReason /*reason*/) override {
      return std::unexpected(core::Error{core::ErrorCode::kTimeout, "abandoned"});
    }
    bool Quiescent() const override { return !busy_; }

   private:
    ResumeAgainDispatcher& owner_;
    bool busy_ = false;
    bool posted_ = false;
  };

  static void Resume(const core::Completion& completion) {
    const core::CompletionEvent event{
        .client_id = completion.client_id,
        .slot_seq = completion.slot_seq,
        .kind = core::CompletionKind::kResume,
        .result = {},
    };
    completion.sink->Post(std::span(&event, 1));
  }

  std::mutex mu_;
  core::Completion pending_;
};

std::string Resp(std::initializer_list<std::string_view> args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const std::string_view arg : args) {
    out += "$" + std::to_string(arg.size()) + "\r\n";
    out += arg;
    out += "\r\n";
  }
  return out;
}

double OrphanGauge() {
  return metrics::testing::GetGaugeValue(metrics::names::kNetOrphans).value_or(0);
}
double LateCompletions() {
  return metrics::testing::GetCounterValue(metrics::names::kNetLateCompletionsTotal).value_or(0);
}

template <typename Done>
bool PollUntil(const Done& done, std::chrono::milliseconds budget = std::chrono::seconds{10}) {
  const auto until = std::chrono::steady_clock::now() + budget;
  while (!done()) {
    if (std::chrono::steady_clock::now() >= until) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  return true;
}

TcpServerConfig DefaultTestConfig() {
  TcpServerConfig c;
  c.bind = "127.0.0.1";
  c.port = 0;
  c.max_connections = 32;
  c.io_threads = 2;
  c.shutdown_grace = std::chrono::seconds{2};
  c.reaper_tick = std::chrono::milliseconds{100};
  c.connection.idle_timeout = std::chrono::seconds{60};
  return c;
}  // namespace

TEST_F(TcpServerComponentTest, BindFailureSurfacesAsError) {
  TcpServerConfig good = DefaultTestConfig();
  good.port = 0;
  StubDispatcher dispatcher;
  TcpServer s1(good, resp::GlobalRegistry(), resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(s1.Start().has_value());

  TcpServerConfig dup = DefaultTestConfig();
  dup.port = s1.BoundPort();
  TcpServer s2(dup, resp::GlobalRegistry(), resp::PipelineDependencies{.dispatcher = &dispatcher});
  auto r = s2.Start();
  EXPECT_FALSE(r.has_value());

  s1.Stop();
}

TEST_F(TcpServerComponentTest, PingRoundTrip) {
  StubDispatcher dispatcher;
  TcpServer server(DefaultTestConfig(), resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  const std::string reply = client.Command({"PING"});
  EXPECT_EQ(reply, "+PONG\r\n");

  server.Stop();
}

TEST_F(TcpServerComponentTest, GetAndSetExerciseDispatcher) {
  StubDispatcher dispatcher;
  TcpServer server(DefaultTestConfig(), resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  EXPECT_EQ(client.Command({"SET", "k", "v"}), "+OK\r\n");
  EXPECT_EQ(client.Command({"GET", "k"}), "$5\r\nvalue\r\n");
  EXPECT_GE(dispatcher.write_calls.load(), 1);
  EXPECT_GE(dispatcher.read_calls.load(), 1);

  server.Stop();
}

TEST_F(TcpServerComponentTest, PipeliningPreservesOrder) {
  StubDispatcher dispatcher;
  TcpServer server(DefaultTestConfig(), resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));

  ASSERT_TRUE(client.SendRaw("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"));

  std::string acc;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
  while (std::chrono::steady_clock::now() < deadline) {
    auto chunk = client.ReadSome(4096, std::chrono::milliseconds{200});
    if (chunk.empty()) continue;
    acc += chunk;
    size_t count = 0;
    size_t pos = 0;
    while ((pos = acc.find("+PONG\r\n", pos)) != std::string::npos) {
      ++count;
      pos += 7;
    }
    if (count >= 3) break;
  }

  EXPECT_EQ(acc, "+PONG\r\n+PONG\r\n+PONG\r\n");
  server.Stop();
}

// #94: Stop must not block per-connection.
TEST_F(TcpServerComponentTest, StopJoinIsBoundedWithActiveConnections) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.shutdown_grace = std::chrono::seconds{1};
  cfg.io_threads = 4;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  std::vector<component_test::SyncRedisClient> clients(8);
  for (auto& c : clients) {
    ASSERT_TRUE(c.Connect(server.BoundPort()));
    EXPECT_EQ(c.Command({"PING"}), "+PONG\r\n");
  }

  const auto t0 = std::chrono::steady_clock::now();
  server.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;
  EXPECT_LT(elapsed, std::chrono::seconds{3});
}

TEST_F(TcpServerComponentTest, MaxConnectionsRejectsNewClients) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.max_connections = 2;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient c1;
  component_test::SyncRedisClient c2;
  ASSERT_TRUE(c1.Connect(server.BoundPort()));
  ASSERT_TRUE(c2.Connect(server.BoundPort()));

  EXPECT_EQ(c1.Command({"PING"}), "+PONG\r\n");
  EXPECT_EQ(c2.Command({"PING"}), "+PONG\r\n");

  // Third connect lands in the kernel backlog; server-side accept closes it.
  component_test::SyncRedisClient c3;
  if (c3.Connect(server.BoundPort())) {
    EXPECT_TRUE(c3.Command({"PING"}).empty());
  }

  server.Stop();
}

TEST_F(TcpServerComponentTest, SlowClientHardLimitsWithoutAffectingOthers) {
#ifdef _WIN32
  GTEST_SKIP() << "Windows TCP loopback ignores SO_SNDBUF; backpressure can't fire.";
#endif
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.connection.write_backpressure_bytes = 4096;
  cfg.connection.write_resume_bytes = 1024;
  cfg.connection.write_hard_limit_bytes = 8192;
  cfg.io_threads = 2;
  StubDispatcher dispatcher;
  dispatcher.read_payload.assign(static_cast<size_t>(32) * 1024 * 1024, 'z');
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  // Small SO_RCVBUF + never reading: receive window stays tight, the server's
  // write_buf_ accumulates past the hard limit, and the close path fires.
  component_test::SyncRedisClient slow;
  ASSERT_TRUE(slow.Connect(server.BoundPort(), component_test::SyncRedisClient::ConnectOptions{
                                                   .timeout = std::chrono::milliseconds{2000},
                                                   .recv_buffer_bytes = 4096,
                                               }));
  ASSERT_TRUE(slow.SendRaw("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"));

  component_test::SyncRedisClient healthy;
  ASSERT_TRUE(healthy.Connect(server.BoundPort()));
  EXPECT_EQ(healthy.Command({"PING"}), "+PONG\r\n");
  EXPECT_EQ(healthy.Command({"PING"}), "+PONG\r\n");

  // Server should hard-limit-close the slow connection. Poll ActiveConnections
  // until only the healthy client remains. Deadline sized for sanitizer
  // overhead — pushing 32 MiB through the reactor and the kernel write buffer
  // takes seconds under TSAN. Healthy builds break out in <1 s.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
  while (std::chrono::steady_clock::now() < deadline && server.ActiveConnections() > 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
  }
  EXPECT_EQ(server.ActiveConnections(), 1U);

  EXPECT_EQ(healthy.Command({"PING"}), "+PONG\r\n");

  server.Stop();
}

TEST_F(TcpServerComponentTest, AcceptBurstHandledWithoutDrops) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.max_connections = 64;
  cfg.io_threads = 2;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  std::vector<component_test::SyncRedisClient> clients(16);
  for (auto& c : clients) {
    ASSERT_TRUE(c.Connect(server.BoundPort()));
  }
  for (auto& c : clients) {
    EXPECT_EQ(c.Command({"PING"}), "+PONG\r\n");
  }
  EXPECT_EQ(server.ActiveConnections(), 16U);

  server.Stop();
}

// NET-4: the read-buffer high-water is a fleet-wide max over live connections,
// not whichever connection wrote the shared gauge last.
TEST_F(TcpServerComponentTest, ReadBufferHighWaterIsFleetMaxNotLastWriter) {
  constexpr size_t kBigValueBytes = size_t{64} * 1024;
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.io_threads = 2;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient small;
  component_test::SyncRedisClient large;
  ASSERT_TRUE(small.Connect(server.BoundPort()));
  ASSERT_TRUE(large.Connect(server.BoundPort()));

  EXPECT_EQ(small.Command({"PING"}), "+PONG\r\n");
  EXPECT_LT(server.MaxReadBufferHighWaterBytes(), kBigValueBytes);

  EXPECT_EQ(large.Command({"SET", "k", std::string(kBigValueBytes, 'v')}), "+OK\r\n");

  // The reply can reach the client a hair before the reactor folds the new
  // high-water into its published value.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
  while (server.MaxReadBufferHighWaterBytes() < kBigValueBytes &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  const size_t fleet_max = server.MaxReadBufferHighWaterBytes();
  EXPECT_GE(fleet_max, kBigValueBytes);

  // A later small read on the other connection must not drag the fleet max down
  // to the last writer's value.
  EXPECT_EQ(small.Command({"PING"}), "+PONG\r\n");
  EXPECT_EQ(small.Command({"PING"}), "+PONG\r\n");
  EXPECT_GE(server.MaxReadBufferHighWaterBytes(), fleet_max);

  server.Stop();
}

TEST_F(TcpServerComponentTest, IdleConnectionsClosedByReaper) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.connection.idle_timeout = std::chrono::seconds{1};
  cfg.reaper_tick = std::chrono::milliseconds{100};
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  EXPECT_EQ(client.Command({"PING"}), "+PONG\r\n");

  std::this_thread::sleep_for(std::chrono::milliseconds{1500});
  EXPECT_TRUE(client.ReadSome(16, std::chrono::milliseconds{500}).empty());

  server.Stop();
}

// The final drain takes a kResume whose task, resumed, posts again from
// the reactor's thread, as a stopped pool fails what it submits: the
// reactor takes that one too, and stops with no orphan left.
TEST_F(TcpServerComponentTest, TheFinalDrainTakesWhatItsOwnEventsPost) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.io_threads = 1;
  ResumeAgainDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());
  const double before = OrphanGauge();

  component_test::SyncRedisClient gone;
  ASSERT_TRUE(gone.Connect(server.BoundPort()));
  ASSERT_TRUE(gone.SendRaw(Resp({"GET", "again"})));
  ASSERT_TRUE(dispatcher.pended.Wait());
  gone.Close();
  ASSERT_TRUE(PollUntil([before] { return OrphanGauge() == before + 1; }))
      << "its pipeline, still owed a post, is orphaned";

  // The reactor is held mid-pass, past its take: the wake waits in its
  // inbox, and the engine is done before the pass ends.
  component_test::SyncRedisClient holder;
  ASSERT_TRUE(holder.Connect(server.BoundPort()));
  ASSERT_TRUE(holder.SendRaw(Resp({"GET", "hold"})));
  ASSERT_TRUE(dispatcher.holding.Wait());
  dispatcher.Wake();
  server.RequestStop();
  dispatcher.release.Open();
  server.Join();

  EXPECT_EQ(dispatcher.runs.load(), 2) << "the post its first resume made was never taken";
  EXPECT_EQ(OrphanGauge(), before);
}

// A kResume for a command already answered, its slot reclaimed, names
// nothing owed: the reactor drops it and counts it.
TEST_F(TcpServerComponentTest, AnEventNamingNothingOwedIsDroppedAndCounted) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.io_threads = 1;
  ResumeAgainDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
  ASSERT_TRUE(server.Start().has_value());
  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  ASSERT_TRUE(client.SendRaw(Resp({"GET", "again"})));
  ASSERT_TRUE(dispatcher.pended.Wait());
  dispatcher.Wake();
  EXPECT_EQ(client.ReadReply(), "$5\r\nagain\r\n");
  const double late = LateCompletions();

  dispatcher.Wake();
  EXPECT_TRUE(PollUntil([late] { return LateCompletions() == late + 1; }));
  EXPECT_EQ(dispatcher.runs.load(), 2) << "a stale event ran the task";
  EXPECT_EQ(client.Command({"PING"}), "+PONG\r\n");
  EXPECT_EQ(LateCompletions(), late + 1);
  server.Stop();
}

}  // namespace
}  // namespace abyss::net
