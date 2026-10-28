#include <gtest/gtest.h>

#include "abyss/engine/tiering_engine.h"

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/durability.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/engine/io_pool.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/net/tcp_server.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/request_pipeline.h"
#include "abyss/resp/serializer.h"
#include "cold_gate.h"
#include "event_inbox.h"
#include "hooked_cold_store.h"
#include "latch.h"
#include "sequenced_engine_fixture.h"
#include "sync_pipeline.h"
#include "sync_redis_client.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

std::string Command(std::initializer_list<std::string> args) {
  const auto bytes = resp::Serializer::SerializeCommand(core::RespCommand{.args = args});
  return {bytes.begin(), bytes.end()};
}
std::string Bulk(const std::string& value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}
std::string Err(std::string_view text) { return "-ERR " + std::string(text) + "\r\n"; }

template <typename Done>
bool PollUntil(const Done& done, std::chrono::milliseconds budget = 10s) {
  const auto until = std::chrono::steady_clock::now() + budget;
  while (!done()) {
    if (std::chrono::steady_clock::now() >= until) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

// Holds the first post a reactor's inbox takes once armed, on the
// poster's thread, until released: a notifier that has taken its
// waiters and not yet posted to them.
class HeldPost {
 public:
  std::function<void()> Hook() {
    return [this] {
      if (!armed_.exchange(false)) return;
      entered.Open();
      (void)release.Wait(30s);
    };
  }
  void Arm() { armed_.store(true); }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  std::atomic<bool> armed_{false};
};

// The engine over the real log, hot, cold and I/O pool, under
// connections: driven by hand on the test's thread as one reactor
// drives them, or through a TCP server.
class FrontEndTest : public SequencedEngineTest {
 protected:
  void SetUp() override {
    loader_cold_ = &hooked_;
    hooked_.SetHook([this](std::string_view key) { gate_.Hook(key); });
  }
  void TearDown() override {
    gate_.ReleaseAll();
    if (server_ != nullptr) server_->Stop();
    server_.reset();
    if (io_pool_ != nullptr) io_pool_->WaitIdleForTesting();
    Settle();
    pipelines_.clear();
    SequencedEngineTest::TearDown();
  }

  resp::RequestPipeline& Connect(uint64_t client) {
    auto& pipeline = pipelines_[client];
    pipeline = std::make_unique<resp::RequestPipeline>(
        resp::GlobalRegistry(), resp::ConnectionState{.client_id = client},
        resp::PipelineDependencies{.dispatcher = engine_.get()},
        resp::PipelineHost{.sink = &inbox_});
    return *pipeline;
  }
  // Adds `input` to what `client` has buffered and feeds it: false if
  // some is left undispatched.
  bool Feed(uint64_t client, const std::string& input = {}) {
    std::string& buffered = input_[client];
    buffered += input;
    const auto fed = pipelines_.at(client)->Feed(
        std::span(reinterpret_cast<const uint8_t*>(buffered.data()), buffered.size()), 0);
    buffered.erase(0, fed.consumed);
    return buffered.empty();
  }
  std::string Drain(uint64_t client) {
    std::vector<uint8_t> out;
    pipelines_.at(client)->Drain(out);
    return {out.begin(), out.end()};
  }
  // Routes every event posted within `wait`, as the reactor does.
  void Pump(std::chrono::milliseconds wait = 0ms) {
    while (auto event = inbox_.Take(core::SteadyClock::now() + wait)) {
      const auto live = pipelines_.find(event->client_id);
      if (!orphans_.Route(*event, live != pipelines_.end() ? live->second.get() : nullptr)) {
        ++dropped_;
      }
      wait = 0ms;
    }
  }
  // What `client` writes until `want` bytes are out, routing events and
  // feeding what it buffered as they come.
  std::string Await(uint64_t client, size_t want, std::chrono::milliseconds budget = 10s) {
    std::string out = Drain(client);
    const auto until = core::SteadyClock::now() + budget;
    while (out.size() < want && core::SteadyClock::now() < until) {
      Pump(10ms);
      (void)Feed(client);
      out += Drain(client);
    }
    return out;
  }
  void Expire(uint64_t client, core::SteadyTime now = core::SteadyClock::now() + 1h) {
    if (auto orphan = pipelines_.at(client)->Expire(now)) orphans_.Adopt(std::move(orphan));
  }
  // Until nothing is owed a post.
  void Settle() {
    const auto until = core::SteadyClock::now() + 10s;
    const auto settled = [this] {
      for (const auto& [id, pipeline] : pipelines_) {
        if (!pipeline->Quiescent()) return false;
      }
      return orphans_.empty();
    };
    while (!settled() && core::SteadyClock::now() < until) Pump(10ms);
    EXPECT_TRUE(settled()) << "a post never landed";
  }

  // Writes `args`, then sends what they wrote to cold and out of hot.
  void Cold(std::vector<std::string> args) {
    ASSERT_EQ(Write(std::move(args)).rfind("error", 0), std::string::npos);
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
    EvictDrained();
  }
  bool Member(std::string_view key, std::string_view member) {
    auto read = hot_->Read(core::ops::ReadOp{core::ops::SetIsMember{.key = key, .member = member}});
    return read.result.has_value() && read.result->IsInteger() && read.result->AsInteger() == 1;
  }

  void StartServer(uint32_t reactors = 1) {
    net::TcpServerConfig config;
    config.bind = "127.0.0.1";
    config.port = 0;
    config.io_threads = reactors;
    config.shutdown_grace = 2s;
    config.reaper_tick = 100ms;
    server_ = std::make_unique<net::TcpServer>(
        config, resp::GlobalRegistry(), resp::PipelineDependencies{.dispatcher = engine_.get()});
    ASSERT_TRUE(server_->Start().has_value());
  }
  // As the server's: every waiter fails and every poster is joined;
  // the engine's objects stay.
  void StopEngine() {
    io_pool_->Shutdown();
    pool_->Stop(0ms);
    queue_->Shutdown();
    hot_->FailLoadWaits();
  }
  net::EngineStop EngineStop() {
    return net::EngineStop{.stop = [this] { StopEngine(); },
                           .running_jobs = [this] { return io_pool_->RunningJobs(); }};
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ColdKeyGate gate_;
  abyss::testing::HookedColdStore hooked_{[this]() -> core::ColdStore& { return *cold_; }};
  abyss::testing::EventInbox inbox_;
  resp::Orphans orphans_;
  std::map<uint64_t, std::unique_ptr<resp::RequestPipeline>> pipelines_;
  std::map<uint64_t, std::string> input_;
  int dropped_ = 0;
  std::unique_ptr<net::TcpServer> server_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- One connection's pipeline over the engine ---

// Each SET is sequenced before the next is dispatched, so the
// connection's writes wait to be durable together.
TEST_F(FrontEndTest, APipelineOfPowerLossSetsSharesFlushes) {
  constexpr int kDepth = 32;
  Open(Options{.durability = core::Durability::kPowerLoss});
  Connect(1);
  std::string input;
  std::string expected;
  for (int i = 0; i < kDepth; ++i) {
    input += Command({"SET", "k" + std::to_string(i), "v"});
    expected += "+OK\r\n";
  }
  const uint64_t before =
      metrics::testing::GetHistogramCount(metrics::names::kWalFlushDurationSeconds).value_or(0);
  EXPECT_TRUE(Feed(1, input)) << "a sequenced write lets the next dispatch";
  EXPECT_EQ(Await(1, expected.size()), expected);
  const uint64_t flushes =
      metrics::testing::GetHistogramCount(metrics::names::kWalFlushDurationSeconds).value_or(0) -
      before;
  RecordProperty("flushes", std::to_string(flushes));
  EXPECT_LT(flushes, static_cast<uint64_t>(kDepth));
}

// The SET is not dispatched while the GET before it is pending, so the
// GET reads what was there before it.
TEST_F(FrontEndTest, AMissThenASetOnOneConnectionReadsTheOldValue) {
  Open();
  Cold({"SET", "k", "old"});
  gate_.Hold("k");
  Connect(1);
  EXPECT_FALSE(Feed(1, Command({"GET", "k"}) + Command({"SET", "k", "new"})));
  ASSERT_TRUE(gate_.AwaitBlocked(1));
  Pump(20ms);
  EXPECT_EQ(Drain(1), "");
  EXPECT_FALSE(Feed(1)) << "the SET waits behind the pending GET";
  gate_.Release("k");
  EXPECT_EQ(Await(1, Bulk("old").size() + 5), Bulk("old") + "+OK\r\n");
  EXPECT_TRUE(Feed(1, Command({"GET", "k"})));
  EXPECT_EQ(Await(1, Bulk("new").size()), Bulk("new"));
}

// Both are linearised at once: the GET's fence waits on the SET's
// durability, not the connection, and both replies leave in order.
TEST_F(FrontEndTest, ASetThenAGetUnderPowerLossReplyInOrderOnceDurable) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  Connect(1);
  EXPECT_TRUE(Feed(1, Command({"SET", "k", "v"}) + Command({"GET", "k"})));
  EXPECT_EQ(pipelines_.at(1)->InFlight(), 2U);
  Pump(50ms);
  EXPECT_EQ(Drain(1), "") << "neither reply shows the SET before it is durable";
  stall.Release();
  EXPECT_EQ(Await(1, 5 + Bulk("v").size()), "+OK\r\n" + Bulk("v"));
}

// A pending command's deadline while its load job runs orphans its
// task under its client and slot. A second, the same; then a write
// that waits on the first's key pends. Each job's late post reaches
// its own orphan, which installs what it loaded and frees the key; the
// pending write goes on untouched until its own wake.
TEST_F(FrontEndTest, LateLoadsReachTheirOwnOrphansAndInstall) {
  Open(Options{.cold_io_threads = 2});
  Cold({"SADD", "a", "x"});
  Cold({"SADD", "b", "x"});
  gate_.Hold("a");
  gate_.Hold("b");
  Connect(1);

  EXPECT_TRUE(Feed(1, Command({"SADD", "a", "y"})));
  ASSERT_TRUE(gate_.AwaitBlocked(1));
  Expire(1);
  EXPECT_EQ(orphans_.size(), 1U);
  EXPECT_TRUE(Feed(1, Command({"SADD", "b", "y"})));
  ASSERT_TRUE(gate_.AwaitBlocked(2));
  Expire(1);
  EXPECT_EQ(orphans_.size(), 2U) << "one client's two orphans, apart";
  EXPECT_TRUE(Feed(1, Command({"SADD", "a", "z"})));
  ASSERT_TRUE(PollUntil([this] { return hot_->LoadWaitersForTesting("a") == 1; }))
      << "the third write waits on the first one's load of its key";

  gate_.Release("a");
  const std::string timed_out = Err("write timed out loading its keys");
  EXPECT_EQ(Await(1, (2 * timed_out.size()) + 4), timed_out + timed_out + ":1\r\n");
  EXPECT_EQ(orphans_.size(), 1U);
  EXPECT_TRUE(Member("a", "x")) << "the abandoned write's load installed";
  EXPECT_TRUE(Member("a", "z"));
  EXPECT_FALSE(Member("a", "y")) << "a write abandoned at its deadline applied";

  gate_.Release("b");
  ASSERT_TRUE(PollUntil([this] {
    Pump();
    return orphans_.empty();
  }));
  EXPECT_TRUE(Member("b", "x"));
  EXPECT_FALSE(Member("b", "y"));
  EXPECT_EQ(dropped_, 0);
}

// Abandon takes back a cold read still queued, so only reads a thread
// has taken leave an orphan: with both threads held, a hundred
// timed-out misses leave two, and the queued ones never read cold.
TEST_F(FrontEndTest, OrphansAreBoundedByThePoolsThreads) {
  constexpr uint64_t kMisses = 100;
  Open(Options{.cold_io_threads = 2});
  for (uint64_t i = 0; i < kMisses; ++i) {
    const std::string key = "m" + std::to_string(i);
    ASSERT_EQ(Write({"SET", key, "v"}).rfind("error", 0), std::string::npos);
    gate_.Hold(key);
  }
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
  EvictDrained();

  for (uint64_t i = 0; i < kMisses; ++i) {
    Connect(i + 1);
    EXPECT_TRUE(Feed(i + 1, Command({"GET", "m" + std::to_string(i)})));
  }
  ASSERT_TRUE(gate_.AwaitBlocked(2));
  for (uint64_t i = 0; i < kMisses; ++i) Expire(i + 1);
  EXPECT_LE(orphans_.size(), 2U);
  RecordProperty("orphans", std::to_string(orphans_.size()));

  gate_.ReleaseAll();
  ASSERT_TRUE(PollUntil([this] {
    Pump();
    return orphans_.empty();
  }));
  io_pool_->WaitIdleForTesting();
  int reads = 0;
  for (uint64_t i = 0; i < kMisses; ++i) reads += gate_.Calls("m" + std::to_string(i));
  EXPECT_EQ(reads, 2) << "a job taken back still ran";
  for (uint64_t i = 0; i < kMisses; ++i) {
    EXPECT_EQ(Drain(i + 1), Err("read timed out loading its key"));
  }
}

// The successor of the pipeline's private blocking loop, which tests
// drive: a pending deadline replies, and the late post still lands.
TEST_F(FrontEndTest, ASyncPipelinesPendingDeadlineReplies) {
  Open(Options{.command_timeout = 100ms, .cold_io_threads = 2});
  Cold({"SET", "k", "v"});
  gate_.Hold("k");
  abyss::testing::SyncPipeline sync(resp::GlobalRegistry(), resp::ConnectionState{.client_id = 77},
                                    resp::PipelineDependencies{.dispatcher = engine_.get()});
  std::thread releaser([this] {
    (void)gate_.AwaitBlocked(1);
    std::this_thread::sleep_for(300ms);
    gate_.ReleaseAll();
  });
  const core::RespValue reply = sync.Dispatch(core::RespCommand{.args = {"GET", "k"}});
  releaser.join();
  ASSERT_TRUE(reply.IsError());
  EXPECT_EQ(reply.AsString(), "ERR read timed out loading its key");
  EXPECT_TRUE(sync.pipeline().Quiescent());
}

// --- Through the TCP server ---

// A cold read held on one connection leaves the reactor free for a
// hot read on another.
TEST_F(FrontEndTest, AHeldColdReadDoesNotDelayAHotReadOnTheSameReactor) {
  Open(Options{.cold_io_threads = 2});
  Cold({"SET", "cold", "c"});
  ASSERT_EQ(Write({"SET", "hot", "h"}), "OK");
  gate_.Hold("cold");
  StartServer(/*reactors=*/1);
  component_test::SyncRedisClient held;
  component_test::SyncRedisClient other;
  ASSERT_TRUE(held.Connect(server_->BoundPort()));
  ASSERT_TRUE(other.Connect(server_->BoundPort()));
  ASSERT_TRUE(held.SendRaw(Command({"GET", "cold"})));
  ASSERT_TRUE(gate_.AwaitBlocked(1));

  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(other.Command({"GET", "hot"}), Bulk("h"));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
  EXPECT_TRUE(held.ReadSome(1, 50ms).empty()) << "the cold read is still held";

  gate_.Release("cold");
  EXPECT_EQ(held.ReadReply(), Bulk("c"));
}

// The connection closes while the committer has taken its slot's node
// and not yet posted: the orphaned pipeline takes the late kDurable.
TEST_F(FrontEndTest, AClosedConnectionsPipelineTakesAPostAlreadyTaken) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  StartServer();
  HeldPost held;
  server_->SetInboxPostHookForTesting(held.Hook());
  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server_->BoundPort()));
  ASSERT_TRUE(client.SendRaw(Command({"SET", "k", "v"})));
  ASSERT_TRUE(PollUntil([this] {
    return queue_->DurableEnd(ShardOf("k"), core::Durability::kProcessCrash).value_or(0) > 1;
  })) << "the SET never sequenced";

  held.Arm();
  stall.Release();
  ASSERT_TRUE(held.entered.Wait());
  client.Close();
  ASSERT_TRUE(PollUntil([this] { return server_->Orphans() == 1; }));
  held.release.Open();
  EXPECT_TRUE(PollUntil([this] { return server_->Orphans() == 0; }));
}

// A write waiting on a spare segment when the server stops: its
// connection is closed, and the engine's stop fails the wait, which
// lands on the orphaned pipeline through the reactor still running.
TEST_F(FrontEndTest, ShutdownFailsRegisteredWaitersThroughTheOrphans) {
  Open(Options{.segment_size_bytes = size_t{16} << 10U});
  StartServer();
  queue_->PauseSegmentPreparerForTesting(0, true);
  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server_->BoundPort()));
  const std::string value(2048, 'v');
  std::string waiting;
  for (int i = 0; i < 200 && waiting.empty(); ++i) {
    const std::string key = "w" + std::to_string(i);
    ASSERT_TRUE(client.SendRaw(Command({"SET", key, value})));
    const std::string reply = client.ReadSome(5, 300ms);
    if (reply.empty()) {
      waiting = key;
    } else {
      ASSERT_EQ(reply, "+OK\r\n");
    }
  }
  ASSERT_FALSE(waiting.empty()) << "the spares never ran out";

  server_->StopAccepting();
  EXPECT_FALSE(server_->Quiesce(100ms)) << "its write is still in flight";
  server_->ForceClose();
  ASSERT_TRUE(PollUntil([this] { return server_->Orphans() == 1; }));
  StopEngine();
  EXPECT_TRUE(PollUntil([this] { return server_->Orphans() == 0; }));
  server_->Join();
  EXPECT_FALSE(Resident(waiting)) << "a write failed at the stop applied";
}

// The engine's stop runs while a notifier holds an event it has taken:
// the reactors keep taking events until the engine is done, so it
// lands and is cleaned up, and they stop with no orphan left.
TEST_F(FrontEndTest, TheReactorsOutlastANotifierHeldThroughTheEnginesStop) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  StartServer();
  HeldPost held;
  server_->SetInboxPostHookForTesting(held.Hook());
  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server_->BoundPort()));
  ASSERT_TRUE(client.SendRaw(Command({"SET", "k", "v"})));
  ASSERT_TRUE(PollUntil([this] {
    return queue_->DurableEnd(ShardOf("k"), core::Durability::kProcessCrash).value_or(0) > 1;
  }));
  held.Arm();
  stall.Release();
  ASSERT_TRUE(held.entered.Wait());
  client.Close();
  ASSERT_TRUE(PollUntil([this] { return server_->Orphans() == 1; }));

  std::thread stopper([this] { server_->Shutdown(EngineStop()); });
  // The stop is now joining the committer that holds the post.
  std::this_thread::sleep_for(100ms);
  held.release.Open();
  stopper.join();
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kNetOrphans).value_or(-1), 0.0);
}

// --- The stop's timeout ---

#ifndef _WIN32
TEST(FrontEndShutdownDeathTest, AStuckJobEndsTheProcessWithinTheTimeout) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EXIT(
      {
        IoPool pool(IoPoolConfig{.threads = 2});
        PoolBlocker stuck;
        pool.Submit(stuck, metrics::IoKind::kPoint, core::SteadyClock::now() + 1h);
        (void)stuck.started.Wait();
        net::TcpServerConfig config;
        config.bind = "127.0.0.1";
        config.port = 0;
        config.io_threads = 1;
        config.shutdown_timeout = 300ms;
        net::TcpServer server(config, resp::GlobalRegistry(), resp::PipelineDependencies{});
        if (!server.Start().has_value()) std::_Exit(1);
        server.Shutdown(net::EngineStop{.stop = [&pool] { pool.Shutdown(); },
                                        .running_jobs = [&pool] { return pool.RunningJobs(); }});
        std::_Exit(0);
      },
      ::testing::ExitedWithCode(net::kShutdownTimeoutExitCode), "shutdown timed out");
  EXPECT_LT(std::chrono::steady_clock::now() - start, 10s);
}
#endif

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
