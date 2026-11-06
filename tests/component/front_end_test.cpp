#include <gtest/gtest.h>

#include "abyss/engine/tiering_engine.h"

#ifdef ABYSS_HAVE_ROCKSDB

#ifndef _WIN32
#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iterator>
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
#include "abyss/metrics/metrics.h"
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
#include "temp_dir.h"

#ifndef _WIN32
extern char** environ;  // NOLINT(readability-redundant-declaration)
#endif

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

double OrphanGauge() {
  return metrics::testing::GetGaugeValue(metrics::names::kNetOrphans).value_or(0);
}
double LateCompletions() {
  return metrics::testing::GetCounterValue(metrics::names::kNetLateCompletionsTotal).value_or(0);
}
double Abandoned(metrics::AbandonReason reason) {
  return metrics::testing::GetCounterValue(metrics::names::kNetCommandsAbandonedTotal, reason)
      .value_or(0);
}
double ColdDeadlinesExceeded() {
  return metrics::testing::GetCounterValue(metrics::names::kColdReadDeadlineExceededTotal,
                                           metrics::IoKind::kPoint)
      .value_or(0);
}

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
    // A post still held would never land.
    held_.release.Open();
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
  // The engine's stop over this fixture's parts: every waiter fails and
  // every poster is joined; the objects stay.
  void StopEngine() {
    io_pool_->Shutdown();
    pool_->Stop(0ms);
    queue_->Shutdown();
    hot_->FailLoadWaits();
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ColdKeyGate gate_;
  // Outlives the server whose posts it holds, on a failure too.
  HeldPost held_;
  abyss::testing::HookedColdStore hooked_{[this]() -> core::ColdStore& { return *cold_; }};
  abyss::testing::EventInbox inbox_;
  resp::Orphans orphans_{metrics::Registry::Instance().Gauge(metrics::names::kNetOrphans)};
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
  const double gauge = OrphanGauge();

  EXPECT_TRUE(Feed(1, Command({"SADD", "a", "y"})));
  ASSERT_TRUE(gate_.AwaitBlocked(1));
  Expire(1);
  EXPECT_EQ(orphans_.size(), 1U);
  EXPECT_TRUE(Feed(1, Command({"SADD", "b", "y"})));
  ASSERT_TRUE(gate_.AwaitBlocked(2));
  Expire(1);
  EXPECT_EQ(orphans_.size(), 2U) << "one client's two orphans, apart";
  EXPECT_EQ(OrphanGauge(), gauge + 2);
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
  EXPECT_EQ(OrphanGauge(), gauge);
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
// The read is held until its deadline has given it up.
TEST_F(FrontEndTest, ASyncPipelinesPendingDeadlineReplies) {
  Open(Options{.command_timeout = 100ms, .cold_io_threads = 2});
  Cold({"SET", "k", "v"});
  gate_.Hold("k");
  const double exceeded = ColdDeadlinesExceeded();
  abyss::testing::SyncPipeline sync(resp::GlobalRegistry(), resp::ConnectionState{.client_id = 77},
                                    resp::PipelineDependencies{.dispatcher = engine_.get()});
  std::thread releaser([this, exceeded] {
    (void)gate_.AwaitBlocked(1);
    (void)PollUntil([exceeded] { return ColdDeadlinesExceeded() > exceeded; });
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
// hot read on another: the hot reply comes while the cold one is held.
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

  EXPECT_EQ(other.Command({"GET", "hot"}), Bulk("h"));
  EXPECT_EQ(gate_.Calls("cold"), 1) << "the cold read is still held";

  gate_.Release("cold");
  EXPECT_EQ(held.ReadReply(), Bulk("c"));
}

// The connection closes while the committer has taken its slot's node
// and not yet posted: its pipeline is orphaned, takes the late kDurable
// and settles.
TEST_F(FrontEndTest, AClosedConnectionsPipelineTakesAPostAlreadyTaken) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  StartServer();
  server_->SetInboxPostHookForTesting(held_.Hook());
  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server_->BoundPort()));
  ASSERT_TRUE(client.SendRaw(Command({"SET", "k", "v"})));
  ASSERT_TRUE(PollUntil([this] {
    return queue_->DurableEnd(ShardOf("k"), core::Durability::kProcessCrash).value_or(0) > 1;
  })) << "the SET never sequenced";
  const double orphans = OrphanGauge();
  const double late = LateCompletions();

  held_.Arm();
  stall.Release();
  ASSERT_TRUE(held_.entered.Wait());
  client.Close();
  ASSERT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans + 1; }))
      << "the closed connection's pipeline, owed the held post, is kept";
  held_.release.Open();
  EXPECT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans; })) << "it never settled";
  EXPECT_EQ(LateCompletions(), late) << "the post found no one";
}

// A write waiting on a spare segment when its client goes runs on,
// with the orphans; the engine's stop fails its wait, which lands on
// the orphaned pipeline through the reactor still running.
TEST_F(FrontEndTest, TheEnginesStopFailsAWaitAClosedConnectionLeft) {
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
    // It replies, or it waits for a spare that never comes.
    std::string reply;
    ASSERT_TRUE(PollUntil([this, &client, &reply] {
      if (queue_->SpareWaitersForTesting(0) > 0) return true;
      reply += client.ReadSome(5 - reply.size(), 1ms);
      return reply.size() == 5;
    }));
    if (queue_->SpareWaitersForTesting(0) > 0) {
      waiting = key;
    } else {
      ASSERT_EQ(reply, "+OK\r\n");
    }
  }
  ASSERT_FALSE(waiting.empty()) << "the spares never ran out";
  const double orphans = OrphanGauge();
  client.Close();
  ASSERT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans + 1; }))
      << "its write runs on with the orphans";

  server_->StopAccepting();
  server_->ForceClose();
  EXPECT_EQ(queue_->SpareWaitersForTesting(0), 1U) << "a client's close gives up nothing";
  StopEngine();
  EXPECT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans; }));
  server_->Join();
  EXPECT_FALSE(Resident(waiting)) << "a write failed at the stop applied";
}

// The server's stop gives up the commands pending on the connections
// it closes, starting no more engine work for them and counting none as
// a timeout: a write whose load lands as the engine stops, one waiting
// on that load, and a cold read, are never applied or logged.
TEST_F(FrontEndTest, TheServersStopGivesUpPendingWritesSoNoneIsLogged) {
  Open(Options{.cold_io_threads = 2});
  Cold({"SADD", "k", "x"});
  Cold({"SET", "r", "v"});
  gate_.Hold("k");
  gate_.Hold("r");
  StartServer();
  component_test::SyncRedisClient loading;
  component_test::SyncRedisClient joining;
  component_test::SyncRedisClient reading;
  ASSERT_TRUE(loading.Connect(server_->BoundPort()));
  ASSERT_TRUE(joining.Connect(server_->BoundPort()));
  ASSERT_TRUE(reading.Connect(server_->BoundPort()));
  ASSERT_TRUE(loading.SendRaw(Command({"SADD", "k", "y1"})));
  ASSERT_TRUE(gate_.AwaitBlocked(1));
  ASSERT_TRUE(joining.SendRaw(Command({"SADD", "k", "y2"})));
  ASSERT_TRUE(PollUntil([this] { return hot_->LoadWaitersForTesting("k") == 1; }))
      << "the second write waits on the first one's load";
  ASSERT_TRUE(reading.SendRaw(Command({"GET", "r"})));
  ASSERT_TRUE(gate_.AwaitBlocked(2));
  const double orphans = OrphanGauge();
  const double given_up = Abandoned(metrics::AbandonReason::kShutdown);
  const double timed_out = Abandoned(metrics::AbandonReason::kDeadline);
  const double exceeded = ColdDeadlinesExceeded();

  server_->StopAccepting();
  server_->ForceClose();
  EXPECT_EQ(Abandoned(metrics::AbandonReason::kShutdown), given_up + 3);
  EXPECT_EQ(Abandoned(metrics::AbandonReason::kDeadline), timed_out);
  EXPECT_EQ(ColdDeadlinesExceeded(), exceeded) << "a read given up at the stop timed out";
  EXPECT_EQ(hot_->LoadWaitersForTesting("k"), 0U) << "the waiting write's wait was taken back";
  EXPECT_EQ(OrphanGauge(), orphans + 2) << "only the cold reads still running are owed";

  // The engine's stop: the held reads land as the pool stops, and the
  // late posts of the commands they were for only clean up.
  gate_.ReleaseAll();
  io_pool_->Shutdown();
  EXPECT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans; }));
  pool_->Stop(0ms);
  queue_->Shutdown();
  hot_->FailLoadWaits();
  server_->Join();

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Members("k"), std::vector<std::string>{"x"});
  for (const core::QueueEntry& entry : Logged(ShardOf("k"))) {
    const std::vector<std::string> args = ArgsOf(entry);
    EXPECT_EQ(
        std::count(args.begin(), args.end(), "y1") + std::count(args.begin(), args.end(), "y2"), 0)
        << "a write given up at the stop was logged";
  }
}

// The engine's stop runs while a notifier holds an event it has taken.
// The reactors keep taking events until the engine is done, so it lands
// and is cleaned up, which is checked before the reactors may stop.
TEST_F(FrontEndTest, TheReactorsOutlastANotifierHeldThroughTheEnginesStop) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  StartServer();
  server_->SetInboxPostHookForTesting(held_.Hook());
  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server_->BoundPort()));
  ASSERT_TRUE(client.SendRaw(Command({"SET", "k", "v"})));
  ASSERT_TRUE(PollUntil([this] {
    return queue_->DurableEnd(ShardOf("k"), core::Durability::kProcessCrash).value_or(0) > 1;
  }));
  const double orphans = OrphanGauge();
  const double late = LateCompletions();
  held_.Arm();
  stall.Release();
  ASSERT_TRUE(held_.entered.Wait());
  client.Close();
  ASSERT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans + 1; }));

  abyss::testing::Latch stopping;
  abyss::testing::Latch checked;
  std::thread stopper([this, &stopping, &checked] {
    server_->Shutdown(net::EngineStop{
        .stop =
            [this, &stopping, &checked] {
              io_pool_->Shutdown();
              pool_->Stop(0ms);
              // Next, the log's stop joins the committer the post
              // holds.
              stopping.Open();
              queue_->Shutdown();
              hot_->FailLoadWaits();
              // The reactors are told the engine is done only after
              // this.
              (void)checked.Wait(30s);
            },
        .running_jobs = [this] { return io_pool_->RunningJobs(); },
    });
  });
  EXPECT_TRUE(stopping.Wait());
  held_.release.Open();
  EXPECT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans; }))
      << "the held post never reached the orphaned pipeline";
  EXPECT_EQ(LateCompletions(), late) << "the held post was dropped";
  checked.Open();
  stopper.join();
  EXPECT_EQ(OrphanGauge(), orphans);
}

// --- The stop's timeout ---

#ifndef _WIN32
constexpr auto kStuckStopTimeout = 300ms;
constexpr const char* kStuckStopDirEnv = "ABYSS_STUCK_STOP_DIR";

// The child half of the test below, which runs it in a process of its
// own: a stop whose engine never finishes, as a cold read stuck for
// good. It marks when the stop begins.
TEST(FrontEndShutdownDeathTest, DISABLED_StuckJobChild) {
  const char* dir = std::getenv(kStuckStopDirEnv);
  if (dir == nullptr) GTEST_SKIP() << "run by AStuckJobEndsTheProcessWithinTheTimeout";
  IoPool pool(IoPoolConfig{.threads = 2});
  PoolBlocker stuck;
  pool.Submit(stuck, metrics::IoKind::kPoint, core::SteadyClock::now() + 1h);
  (void)stuck.started.Wait();
  net::TcpServerConfig config;
  config.bind = "127.0.0.1";
  config.port = 0;
  config.io_threads = 1;
  config.shutdown_timeout = kStuckStopTimeout;
  net::TcpServer server(config, resp::GlobalRegistry(), resp::PipelineDependencies{});
  if (!server.Start().has_value()) std::_Exit(1);
  std::ofstream(std::filesystem::path(dir) / "stopping") << "stopping\n";
  server.Shutdown(net::EngineStop{
      .stop = [&pool] { pool.Shutdown(); },
      .running_jobs = [&pool] { return pool.RunningJobs(); },
  });
  std::_Exit(0);
}

// From the stop's start, the parent waits no longer than the timeout
// and a margin: a stop that never ends fails here, not at ctest's.
TEST(FrontEndShutdownDeathTest, AStuckJobEndsTheProcessWithinTheTimeout) {
  const abyss::testing::TempDir dir("stuck-stop");
  std::string self = ::testing::internal::GetArgvs().front();
  std::string filter = "--gtest_filter=FrontEndShutdownDeathTest.DISABLED_StuckJobChild";
  std::string disabled = "--gtest_also_run_disabled_tests";
  std::string env = std::string(kStuckStopDirEnv) + "=" + dir.String();
  const std::string err = dir.Sub("stderr").string();
  std::vector<char*> argv = {self.data(), filter.data(), disabled.data(), nullptr};
  std::vector<char*> envp;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): environ's own shape.
  for (char** e = environ; *e != nullptr; ++e) envp.push_back(*e);
  envp.push_back(env.data());
  envp.push_back(nullptr);
  posix_spawn_file_actions_t actions{};
  ASSERT_EQ(posix_spawn_file_actions_init(&actions), 0);
  ASSERT_EQ(posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, err.c_str(),
                                             O_WRONLY | O_CREAT | O_TRUNC, 0644),
            0);
  pid_t pid = 0;
  const int spawned = posix_spawnp(&pid, self.c_str(), &actions, nullptr, argv.data(), envp.data());
  posix_spawn_file_actions_destroy(&actions);
  ASSERT_EQ(spawned, 0) << "spawn failed";

  int status = 0;
  pid_t done = 0;
  const auto reap = [&] {
    done = waitpid(pid, &status, WNOHANG);
    return done != 0;
  };
  // The child's start-up, which sanitizers slow, is not the stop's.
  const bool started =
      PollUntil([&] { return std::filesystem::exists(dir.Sub("stopping")) || reap(); }, 120s);
  const bool exited = done != 0 || PollUntil(reap, kStuckStopTimeout + 2s);
  if (!exited) {
    ::kill(pid, SIGKILL);
    (void)waitpid(pid, &status, 0);
  }
  ASSERT_TRUE(started) << "the child never began its stop";
  ASSERT_TRUE(exited) << "the stop outlived its timeout";
  ASSERT_EQ(done, pid);
  ASSERT_TRUE(WIFEXITED(status)) << "status " << status;
  EXPECT_EQ(WEXITSTATUS(status), net::kShutdownTimeoutExitCode);
  std::ifstream in(err);
  const std::string logged((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_NE(logged.find("shutdown timed out"), std::string::npos) << logged;
}
#endif

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
