#include <gtest/gtest.h>

#include "abyss/engine/tiering_engine.h"

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/durability.h"
#include "abyss/core/result.h"
#include "abyss/engine/io_pool.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "event_inbox.h"
#include "fatal_capture.h"
#include "hooked_cold_store.h"
#include "latch.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using Kind = core::Outcome::Kind;

// One connection's state, driven by hand: its events, its slot and the
// task it reuses.
struct Conn {
  explicit Conn(uint64_t id) : client(id) {}
  // Declared in the order they must outlive one another.
  abyss::testing::EventInbox inbox;
  core::Slot slot;
  std::unique_ptr<core::Task> task;
  uint64_t client;
  uint64_t seq = 0;

  bool Quiescent() const {
    return (task == nullptr || task->Quiescent()) && slot.durable.Quiescent();
  }
};

core::Request Req(core::Request::Kind kind, std::string_view name, std::vector<std::string> args,
                  core::MultiKeyKind multi = core::MultiKeyKind::kNone) {
  return core::Request{.kind = kind,
                       .name = name,
                       .multi_key = multi,
                       .cmd = core::RespCommand{.args = std::move(args)}};
}
core::Request WriteReq(std::vector<std::string> args) {
  return Req(core::Request::Kind::kWrite, "", std::move(args));
}
core::Request Get(std::string key) {
  return Req(core::Request::Kind::kRead, "GET", {"GET", std::move(key)});
}

// Blocks every cold call for `key` the hook sees until released, and
// records the thread it ran on.
class ColdGate {
 public:
  void Hold(std::string key, int pass_first = 0) {
    const std::scoped_lock lock(mu_);
    key_ = std::move(key);
    pass_ = pass_first;
  }
  void Hook(std::string_view key) {
    {
      const std::scoped_lock lock(mu_);
      if (key != key_) return;
      thread_ = std::this_thread::get_id();
      if (pass_ > 0) {
        --pass_;
        return;
      }
    }
    entered.Open();
    (void)release.Wait(30s);
  }
  std::thread::id Thread() {
    const std::scoped_lock lock(mu_);
    return thread_;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  std::mutex mu_;
  std::string key_;
  int pass_ = 0;
  std::thread::id thread_;
};

// Holds a pool thread until released.
class Blocker final : public IoJob {
 public:
  void Run() noexcept override {
    started.Open();
    (void)release.Wait(30s);
    done.Open();
  }
  void Fail(core::Error /*error*/) noexcept override { done.Open(); }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch started;
  abyss::testing::Latch release;
  abyss::testing::Latch done;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// The engine's Dispatch over the real log, hot, cold and I/O pool: what
// a connection drives, one command at a time or by hand.
class DispatchTest : public SequencedEngineTest {
 protected:
  void SetUp() override {
    loader_cold_ = &hooked_;
    hooked_.SetHook([this](std::string_view key) {
      if (key.starts_with("q")) cold_calls_q_.fetch_add(1);
      gate_->Hook(key);
    });
  }
  void TearDown() override {
    gate_->release.Open();
    SequencedEngineTest::TearDown();
  }

  core::Outcome Dispatch(Conn& conn, core::Request request) {
    conn.slot.completion =
        core::Completion{.sink = &conn.inbox, .client_id = conn.client, .slot_seq = ++conn.seq};
    return engine_->Dispatch(std::move(request), conn.slot, conn.task);
  }
  // On from `outcome` as a connection's loop goes: its events until it
  // replies or its deadline passes, then on until it is quiescent.
  static core::Result<core::RespValue> Finish(Conn& conn, core::Outcome outcome) {
    std::optional<core::Result<core::RespValue>> reply;
    bool pending = outcome.kind == Kind::kPending;
    if (outcome.kind == Kind::kReplied) reply = std::move(outcome.reply);
    while (!reply.has_value() || !conn.Quiescent()) {
      std::optional<core::CompletionEvent> event =
          conn.inbox.Take(reply.has_value() ? std::nullopt : std::optional(conn.slot.deadline));
      if (!event.has_value()) {
        reply = pending ? conn.task->Abandon(conn.slot) : conn.slot.durable.Expire();
        continue;
      }
      if (event->slot_seq != conn.seq) continue;
      if (event->kind == core::CompletionKind::kDurable) {
        auto released = conn.slot.durable.Arrived(std::move(event->result));
        if (released.has_value() && !reply.has_value()) reply = *std::move(released);
        continue;
      }
      core::Outcome next = conn.task->Run(conn.slot, std::move(event->result));
      if (reply.has_value()) continue;
      pending = next.kind == Kind::kPending;
      if (next.kind == Kind::kReplied) reply = std::move(next.reply);
    }
    return *std::move(reply);
  }
  std::string Run(Conn& conn, core::Request request) {
    return Text(Finish(conn, Dispatch(conn, std::move(request))));
  }
  static std::string Text(const core::Result<core::RespValue>& reply) {
    if (!reply.has_value()) return "error: " + reply.error().message();
    if (reply->IsArray()) {
      std::string out = "[";
      for (const auto& item : reply->AsArray()) {
        if (out.size() > 1) out += ",";
        out += Describe(item);
      }
      return out + "]";
    }
    return Describe(*reply);
  }
  // Writes `args`, then sends what they wrote to cold and out of hot.
  void Cold(std::vector<std::string> args) {
    ASSERT_EQ(Write(std::move(args)).rfind("error", 0), std::string::npos);
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
    EvictDrained();
  }
  uint64_t PoolJobs() {
    return metrics::testing::GetHistogramCount(metrics::names::kEngineIoWaitSeconds).value_or(0);
  }
  // A sequenced reply's events, with the stall released.
  std::string Durable(Conn& conn) {
    return Text(Finish(conn, core::Outcome{.kind = Kind::kSequenced}));
  }
  bool Member(std::string_view key, std::string_view member) {
    auto read = hot_->Read(core::ops::ReadOp{core::ops::SetIsMember{.key = key, .member = member}});
    return read.result.has_value() && read.result->IsInteger() && read.result->AsInteger() == 1;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<ColdGate> gate_ = std::make_unique<ColdGate>();
  // Cold reads of keys starting "q".
  std::atomic<int> cold_calls_q_{0};
  abyss::testing::HookedColdStore hooked_{[this]() -> core::ColdStore& { return *cold_; }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Outcomes ---

TEST_F(DispatchTest, EveryCommandClassReturnsItsOutcome) {
  Open();
  Conn conn(1);
  core::Outcome set = Dispatch(conn, WriteReq({"SET", "k", "v"}));
  EXPECT_EQ(set.kind, Kind::kReplied) << "a resident write under process_crash";
  EXPECT_EQ(Text(set.reply), "OK");
  const core::Task* task = conn.task.get();
  ASSERT_NE(task, nullptr);

  core::Outcome hit = Dispatch(conn, Get("k"));
  EXPECT_EQ(hit.kind, Kind::kReplied);
  EXPECT_EQ(Text(hit.reply), "v");

  Cold({"SET", "c", "1"});
  core::Outcome miss = Dispatch(conn, Get("c"));
  EXPECT_EQ(miss.kind, Kind::kPending);
  EXPECT_EQ(Text(Finish(conn, std::move(miss))), "1");

  core::Outcome mget = Dispatch(conn, Req(core::Request::Kind::kFanOut, "MGET", {"MGET", "k", "c"},
                                          core::MultiKeyKind::kMget));
  EXPECT_EQ(mget.kind, Kind::kPending) << "c is still in cold alone";
  EXPECT_EQ(Text(Finish(conn, std::move(mget))), "[v,1]");

  core::Outcome mset = Dispatch(conn, Req(core::Request::Kind::kFanOut, "MSET",
                                          {"MSET", "a", "1", "b", "2"}, core::MultiKeyKind::kMset));
  EXPECT_EQ(mset.kind, Kind::kReplied);
  EXPECT_EQ(Text(mset.reply), "OK");

  core::Outcome flush = Dispatch(conn, Req(core::Request::Kind::kFlush, "FLUSHDB", {"FLUSHDB"}));
  EXPECT_EQ(flush.kind, Kind::kReplied);
  EXPECT_EQ(Text(flush.reply), "OK");
  EXPECT_EQ(Run(conn, Get("k")), "nil");
  EXPECT_EQ(conn.task.get(), task) << "one task, reused";
}

TEST_F(DispatchTest, UnderPowerLossWritesAndFencedHitsAreSequenced) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  Conn writer(1);
  Conn reader(2);
  Conn flusher(3);
  EXPECT_EQ(Dispatch(writer, WriteReq({"SET", "k", "v"})).kind, Kind::kSequenced);
  EXPECT_EQ(Dispatch(reader, Get("k")).kind, Kind::kSequenced)
      << "the hit shows a write not yet durable";
  EXPECT_EQ(Dispatch(flusher, Req(core::Request::Kind::kFlush, "FLUSHDB", {"FLUSHDB"})).kind,
            Kind::kSequenced);
  EXPECT_EQ(reader.inbox.Pending(), 0U);
  stall.Release();
  EXPECT_EQ(Durable(writer), "OK");
  EXPECT_EQ(Durable(reader), "v");
  EXPECT_EQ(Durable(flusher), "OK") << "every shard's stream posted";
}

// --- The read split ---

TEST_F(DispatchTest, AHitNeverTouchesThePool) {
  Open();
  Conn writer(1);
  ASSERT_EQ(Run(writer, WriteReq({"SET", "k", "v"})), "OK");
  ASSERT_EQ(Run(writer, WriteReq({"SET", "j", "w"})), "OK");
  const uint64_t jobs = PoolJobs();
  Conn reader(2);
  core::Outcome hit = Dispatch(reader, Get("k"));
  EXPECT_EQ(hit.kind, Kind::kReplied);
  EXPECT_EQ(Text(hit.reply), "v");
  EXPECT_EQ(reader.task, nullptr) << "a hit needs no task";
  core::Outcome mget = Dispatch(reader, Req(core::Request::Kind::kFanOut, "MGET",
                                            {"MGET", "k", "j"}, core::MultiKeyKind::kMget));
  EXPECT_EQ(mget.kind, Kind::kReplied);
  EXPECT_EQ(Text(mget.reply), "[v,w]");
  EXPECT_EQ(PoolJobs(), jobs) << "no job was queued";
}

TEST_F(DispatchTest, AMissAnswersOnThePool) {
  Open();
  Cold({"SET", "k", "v"});
  gate_->Hold("k");
  Conn conn(1);
  core::Outcome miss = Dispatch(conn, Get("k"));
  ASSERT_EQ(miss.kind, Kind::kPending) << "dispatch returned with the cold read held";
  ASSERT_TRUE(gate_->entered.Wait());
  EXPECT_NE(gate_->Thread(), std::this_thread::get_id());
  EXPECT_EQ(conn.inbox.Pending(), 0U);
  gate_->release.Open();
  EXPECT_EQ(Text(Finish(conn, std::move(miss))), "v");
}

// The fence is data the dispatcher registers: without the registration
// the hit would reply before what it shows is durable.
TEST_F(DispatchTest, AFenceRegisteredThroughWhenDurableGatesTheReply) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  Conn writer(1);
  Conn reader(2);
  ASSERT_EQ(Dispatch(writer, WriteReq({"SET", "k", "v"})).kind, Kind::kSequenced);
  core::Outcome read = Dispatch(reader, Get("k"));
  ASSERT_EQ(read.kind, Kind::kSequenced) << "replied before the write it shows was durable";
  EXPECT_TRUE(reader.slot.durable.waiting());
  EXPECT_FALSE(reader.inbox.Take(core::SteadyClock::now() + 50ms).has_value());
  stall.Release();
  EXPECT_EQ(Durable(reader), "v");
  EXPECT_EQ(Durable(writer), "OK");
}

TEST_F(DispatchTest, AFillRunsAfterTheReply) {
  Open(Options{.fill_doorkeeper = false});
  Cold({"HSET", "qa", "f", "1", "g", "2"});
  Cold({"HSET", "qb", "f", "1", "g", "2"});
  // How many cold reads an HGET miss and then its fill make: the fill
  // reads the hash once.
  Conn conn(1);
  int before = cold_calls_q_.load();
  EXPECT_EQ(Run(conn, Req(core::Request::Kind::kRead, "HGET", {"HGET", "qa", "f"})), "1");
  io_pool_->WaitIdleForTesting();
  const int answer_reads = cold_calls_q_.load() - before - 1;
  ASSERT_GE(answer_reads, 1);

  // Hold the fill: the reply comes with it held.
  gate_->Hold("qb", answer_reads);
  EXPECT_EQ(Run(conn, Req(core::Request::Kind::kRead, "HGET", {"HGET", "qb", "f"})), "1");
  ASSERT_TRUE(gate_->entered.Wait()) << "no fill ran";
  auto read = hot_->Read(core::ops::ReadOp{core::ops::HashLen{.key = "qb"}});
  EXPECT_FALSE(read.result.has_value()) << "filled before the reply";
  gate_->release.Open();
  io_pool_->WaitIdleForTesting();
  read = hot_->Read(core::ops::ReadOp{core::ops::HashLen{.key = "qb"}});
  ASSERT_TRUE(read.result.has_value());
  EXPECT_EQ(read.result->AsInteger(), 2);
}

TEST_F(DispatchTest, ADroppedFillChangesNoAnswer) {
  Open(Options{.fill_doorkeeper = false, .io_background_capacity = 0});
  Cold({"HSET", "h", "f", "1", "g", "2"});
  const double dropped = metrics::testing::GetCounterValue(metrics::names::kHotFillsTotal,
                                                           metrics::FillOutcome::kDropped)
                             .value_or(0);
  Conn conn(1);
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(Run(conn, Req(core::Request::Kind::kRead, "HGET", {"HGET", "h", "f"})), "1");
    EXPECT_EQ(Run(conn, Req(core::Request::Kind::kRead, "HLEN", {"HLEN", "h"})), ":2");
  }
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kHotFillsTotal,
                                              metrics::FillOutcome::kDropped)
                .value_or(0),
            dropped + 4);
  EXPECT_FALSE(hot_->Read(core::ops::ReadOp{core::ops::HashLen{.key = "h"}}).result.has_value());
}

// --- A write's loads on the pool ---

// At the deadline the write is abandoned with its load running: it
// replies at once, and is never applied. The load lands later and
// installs (N2), ending the placeholder a keyed waiter is linked on.
TEST_F(DispatchTest, ADeadlineWhileALoadRunsInstallsItAndFreesTheKey) {
  Open();
  Cold({"SADD", "s", "a"});
  gate_->Hold("s");
  Conn late(1);
  core::Outcome load = Dispatch(late, WriteReq({"SADD", "s", "b"}));
  ASSERT_EQ(load.kind, Kind::kPending);
  ASSERT_TRUE(gate_->entered.Wait());

  Conn next(2);
  std::string next_reply;
  std::thread waiter([&] { next_reply = Run(next, WriteReq({"SADD", "s", "c"})); });
  for (int i = 0; i < 1000 && hot_->LoadWaitersForTesting("s") == 0; ++i) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(hot_->LoadWaitersForTesting("s"), 1U) << "the next write waits on the key";

  // The deadline, as the connection's timer would fire it.
  const auto reply = late.task->Abandon(late.slot);
  EXPECT_EQ(Text(reply), "error: write timed out loading its keys");
  EXPECT_FALSE(late.Quiescent()) << "its load job is still running";

  gate_->release.Open();
  std::optional<core::CompletionEvent> event = late.inbox.Take(core::SteadyClock::now() + 10s);
  ASSERT_TRUE(event.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  (void)late.task->Run(late.slot, std::move(event->result));
  EXPECT_TRUE(late.Quiescent());
  waiter.join();
  EXPECT_EQ(next_reply, ":1");
  EXPECT_TRUE(Member("s", "a")) << "the abandoned write's load installed";
  EXPECT_TRUE(Member("s", "c"));
  EXPECT_FALSE(Member("s", "b")) << "the abandoned write applied";
  const auto logged = Logged(ShardOf("s"));
  ASSERT_EQ(logged.size(), 2U);
  EXPECT_EQ(ArgsOf(logged[1]), (std::vector<std::string>{"SADD", "s", "c"}));
}

// The load's post is in the inbox before the connection reaches it,
// and the deadline passes in between: the attempt it runs only cleans
// up. Then the same race, released at random around the abandon.
TEST_F(DispatchTest, ALoadCompletionRacingAbandonNeverApplies) {
  Open(Options{.command_timeout = 200ms});
  Cold({"SADD", "s", "a"});
  {
    Conn conn(1);
    core::Outcome load = Dispatch(conn, WriteReq({"SADD", "s", "b"}));
    ASSERT_EQ(load.kind, Kind::kPending);
    while (conn.inbox.Pending() == 0) std::this_thread::sleep_for(1ms);
    std::this_thread::sleep_until(conn.slot.deadline + 5ms);
    EXPECT_EQ(Text(Finish(conn, std::move(load))), "error: write timed out loading its keys");
    EXPECT_TRUE(Member("s", "a"));
    EXPECT_FALSE(Member("s", "b"));
  }
  // Seeded, so a failing schedule repeats.
  std::mt19937 rng(7);  // NOLINT(bugprone-random-generator-seed)
  for (int round = 0; round < 20; ++round) {
    const std::string key = "r" + std::to_string(round);
    Cold({"SADD", key, "a"});
    gate_->Hold(key);
    Conn conn(10 + round);
    core::Outcome load = Dispatch(conn, WriteReq({"SADD", key, "b"}));
    ASSERT_EQ(load.kind, Kind::kPending);
    ASSERT_TRUE(gate_->entered.Wait());
    const auto delay = std::chrono::microseconds(std::uniform_int_distribution<int>(0, 400)(rng));
    std::thread releaser([this, delay] {
      std::this_thread::sleep_for(delay);
      gate_->release.Open();
    });
    std::this_thread::sleep_for(200us);
    const auto reply = conn.task->Abandon(conn.slot);
    releaser.join();
    EXPECT_EQ(Text(reply), "error: write timed out loading its keys");
    std::optional<core::CompletionEvent> event = conn.inbox.Take(core::SteadyClock::now() + 10s);
    ASSERT_TRUE(event.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
    (void)conn.task->Run(conn.slot, std::move(event->result));
    EXPECT_TRUE(conn.Quiescent());
    EXPECT_FALSE(Member(key, "b")) << "round " << round;
    gate_ = std::make_unique<ColdGate>();
  }
  EXPECT_EQ(Logged(ShardOf("s")).size() + Logged(ShardOf("r0")).size() > 0, true);
}

// A load job dequeued past its deadline never reads cold, but still
// ends its placeholder: a write waiting on the key goes on at once,
// though the abandoned write's connection has not yet seen its post.
TEST_F(DispatchTest, AnExpiredLoadJobFreesTheKeyAtOnce) {
  Open(Options{.command_timeout = 1s});
  Cold({"SADD", "s", "a"});
  Blocker first;
  Blocker second;
  io_pool_->Submit(first, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  io_pool_->Submit(second, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  ASSERT_TRUE(first.started.Wait());
  ASSERT_TRUE(second.started.Wait());

  Conn expired(1);
  core::Outcome load = Dispatch(expired, WriteReq({"SADD", "s", "b"}));
  ASSERT_EQ(load.kind, Kind::kPending) << "its load job is queued behind the blockers";
  std::this_thread::sleep_for(500ms);
  Conn next(2);
  std::atomic<bool> done{false};
  std::string next_reply;
  std::thread waiter([&] {
    next_reply = Run(next, WriteReq({"SADD", "s", "c"}));
    done = true;
  });
  std::this_thread::sleep_until(expired.slot.deadline + 50ms);
  first.release.Open();
  const auto freed = core::SteadyClock::now();
  // Before the expired write's own connection takes its post.
  while (!done && core::SteadyClock::now() < next.slot.deadline - 100ms) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done) << "the key stayed held after its load expired";
  EXPECT_LT(core::SteadyClock::now() - freed, 300ms);
  waiter.join();
  EXPECT_EQ(next_reply, ":1") << "it loaded the key itself";

  EXPECT_EQ(Text(Finish(expired, std::move(load))), "error: write timed out loading its keys");
  EXPECT_FALSE(Member("s", "b"));
  EXPECT_TRUE(Member("s", "c"));
  second.release.Open();
  ASSERT_TRUE(first.done.Wait());
  ASSERT_TRUE(second.done.Wait());
}

// Abandoned while its load job is still queued, a write takes the job
// back: it never runs or posts, the task is quiescent at once, and the
// placeholder it held is released, so a write waiting on the key goes on.
TEST_F(DispatchTest, AbandonTakesBackAQueuedLoadJobAndFreesItsKey) {
  constexpr int kWrites = 3;
  Open();
  for (int i = 0; i < kWrites; ++i) Cold({"SADD", "q" + std::to_string(i), "a"});
  const int cold_before = cold_calls_q_.load();
  Blocker first;
  Blocker second;
  io_pool_->Submit(first, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  io_pool_->Submit(second, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  ASSERT_TRUE(first.started.Wait());
  ASSERT_TRUE(second.started.Wait());

  std::vector<std::unique_ptr<Conn>> abandoned;
  std::vector<std::unique_ptr<Conn>> waiters;
  std::vector<core::Outcome> waiting;
  for (int i = 0; i < kWrites; ++i) {
    const std::string key = "q" + std::to_string(i);
    abandoned.push_back(std::make_unique<Conn>(100 + i));
    ASSERT_EQ(Dispatch(*abandoned.back(), WriteReq({"SADD", key, "b"})).kind, Kind::kPending);
    waiters.push_back(std::make_unique<Conn>(200 + i));
    waiting.push_back(Dispatch(*waiters.back(), WriteReq({"SADD", key, "c"})));
    ASSERT_EQ(waiting.back().kind, Kind::kPending);
    ASSERT_EQ(hot_->LoadWaitersForTesting(key), 1U);
  }
  for (int i = 0; i < kWrites; ++i) {
    Conn& conn = *abandoned[static_cast<size_t>(i)];
    EXPECT_EQ(Text(conn.task->Abandon(conn.slot)), "error: write timed out loading its keys");
    EXPECT_TRUE(conn.Quiescent()) << "its job was still queued, so nothing is owed";
    EXPECT_EQ(hot_->LoadWaitersForTesting("q" + std::to_string(i)), 0U) << "the key stayed held";
  }
  first.release.Open();
  second.release.Open();
  for (int i = 0; i < kWrites; ++i) {
    const auto index = static_cast<size_t>(i);
    EXPECT_EQ(Text(Finish(*waiters[index], std::move(waiting[index]))), ":1");
    EXPECT_EQ(abandoned[index]->inbox.Pending(), 0U) << "a cancelled job posted";
    EXPECT_FALSE(Member("q" + std::to_string(i), "b"));
    EXPECT_TRUE(Member("q" + std::to_string(i), "c"));
  }
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(cold_calls_q_.load() - cold_before, kWrites) << "only the waiters read cold";
}

// --- The task and its slots ---

// The connection's one task moves on to the next command while the
// slots of earlier ones hold their own durable waits.
TEST_F(DispatchTest, SequencedWritesKeepTheirDurableStateAcrossTheNextDispatch) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  abyss::testing::EventInbox inbox;
  std::unique_ptr<core::Task> task;
  std::vector<std::unique_ptr<core::Slot>> slots;
  const auto dispatch = [&](core::Request request) {
    slots.push_back(std::make_unique<core::Slot>());
    core::Slot& slot = *slots.back();
    slot.completion = core::Completion{.sink = &inbox, .client_id = 1, .slot_seq = slots.size()};
    return engine_->Dispatch(std::move(request), slot, task);
  };
  ASSERT_EQ(dispatch(WriteReq({"SET", KeyOn(0), "a"})).kind, Kind::kSequenced);
  const core::Task* first = task.get();
  ASSERT_EQ(dispatch(WriteReq({"SET", KeyOn(1), "b"})).kind, Kind::kSequenced);
  ASSERT_EQ(dispatch(Req(core::Request::Kind::kFanOut, "MSET",
                         {"MSET", KeyOn(2), "c", KeyOn(3), "d"}, core::MultiKeyKind::kMset))
                .kind,
            Kind::kSequenced);
  EXPECT_EQ(task.get(), first);
  EXPECT_TRUE(task->Quiescent());
  for (const auto& slot : slots) EXPECT_TRUE(slot->durable.waiting());

  stall.Release();
  std::vector<std::string> replies(slots.size());
  size_t open = slots.size();
  while (open > 0) {
    std::optional<core::CompletionEvent> event = inbox.Take(core::SteadyClock::now() + 10s);
    ASSERT_TRUE(event.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
    ASSERT_EQ(event->kind, core::CompletionKind::kDurable);
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
    auto reply = slots.at(event->slot_seq - 1)->durable.Arrived(std::move(event->result));
    if (reply.has_value()) {
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
      replies.at(event->slot_seq - 1) = Text(*reply);
      --open;
    }
  }
  EXPECT_EQ(replies, (std::vector<std::string>{"OK", "OK", "OK"}));
  for (const auto& slot : slots) EXPECT_TRUE(slot->durable.Quiescent());
}

// Abandoned before its deadline (a client gone), a write whose wake
// then comes ok is never applied: the resumed task checks first.
TEST_F(DispatchTest, AResumedTaskChecksForAbandonmentFirst) {
  Open();
  Cold({"SADD", "s", "a"});
  const hot::LoadStart theirs = hot_->BeginLoad("s");
  ASSERT_TRUE(theirs.started());
  Conn conn(1);
  core::Outcome wait = Dispatch(conn, WriteReq({"SADD", "s", "b"}));
  ASSERT_EQ(wait.kind, Kind::kPending) << "waiting on their load";
  // Their load ends, so the wake is posted before the abandon.
  hot_->AbortLoad("s", theirs.token);
  while (conn.inbox.Pending() == 0) std::this_thread::sleep_for(1ms);
  EXPECT_EQ(Text(conn.task->Abandon(conn.slot)),
            "error: write timed out awaiting a load of its key");
  std::optional<core::CompletionEvent> event = conn.inbox.Take(core::SteadyClock::now() + 1s);
  ASSERT_TRUE(event.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  ASSERT_TRUE(event->result.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  (void)conn.task->Run(conn.slot, std::move(event->result));
  EXPECT_TRUE(conn.Quiescent());
  EXPECT_FALSE(Member("s", "b"));
  EXPECT_EQ(Logged(ShardOf("s")).size(), 1U);
  EXPECT_EQ(Run(conn, WriteReq({"SADD", "s", "c"})), ":1")
      << "the task goes on to the next command";
}

#ifndef NDEBUG
TEST_F(DispatchTest, AResumeRunsOnlyOnItsOwnersThread) {
  Open();
  Cold({"SET", "k", "v"});
  Conn conn(1);
  core::Outcome miss = Dispatch(conn, Get("k"));
  ASSERT_EQ(miss.kind, Kind::kPending);
  std::optional<core::CompletionEvent> event = conn.inbox.Take(core::SteadyClock::now() + 10s);
  ASSERT_TRUE(event.has_value());
  std::string reason;
  std::thread other([&] {
    const abyss::testing::ScopedFatalCapture capture;
    try {
      (void)conn.task->Run(conn.slot, event->result);
    } catch (const abyss::testing::FatalCalled& fatal) {
      reason = fatal.reason;
    }
  });
  other.join();
  EXPECT_EQ(reason, "a task resumed off its owner's thread");
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(Text(conn.task->Run(conn.slot, std::move(event->result)).reply), "v");
}
#endif

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
