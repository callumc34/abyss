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
core::Request HGet(std::string key, std::string field) {
  return Req(core::Request::Kind::kRead, "HGET", {"HGET", std::move(key), std::move(field)});
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
      ++calls_;
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
  int Calls() {
    const std::scoped_lock lock(mu_);
    return calls_;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  std::mutex mu_;
  std::string key_;
  int pass_ = 0;
  int calls_ = 0;
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
    hooked_.SetAfterHook([this](std::string_view key) { after_gate_->Hook(key); });
  }
  void TearDown() override {
    gate_->release.Open();
    after_gate_->release.Open();
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
  // Every post still to come for `conn`'s command, taken as its loop
  // would, with the pool run dry between them.
  void Drain(Conn& conn) {
    for (;;) {
      io_pool_->WaitIdleForTesting();
      std::optional<core::CompletionEvent> event = conn.inbox.Take(core::SteadyClock::now() + 50ms);
      if (!event.has_value()) return;
      if (event->slot_seq != conn.seq) continue;
      if (event->kind == core::CompletionKind::kDurable) {
        (void)conn.slot.durable.Arrived(std::move(event->result));
      } else {
        (void)conn.task->Run(conn.slot, std::move(event->result));
      }
    }
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
  static double Fills(metrics::FillOutcome outcome) {
    return metrics::testing::GetCounterValue(metrics::names::kHotFillsTotal, outcome).value_or(0);
  }
  static double AllFills() {
    double all = 0;
    for (uint8_t i = 0; i <= static_cast<uint8_t>(metrics::FillOutcome::kExpired); ++i) {
      all += Fills(static_cast<metrics::FillOutcome>(i));
    }
    return all;
  }
  // How many cold reads an HGET miss makes before its fill reads the
  // hash once, measured on a hash of its own.
  int AnswerReads(Conn& conn) {
    Cold({"HSET", "qa", "f", "1", "g", "2"});
    const int before = cold_calls_q_.load();
    EXPECT_EQ(Run(conn, HGet("qa", "f")), "1");
    io_pool_->WaitIdleForTesting();
    return cold_calls_q_.load() - before - 1;
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
  // As gate_, once the cold call has read.
  std::unique_ptr<ColdGate> after_gate_ = std::make_unique<ColdGate>();
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
  io_pool_->WaitIdleForTesting();
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
  Conn conn(1);
  const int answer_reads = AnswerReads(conn);
  ASSERT_GE(answer_reads, 1);
  Cold({"HSET", "qb", "f", "1", "g", "2"});

  // Hold the fill: the reply comes with it held.
  gate_->Hold("qb", answer_reads);
  EXPECT_EQ(Run(conn, HGet("qb", "f")), "1");
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

// --- A fill overlapping the next command on its key ---

// The fill holds the key's placeholder from before its read, so a write
// that reads the key waits on it, then goes on over what it installed.
// XX makes the SET read the key.
TEST_F(DispatchTest, AWriteWaitsOnAFillHoldingItsKey) {
  Open(Options{.fill_doorkeeper = false});
  Conn reader(1);
  const int answer_reads = AnswerReads(reader);
  Cold({"HSET", "qk", "f", "1", "g", "2"});
  after_gate_->Hold("qk", answer_reads);
  EXPECT_EQ(Run(reader, HGet("qk", "f")), "1");
  ASSERT_TRUE(after_gate_->entered.Wait()) << "no fill ran";
  const size_t logged = Logged(ShardOf("qk")).size();

  Conn writer(2);
  core::Request set_xx = WriteReq({"SET", "qk", "v2", "XX"});
  set_xx.flags = core::PredicateFlags::kXx;
  core::Outcome set = Dispatch(writer, std::move(set_xx));
  ASSERT_EQ(set.kind, Kind::kPending) << "the write went on under the fill";
  EXPECT_EQ(hot_->LoadWaitersForTesting("qk"), 1U);
  EXPECT_EQ(Logged(ShardOf("qk")).size(), logged);
  after_gate_->release.Open();
  EXPECT_EQ(Text(Finish(writer, std::move(set))), "OK");
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(Run(reader, Get("qk")), "v2");
  EXPECT_EQ(Text(hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = "qk"}}).result), "v2");
}

// A blind write waits on no load: it ends the fill's placeholder, so
// the fill installs nothing, though the write has since left hot and
// what the fill read is all that would answer for the key there.
TEST_F(DispatchTest, AFillOvertakenByABlindWriteInstallsNothing) {
  Open(Options{.fill_doorkeeper = false});
  Conn reader(1);
  const int answer_reads = AnswerReads(reader);
  Cold({"HSET", "qk", "f", "1", "g", "2"});
  after_gate_->Hold("qk", answer_reads);
  EXPECT_EQ(Run(reader, HGet("qk", "f")), "1");
  ASSERT_TRUE(after_gate_->entered.Wait()) << "no fill ran";
  const double discarded = Fills(metrics::FillOutcome::kDiscarded);

  Conn writer(2);
  EXPECT_EQ(Run(writer, WriteReq({"SET", "qk", "v2"})), "OK");
  DrainToCold(ShardOf("qk"));
  EvictDrained();
  ASSERT_FALSE(Resident("qk"));
  after_gate_->release.Open();
  io_pool_->WaitIdleForTesting();
  EXPECT_FALSE(Resident("qk")) << "the fill installed the state the write replaced";
  EXPECT_EQ(Fills(metrics::FillOutcome::kDiscarded), discarded + 1);
  EXPECT_EQ(Run(reader, Get("qk")), "v2");
}

// The write lands before the fill is dequeued: the fill finds the key
// resident and skips it, reading nothing and counting no outcome.
TEST_F(DispatchTest, AFillAfterTheWriteFindsItsKeyResident) {
  Open(Options{.fill_doorkeeper = false});
  Cold({"HSET", "qk", "f", "1", "g", "2"});
  gate_->Hold("qk");
  Conn reader(1);
  core::Outcome miss = Dispatch(reader, HGet("qk", "f"));
  ASSERT_EQ(miss.kind, Kind::kPending);
  ASSERT_TRUE(gate_->entered.Wait());
  // Both threads busy once the miss's job ends, so the fill it queues
  // waits behind them.
  Blocker first;
  Blocker second;
  io_pool_->Submit(first, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  ASSERT_TRUE(first.started.Wait());
  io_pool_->Submit(second, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  gate_->release.Open();
  EXPECT_EQ(Text(Finish(reader, std::move(miss))), "1");
  ASSERT_TRUE(second.started.Wait());
  const uint64_t jobs = PoolJobs();
  const int cold_reads = cold_calls_q_.load();
  const double fills = AllFills();

  Conn writer(2);
  EXPECT_EQ(Run(writer, WriteReq({"SET", "qk", "v2"})), "OK");
  first.release.Open();
  second.release.Open();
  ASSERT_TRUE(first.done.Wait());
  ASSERT_TRUE(second.done.Wait());
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(PoolJobs(), jobs + 1) << "no fill ran";
  EXPECT_EQ(cold_calls_q_.load(), cold_reads) << "the fill read cold";
  EXPECT_EQ(AllFills(), fills);
  EXPECT_EQ(Run(writer, Get("qk")), "v2");
  EXPECT_TRUE(Resident("qk"));
}

// --- A write's loads on the pool ---

// At the deadline the write is abandoned with its load running: it
// replies at once, and is never applied. The load lands later and
// installs, as its token still allows, ending the placeholder a keyed waiter is linked on.
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

// --- Joins of a load in flight ---

// A miss that finds another's load of its key in flight joins it and
// ends: the pool thread is free at once. With two threads, one held
// by the load of K and eight misses of K joined to it, a miss of J is
// answered while K's read is still held; then each joiner takes the
// one read of K.
TEST_F(DispatchTest, AJoinOfALoadInFlightNeverHoldsAPoolThread) {
  constexpr int kJoiners = 8;
  Open(Options{.cold_io_threads = 2});
  Cold({"SET", "fk", "v"});
  Cold({"SET", "fj", "w"});
  gate_->Hold("fk");
  Conn leader(1);
  core::Outcome led = Dispatch(leader, Get("fk"));
  ASSERT_EQ(led.kind, Kind::kPending);
  ASSERT_TRUE(gate_->entered.Wait());
  std::vector<std::unique_ptr<Conn>> joiners;
  std::vector<core::Outcome> joined;
  for (int i = 0; i < kJoiners; ++i) {
    joiners.push_back(std::make_unique<Conn>(10 + i));
    joined.push_back(Dispatch(*joiners.back(), Get("fk")));
    ASSERT_EQ(joined.back().kind, Kind::kPending);
  }
  const core::ShardId shard = ShardOf("fk");
  for (int i = 0; i < 2000 && loader_->JoinersForTesting(shard) < kJoiners; ++i) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(loader_->JoinersForTesting(shard), static_cast<size_t>(kJoiners));

  Conn other(2);
  const auto start = core::SteadyClock::now();
  EXPECT_EQ(Run(other, Get("fj")), "w");
  EXPECT_LT(core::SteadyClock::now() - start, 1s) << "a joiner held the other pool thread";
  EXPECT_EQ(gate_->Calls(), 1) << "K's read was not still held";

  gate_->release.Open();
  EXPECT_EQ(Text(Finish(leader, std::move(led))), "v");
  for (int i = 0; i < kJoiners; ++i) {
    const auto index = static_cast<size_t>(i);
    EXPECT_EQ(Text(Finish(*joiners[index], std::move(joined[index]))), "v") << i;
  }
  EXPECT_EQ(gate_->Calls(), 1) << "a joiner read K again";
  EXPECT_EQ(loader_->JoinersForTesting(shard), 0U);
  EXPECT_EQ(loader_->FlightsForTesting(shard), 0U);
}

// The deadline comes for a joiner armed on a load as the load lands:
// whichever wins, no joiner is left linked and the task is quiescent.
TEST_F(DispatchTest, AnAbandonRacingTheLandingLeavesNoJoiner) {
  Open();
  std::mt19937 rng(11);  // NOLINT(bugprone-random-generator-seed)
  for (int round = 0; round < 20; ++round) {
    const std::string key = "fr" + std::to_string(round);
    Cold({"SET", key, "v"});
    gate_ = std::make_unique<ColdGate>();
    gate_->Hold(key);
    Conn leader(100 + round);
    core::Outcome led = Dispatch(leader, Get(key));
    ASSERT_EQ(led.kind, Kind::kPending);
    ASSERT_TRUE(gate_->entered.Wait());
    Conn joiner(200 + round);
    ASSERT_EQ(Dispatch(joiner, Get(key)).kind, Kind::kPending);
    // Its job's post: the join is made, and the task arms it.
    std::optional<core::CompletionEvent> ended = joiner.inbox.Take(core::SteadyClock::now() + 5s);
    ASSERT_TRUE(ended.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
    ASSERT_EQ(joiner.task->Run(joiner.slot, std::move(ended->result)).kind, Kind::kPending);
    ASSERT_FALSE(joiner.Quiescent()) << "armed on the load";

    const auto delay = std::chrono::microseconds(std::uniform_int_distribution<int>(0, 300)(rng));
    std::thread releaser([this, delay] {
      std::this_thread::sleep_for(delay);
      gate_->release.Open();
    });
    std::this_thread::sleep_for(150us);
    EXPECT_FALSE(joiner.task->Abandon(joiner.slot).has_value());
    releaser.join();
    EXPECT_EQ(Text(Finish(leader, std::move(led))), "v");
    Drain(joiner);
    EXPECT_TRUE(joiner.Quiescent()) << "round " << round;
    EXPECT_EQ(loader_->JoinersForTesting(ShardOf(key)), 0U) << "round " << round;
  }
}

// --- Fan-out ---

TEST_F(DispatchTest, AFanOutWithSeveralMissesIsOnePoolJob) {
  Open();
  Cold({"MSET", "ma", "1", "mb", "2", "mc", "3"});
  Conn conn(1);
  io_pool_->WaitIdleForTesting();
  uint64_t jobs = PoolJobs();
  EXPECT_EQ(Run(conn, Req(core::Request::Kind::kFanOut, "MGET", {"MGET", "ma", "mb", "mc"},
                          core::MultiKeyKind::kMget)),
            "[1,2,3]");
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(PoolJobs() - jobs, 1U);
  // The doorkeeper filled none of them on a first miss: EXISTS misses
  // all three again, in one job too.
  jobs = PoolJobs();
  EXPECT_EQ(Run(conn, Req(core::Request::Kind::kFanOut, "EXISTS", {"EXISTS", "ma", "nowhere", "mc"},
                          core::MultiKeyKind::kExists)),
            ":2");
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(PoolJobs() - jobs, 1U);
}

// Durability is monotonic per shard, so the fences a fan-out gathers
// fold to one per shard however many keys it read.
TEST_F(DispatchTest, AFanOutsFencesFoldToOnePerShard) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  const std::string a = KeyOn(0, 0);
  const std::string b = KeyOn(0, 1);
  const std::string c = KeyOn(1, 0);
  Conn writer(1);
  for (const std::string& key : {a, b, c}) {
    ASSERT_EQ(Dispatch(writer, WriteReq({"SET", key, "v"})).kind, Kind::kSequenced);
    EXPECT_FALSE(writer.slot.durable.Expire().has_value()) << "it became durable while stalled";
    Drain(writer);
  }
  Conn reader(2);
  core::Outcome read = Dispatch(reader, Req(core::Request::Kind::kFanOut, "MGET",
                                            {"MGET", a, b, c, a}, core::MultiKeyKind::kMget));
  ASSERT_EQ(read.kind, Kind::kSequenced);
  EXPECT_EQ(reader.slot.durable.StreamsForTesting(), 2U) << "one fence per shard";
  stall.Release();
  EXPECT_EQ(Durable(reader), "[v,v,v,v]");
}

// A fan-out that fails without reading its keys shows no state, so it
// does not wait on the fences it gathered: its error replies at once.
TEST_F(DispatchTest, AFanOutsErrorSkipsItsFences) {
  Open(Options{.durability = core::Durability::kPowerLoss, .command_timeout = 300ms});
  ASSERT_NO_FATAL_FAILURE(Cold({"SET", "fc", "1"}));
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  Conn writer(1);
  ASSERT_EQ(Dispatch(writer, WriteReq({"SET", "fa", "v"})).kind, Kind::kSequenced);
  Blocker first;
  Blocker second;
  io_pool_->Submit(first, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  io_pool_->Submit(second, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  ASSERT_TRUE(first.started.Wait());
  ASSERT_TRUE(second.started.Wait());
  Conn reader(2);
  core::Outcome read = Dispatch(reader, Req(core::Request::Kind::kFanOut, "MGET",
                                            {"MGET", "fa", "fc"}, core::MultiKeyKind::kMget));
  ASSERT_EQ(read.kind, Kind::kPending) << "fa's hit, then fc's miss queued";
  std::this_thread::sleep_until(reader.slot.deadline + 20ms);
  first.release.Open();
  std::optional<core::CompletionEvent> event = reader.inbox.Take(core::SteadyClock::now() + 5s);
  ASSERT_TRUE(event.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  core::Outcome replied = reader.task->Run(reader.slot, std::move(event->result));
  ASSERT_EQ(replied.kind, Kind::kReplied) << "it waited on fa's fence";
  EXPECT_EQ(Text(replied.reply), "error: command deadline passed while queued for cold I/O");
  EXPECT_FALSE(reader.slot.durable.waiting());
  second.release.Open();
  ASSERT_TRUE(second.done.Wait());
  EXPECT_FALSE(writer.slot.durable.Expire().has_value()) << "it became durable while stalled";
  stall.Release();
  Drain(writer);
}

// A fill queued behind foreground work past the deadline of the read
// that admitted it never runs, and counts as expired.
TEST_F(DispatchTest, AFillPastItsDeadlineInTheQueueExpires) {
  Open(Options{.fill_doorkeeper = false, .command_timeout = 300ms, .cold_io_threads = 2});
  Cold({"HSET", "eh", "f", "1"});
  const auto expired = [] {
    return metrics::testing::GetCounterValue(metrics::names::kHotFillsTotal,
                                             metrics::FillOutcome::kExpired)
        .value_or(0);
  };
  const double before = expired();
  Blocker held;
  io_pool_->Submit(held, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  ASSERT_TRUE(held.started.Wait());
  Conn conn(1);
  core::Outcome miss = Dispatch(conn, Req(core::Request::Kind::kRead, "HGET", {"HGET", "eh", "f"}));
  ASSERT_EQ(miss.kind, Kind::kPending);
  // Behind the miss on the one free thread, ahead of its fill.
  Blocker after;
  io_pool_->Submit(after, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  EXPECT_EQ(Text(Finish(conn, std::move(miss))), "1");
  ASSERT_TRUE(after.started.Wait());
  std::this_thread::sleep_until(conn.slot.deadline + 20ms);
  held.release.Open();
  after.release.Open();
  io_pool_->WaitIdleForTesting();
  EXPECT_EQ(expired(), before + 1);
  EXPECT_FALSE(hot_->Read(core::ops::ReadOp{core::ops::HashLen{.key = "eh"}}).result.has_value());
}

// --- Shutdown ---

// The pool stops with a write's load job still queued: the job fails
// unrun, still ending its placeholder, so the write waiting on that
// key goes on, and either replies or meets the stopped pool itself.
TEST_F(DispatchTest, ShutdownFreesTheKeysOfQueuedLoadJobs) {
  Open();
  Cold({"SADD", "ss", "a"});
  Blocker first;
  Blocker second;
  io_pool_->Submit(first, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  io_pool_->Submit(second, metrics::IoKind::kPoint, core::SteadyClock::now() + 30s);
  ASSERT_TRUE(first.started.Wait());
  ASSERT_TRUE(second.started.Wait());
  Conn owner(1);
  core::Outcome loading = Dispatch(owner, WriteReq({"SADD", "ss", "b"}));
  ASSERT_EQ(loading.kind, Kind::kPending);
  Conn waiter(2);
  core::Outcome waiting = Dispatch(waiter, WriteReq({"SADD", "ss", "c"}));
  ASSERT_EQ(waiting.kind, Kind::kPending);
  ASSERT_EQ(hot_->LoadWaitersForTesting("ss"), 1U);

  std::thread stopper([this] { io_pool_->Shutdown(); });
  for (int i = 0; i < 2000 && hot_->LoadWaitersForTesting("ss") > 0; ++i) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(hot_->LoadWaitersForTesting("ss"), 0U) << "the failed job held its key";
  first.release.Open();
  second.release.Open();
  stopper.join();
  EXPECT_EQ(Text(Finish(owner, std::move(loading))), "error: cold I/O pool shut down");
  const std::string waited = Text(Finish(waiter, std::move(waiting)));
  EXPECT_TRUE(waited == ":1" || waited == "error: cold I/O pool shut down") << waited;
  EXPECT_FALSE(Member("ss", "b"));
  EXPECT_FALSE(hot_->LoadPending("ss"));
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
  // Whatever it went on to do, let it finish.
  Drain(conn);
  EXPECT_FALSE(Member("s", "b")) << "the abandoned write was applied";
  EXPECT_EQ(Logged(ShardOf("s")).size(), 1U) << "the abandoned write was logged";
  EXPECT_TRUE(conn.Quiescent());
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
