#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/request_pipeline.h"
#include "abyss/resp/serializer.h"
#include "event_inbox.h"
#include "mock_queue.h"
#include "scripted_dispatcher.h"

namespace abyss::resp {
namespace {

using namespace std::chrono_literals;
using CloseRequest = RequestPipeline::CloseRequest;

std::string Command(std::initializer_list<std::string> args) {
  const auto bytes = Serializer::SerializeCommand(core::RespCommand{.args = args});
  return {bytes.begin(), bytes.end()};
}
std::string Get(const std::string& key) { return Command({"GET", key}); }
std::string Set(const std::string& key) { return Command({"SET", key, "v"}); }
std::string Bulk(const std::string& value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}
std::span<const uint8_t> Bytes(const std::string& s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

// One reactor's worth, by hand: the connection's pipeline, the sink its
// events land in, and the orphans they may be for.
class AsyncPipelineTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kClient = 7;

  std::unique_ptr<RequestPipeline> Make(PipelineLimits limits = {}) {
    return std::make_unique<RequestPipeline>(
        GlobalRegistry(), ConnectionState{.client_id = kClient},
        PipelineDependencies{.dispatcher = &dispatcher_}, PipelineHost{.sink = &inbox_}, limits);
  }
  // Feeds what is left of `input_`, as a connection does, and keeps the
  // rest for the next feed.
  RequestPipeline::FeedResult Feed(RequestPipeline& pipeline, size_t unwritten = 0) {
    const auto fed = pipeline.Feed(Bytes(input_), unwritten);
    input_.erase(0, fed.consumed);
    return fed;
  }
  std::string Drain(RequestPipeline& pipeline) {
    std::vector<uint8_t> out;
    pipeline.Drain(out);
    return {out.begin(), out.end()};
  }
  // Every event posted so far, routed as the reactor routes them; the
  // count it dropped.
  int Deliver(RequestPipeline* live) {
    int dropped = 0;
    while (auto event = inbox_.Take(core::SteadyClock::now())) {
      if (!orphans_.Route(*event, live)) ++dropped;
    }
    return dropped;
  }
  void Expire(RequestPipeline& pipeline, core::SteadyTime now) {
    if (auto orphan = pipeline.Expire(now)) orphans_.Adopt(std::move(orphan));
  }
  static core::SteadyTime Later() { return core::SteadyClock::now() + 1h; }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<abyss::testing::MockQueue> queue_;
  abyss::testing::ScriptedDispatcher dispatcher_{queue_};
  abyss::testing::EventInbox inbox_;
  Orphans orphans_;
  std::string input_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Ordering ---

TEST_F(AsyncPipelineTest, RepliesLeaveInCommandOrder) {
  auto pipeline = Make();
  input_ = Set("s1") + Get("r1") + Set("s2") + Get("r2");
  const auto fed = Feed(*pipeline);
  EXPECT_FALSE(fed.blocked);
  EXPECT_TRUE(input_.empty()) << "a sequenced command lets the next dispatch";
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"s1", "r1", "s2", "r2"}));
  EXPECT_EQ(Drain(*pipeline), "") << "the head waits to be durable";

  queue_.ReleaseParked(2);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), "") << "a later reply never passes an earlier one";

  queue_.ReleaseParked(1);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("s1") + Bulk("r1") + Bulk("s2") + Bulk("r2"));
  EXPECT_TRUE(pipeline->Quiescent());
}

TEST_F(AsyncPipelineTest, NothingDispatchesBehindAPendingCommand) {
  auto pipeline = Make();
  input_ = Get("p1") + Get("r1") + Set("s1");
  const auto fed = Feed(*pipeline);
  EXPECT_TRUE(fed.blocked);
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"p1"}));
  // The gate stays shut however often it is fed.
  EXPECT_TRUE(Feed(*pipeline).blocked);
  EXPECT_EQ(dispatcher_.dispatched.size(), 1U);

  dispatcher_.WakeLast();
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_FALSE(Feed(*pipeline).blocked);
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"p1", "r1", "s1"}));
  EXPECT_EQ(Drain(*pipeline), Bulk("p1") + Bulk("r1"));
  queue_.ReleaseParked(1);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("s1"));
}

TEST_F(AsyncPipelineTest, APendingCommandThatSequencesOpensTheGate) {
  auto pipeline = Make();
  input_ = Set("ps3") + Get("r1");
  EXPECT_TRUE(Feed(*pipeline).blocked);
  dispatcher_.WakeLast();
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_FALSE(Feed(*pipeline).blocked);
  EXPECT_EQ(Drain(*pipeline), "");
  queue_.ReleaseParked(3);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("ps3") + Bulk("r1"));
}

// --- Caps ---

TEST_F(AsyncPipelineTest, TheSlotCapStopsDispatch) {
  auto pipeline = Make({.max_inflight = 2});
  input_ = Set("s1") + Set("s2") + Get("r1");
  EXPECT_TRUE(Feed(*pipeline).blocked);
  EXPECT_EQ(pipeline->Slots(), 2U);
  EXPECT_EQ(dispatcher_.dispatched.size(), 2U);

  queue_.ReleaseParked(1);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("s1"));
  EXPECT_FALSE(Feed(*pipeline).blocked);
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"s1", "s2", "r1"}));
  queue_.ReleaseParked(2);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("s2") + Bulk("r1"));
}

// Replies held for sequenced commands count with the unwritten bytes.
TEST_F(AsyncPipelineTest, HeldReplyBytesStopDispatch) {
  auto pipeline = Make({.reply_bytes = 12});
  input_ = Set("s1") + Set("s2") + Get("r1");
  EXPECT_TRUE(Feed(*pipeline).blocked);
  EXPECT_EQ(pipeline->ReplyBytes(), 2 * Bulk("s1").size());
  EXPECT_EQ(dispatcher_.dispatched.size(), 2U);
  EXPECT_TRUE(Feed(*pipeline, /*unwritten=*/0).blocked);

  queue_.ReleaseParked(1);
  queue_.ReleaseParked(2);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("s1") + Bulk("s2"));
  EXPECT_TRUE(Feed(*pipeline, /*unwritten=*/12).blocked) << "the connection's unwritten count too";
  EXPECT_FALSE(Feed(*pipeline, 0).blocked);
  EXPECT_EQ(Drain(*pipeline), Bulk("r1"));
}

TEST_F(AsyncPipelineTest, QuitBehindAPendingCommandClosesOnceItReplies) {
  auto pipeline = Make();
  input_ = Get("p1") + Command({"QUIT"}) + Get("r1");
  EXPECT_TRUE(Feed(*pipeline).blocked);
  EXPECT_EQ(pipeline->close_request(), CloseRequest::kNone);
  dispatcher_.WakeLast();
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  (void)Feed(*pipeline);
  EXPECT_EQ(pipeline->close_request(), CloseRequest::kClientQuit);
  EXPECT_EQ(dispatcher_.dispatched, (std::vector<std::string>{"p1"})) << "nothing after QUIT runs";
  EXPECT_EQ(Drain(*pipeline), Bulk("p1") + "+OK\r\n");
  EXPECT_EQ(pipeline->InFlight(), 0U);
}

TEST_F(AsyncPipelineTest, AProtocolErrorBehindPendingCommandsRepliesInOrder) {
  auto pipeline = Make();
  input_ = Set("s1") + Get("p1") + "*-5\r\nGARBAGE\r\n";
  EXPECT_TRUE(Feed(*pipeline).blocked);
  dispatcher_.WakeLast();
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  (void)Feed(*pipeline);
  EXPECT_EQ(pipeline->close_request(), CloseRequest::kProtocolError);
  EXPECT_TRUE(input_.empty());
  EXPECT_EQ(Drain(*pipeline), "") << "behind the sequenced head";
  queue_.ReleaseParked(1);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  const std::string out = Drain(*pipeline);
  EXPECT_EQ(out.rfind(Bulk("s1") + Bulk("p1") + "-ERR Protocol error", 0), 0U) << out;
}

// --- Deadlines ---

TEST_F(AsyncPipelineTest, APendingDeadlineRepliesTheReasonAndNeverApplies) {
  dispatcher_.cancel_fails = true;
  auto pipeline = Make();
  input_ = Set("p1") + Get("r1");
  EXPECT_TRUE(Feed(*pipeline).blocked);
  const auto deadline = pipeline->NextDeadline();
  ASSERT_TRUE(deadline.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  Expire(*pipeline, *deadline - 1ms);
  EXPECT_EQ(Drain(*pipeline), "") << "never before its deadline";

  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  Expire(*pipeline, *deadline);
  EXPECT_EQ(Drain(*pipeline), "-ERR " + dispatcher_.timeout_text + "\r\n");
  EXPECT_EQ(orphans_.size(), 1U) << "its wake is still to land";
  EXPECT_FALSE(Feed(*pipeline).blocked) << "the next command dispatches on a fresh task";
  EXPECT_EQ(Drain(*pipeline), Bulk("r1"));

  dispatcher_.Wake(kClient, 1);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_TRUE(orphans_.empty());
  EXPECT_EQ(dispatcher_.cleanups["p1"], 1);
  EXPECT_EQ(dispatcher_.applied["p1"], 0) << "a command abandoned at its deadline never applies";
}

TEST_F(AsyncPipelineTest, ASequencedDeadlineRepliesTheDurableText) {
  auto pipeline = Make();
  input_ = Set("s4") + Get("r1");
  (void)Feed(*pipeline);
  Expire(*pipeline, Later());
  EXPECT_EQ(Drain(*pipeline),
            "-ERR write durable wait exceeded server timeout; the write is applied and may yet "
            "become durable\r\n" +
                Bulk("r1"));
  EXPECT_EQ(dispatcher_.applied["s4"], 1);
  EXPECT_TRUE(pipeline->Quiescent()) << "its node was unlinked";
}

// --- Late events ---

// An event routes by client and slot: one naming a slot already
// reclaimed never runs the current command's task.
TEST_F(AsyncPipelineTest, AStaleEventForAReclaimedSlotIsDropped) {
  auto pipeline = Make();
  input_ = Get("r1");
  (void)Feed(*pipeline);
  EXPECT_EQ(Drain(*pipeline), Bulk("r1"));
  input_ = Get("p2");
  (void)Feed(*pipeline);
  dispatcher_.Wake(kClient, 1, std::unexpected(core::Error{core::ErrorCode::kInternal, "stale"}));
  EXPECT_EQ(Deliver(pipeline.get()), 1);
  EXPECT_EQ(Drain(*pipeline), "");
  dispatcher_.Wake(kClient, 2);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), Bulk("p2"));
}

// Two tasks of one client orphaned at once, each taking only its own
// slot's late event.
TEST_F(AsyncPipelineTest, OrphansAreKeyedByClientAndSlot) {
  dispatcher_.cancel_fails = true;
  auto pipeline = Make();
  input_ = Set("p1");
  (void)Feed(*pipeline);
  Expire(*pipeline, Later());
  input_ = Set("p2");
  (void)Feed(*pipeline);
  Expire(*pipeline, Later());
  input_ = Set("p3");
  (void)Feed(*pipeline);
  EXPECT_EQ(orphans_.size(), 2U);

  dispatcher_.Wake(kClient, 2);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(dispatcher_.cleanups["p2"], 1);
  EXPECT_EQ(dispatcher_.cleanups["p1"], 0);
  dispatcher_.Wake(kClient, 1);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(dispatcher_.cleanups["p1"], 1);
  EXPECT_TRUE(orphans_.empty());
  EXPECT_EQ(dispatcher_.applied["p3"], 0) << "the live task is untouched";

  dispatcher_.Wake(kClient, 3);
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline), "-ERR " + dispatcher_.timeout_text + "\r\n-ERR " +
                                  dispatcher_.timeout_text + "\r\n" + Bulk("p3"));
}

// A closed connection's pending command runs on to its outcome; its
// pipeline waits with the orphans and goes once nothing is owed it.
TEST_F(AsyncPipelineTest, AClosedPipelineIsKeptUntilItsLastEventLands) {
  auto pipeline = Make();
  input_ = Set("s1") + Set("p1");
  (void)Feed(*pipeline);
  pipeline->Close();
  EXPECT_FALSE(pipeline->Quiescent());
  orphans_.Adopt(std::move(pipeline));
  EXPECT_EQ(orphans_.size(), 1U);

  dispatcher_.WakeLast();
  EXPECT_EQ(Deliver(nullptr), 0);
  EXPECT_EQ(dispatcher_.applied["p1"], 1) << "dispatched, so it runs to its outcome";
  EXPECT_TRUE(orphans_.empty());
}

TEST_F(AsyncPipelineTest, AClosedPipelinesDeadlineEndsItsPendingCommand) {
  dispatcher_.cancel_fails = true;
  auto pipeline = Make();
  input_ = Set("p1");
  (void)Feed(*pipeline);
  pipeline->Close();
  orphans_.Adopt(std::move(pipeline));
  orphans_.Expire(kClient, Later());
  EXPECT_EQ(orphans_.size(), 1U) << "its wake is still to land";
  dispatcher_.WakeLast();
  EXPECT_EQ(Deliver(nullptr), 0);
  EXPECT_EQ(dispatcher_.applied["p1"], 0);
  EXPECT_TRUE(orphans_.empty());
}

// --- The ring ---

TEST_F(AsyncPipelineTest, TheRingGrowsToTheCapAndGivesItBackWhenEmpty) {
  constexpr size_t kCap = 1024;
  auto pipeline = Make({.max_inflight = kCap});
  EXPECT_EQ(pipeline->RingCapacityForTesting(), 0U);
  for (size_t i = 0; i < kCap; ++i) input_ += Set("s" + std::to_string(i));
  EXPECT_FALSE(Feed(*pipeline).blocked);
  EXPECT_EQ(pipeline->Slots(), kCap);
  EXPECT_EQ(pipeline->RingCapacityForTesting(), kCap);
  // What a connection whose ring once filled holds, before and after.
  RecordProperty("slot_bytes", std::to_string(RequestPipeline::EntryBytesForTesting()));
  std::cout << "[ measure ] a full ring: " << kCap << " slots of "
            << RequestPipeline::EntryBytesForTesting() << " B\n";
  size_t written = 0;
  for (size_t i = 0; i < kCap; ++i) {
    queue_.ReleaseParked(static_cast<core::ShardId>(i));
    written += Bulk("s" + std::to_string(i)).size();
  }
  EXPECT_EQ(Deliver(pipeline.get()), 0);
  EXPECT_EQ(Drain(*pipeline).size(), written);
  EXPECT_EQ(pipeline->Slots(), 0U);
  EXPECT_LE(pipeline->RingCapacityForTesting(), 16U) << "an idle connection keeps a few";
}

}  // namespace
}  // namespace abyss::resp
