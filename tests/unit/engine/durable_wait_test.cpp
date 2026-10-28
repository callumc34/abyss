#include "abyss/core/durable_wait.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>

#include "abyss/core/completion.h"
#include "abyss/core/durability.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "event_inbox.h"
#include "latch.h"
#include "mock_queue.h"

namespace abyss::core {
namespace {

using namespace std::chrono_literals;
using ::testing::_;

// A queue on which every registered seq waits until released.
class DurableWaitTest : public ::testing::Test {
 protected:
  DurableWaitTest() {
    ON_CALL(queue_, WhenDurable(_, _, _))
        .WillByDefault([this](ShardId shard, Durability, SeqWaiterNode& node) {
          return queue_.ParkDurable(shard, node);
        });
  }
  Completion To(CompletionSink& sink) const {
    return Completion{.sink = &sink, .client_id = 1, .slot_seq = 1};
  }
  // Every post `inbox` holds, through Arrived; the reply if one came.
  std::optional<Result<RespValue>> Drain(abyss::testing::EventInbox& inbox, size_t posts) {
    std::optional<Result<RespValue>> reply;
    for (size_t i = 0; i < posts; ++i) {
      std::optional<CompletionEvent> event = inbox.Take(SteadyClock::now() + 5s);
      EXPECT_TRUE(event.has_value());
      if (!event.has_value()) break;
      EXPECT_EQ(event->kind, CompletionKind::kDurable);
      auto released = wait_.Arrived(std::move(event->result));
      if (released.has_value()) {
        EXPECT_FALSE(reply.has_value()) << "released twice";
        reply = std::move(released);
      }
    }
    return reply;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<abyss::testing::MockQueue> queue_;
  abyss::testing::EventInbox inbox_;
  DurableWait wait_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(DurableWaitTest, TheReplyGoesOnceEveryStreamHasPosted) {
  wait_.Begin(queue_, DurableWait::Kind::kWrite, To(inbox_), 3, RespValue::SimpleString("OK"));
  for (ShardId shard = 0; shard < 3; ++shard) wait_.Add(shard, 5);
  EXPECT_FALSE(wait_.Ready().has_value());
  EXPECT_FALSE(wait_.Quiescent());
  queue_.ReleaseDurable();
  for (int i = 0; i < 2; ++i) {
    std::optional<CompletionEvent> event = inbox_.Take(SteadyClock::now() + 5s);
    ASSERT_TRUE(event.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
    EXPECT_FALSE(wait_.Arrived(std::move(event->result)).has_value()) << "a stream is left";
  }
  auto reply = Drain(inbox_, 1);
  ASSERT_TRUE(reply.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  ASSERT_TRUE(reply->has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ((*reply)->AsString(), "OK");
  EXPECT_TRUE(wait_.Quiescent());
}

TEST_F(DurableWaitTest, WhatIsDurableAlreadyRepliesAtOnceAndLinksNothing) {
  ON_CALL(queue_, WhenDurable(_, _, _)).WillByDefault([](ShardId, Durability, SeqWaiterNode&) {
    return std::optional<Result<void>>(std::in_place);
  });
  wait_.Begin(queue_, DurableWait::Kind::kFence, To(inbox_), 2, RespValue::Integer(1));
  wait_.Add(0, 3);
  wait_.Add(1, 4);
  auto reply = wait_.Ready();
  ASSERT_TRUE(reply.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ((*reply)->AsInteger(), 1);
  EXPECT_TRUE(wait_.Quiescent());
  EXPECT_EQ(inbox_.Pending(), 0U);
}

TEST_F(DurableWaitTest, ExpireCancelsWhatIsLinkedAndRepliesItsKindsTimeout) {
  const std::array<std::pair<DurableWait::Kind, std::string>, 3> kinds = {{
      {DurableWait::Kind::kWrite,
       "write durable wait exceeded server timeout; the write is applied and may yet become "
       "durable"},
      {DurableWait::Kind::kFlush,
       "flush durable wait exceeded server timeout; retry to complete the wipe"},
      {DurableWait::Kind::kFence,
       "durable wait exceeded server timeout; the reply would show a write not yet durable"},
  }};
  for (const auto& [kind, text] : kinds) {
    wait_.Begin(queue_, kind, To(inbox_), 2, RespValue::SimpleString("OK"));
    wait_.Add(0, 7);
    wait_.Add(1, 7);
    ASSERT_FALSE(wait_.Ready().has_value());
    const auto reply = wait_.Expire();
    ASSERT_FALSE(reply.has_value());
    EXPECT_EQ(reply.error().code(), ErrorCode::kTimeout);
    EXPECT_EQ(reply.error().message(), text);
    EXPECT_TRUE(wait_.Quiescent()) << "both nodes were still linked";
  }
  queue_.ReleaseDurable();
  EXPECT_EQ(inbox_.Pending(), 0U) << "a cancelled node posted";
}

TEST_F(DurableWaitTest, AFailedStreamRepliesItsErrorAndCancelsTheRest) {
  wait_.Begin(queue_, DurableWait::Kind::kWrite, To(inbox_), 2, RespValue::SimpleString("OK"));
  wait_.Add(0, 9);
  wait_.Add(1, 9);
  ASSERT_FALSE(wait_.Ready().has_value());
  queue_.FailParked(0, Error{ErrorCode::kUnavailable, "stopped"});
  std::optional<CompletionEvent> event = inbox_.Take(SteadyClock::now() + 5s);
  ASSERT_TRUE(event.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  auto reply = wait_.Arrived(std::move(event->result));
  ASSERT_TRUE(reply.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  ASSERT_FALSE(reply->has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(reply->error().code(), ErrorCode::kUnavailable);
  EXPECT_TRUE(wait_.Quiescent()) << "the other stream's node was cancelled";
  queue_.ReleaseDurable();
  EXPECT_EQ(inbox_.Pending(), 0U);
}

// Holds the notifier in Post until released.
class GatedSink final : public CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  void Post(std::span<const CompletionEvent> events) noexcept override {
    entered.Open();
    (void)release.Wait();
    inbox.Post(events);
  }
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  abyss::testing::EventInbox inbox;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// The notifier has taken the node when the deadline comes: the timeout
// replies at once, and the wait is quiescent only once that post lands.
TEST_F(DurableWaitTest, AnExpireThatLosesToTheNotifierAbsorbsItsLatePost) {
  GatedSink gated;
  wait_.Begin(queue_, DurableWait::Kind::kWrite, To(gated), 1, RespValue::SimpleString("OK"));
  wait_.Add(0, 3);
  ASSERT_FALSE(wait_.Ready().has_value());
  std::thread notifier([this] { queue_.ReleaseDurable(); });
  ASSERT_TRUE(gated.entered.Wait());
  const auto reply = wait_.Expire();
  ASSERT_FALSE(reply.has_value());
  EXPECT_EQ(reply.error().code(), ErrorCode::kTimeout);
  EXPECT_FALSE(wait_.Quiescent()) << "its post is on the way";
  gated.release.Open();
  notifier.join();
  std::optional<CompletionEvent> event = gated.inbox.Take(SteadyClock::now() + 5s);
  ASSERT_TRUE(event.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_FALSE(wait_.Arrived(std::move(event->result)).has_value()) << "replied twice";
  EXPECT_TRUE(wait_.Quiescent());
}

TEST_F(DurableWaitTest, ItsNodesGrowOnceAndAreReused) {
  EXPECT_EQ(wait_.CapacityForTesting(), 1U) << "a single stream needs nothing grown";
  for (const size_t streams : {4U, 1U, 4U, 2U}) {
    wait_.Begin(queue_, DurableWait::Kind::kWrite, To(inbox_), streams, RespValue::Integer(0));
    for (size_t i = 0; i < streams; ++i) wait_.Add(static_cast<ShardId>(i), 2);
    ASSERT_FALSE(wait_.Ready().has_value());
    queue_.ReleaseDurable();
    EXPECT_TRUE(Drain(inbox_, streams).has_value());
    EXPECT_TRUE(wait_.Quiescent());
    EXPECT_EQ(wait_.CapacityForTesting(), 4U);
  }
}

#ifndef NDEBUG
// A wait with a node still linked must not be freed: its notifier
// would post through freed memory.
TEST_F(DurableWaitTest, FreedWithANodeLinkedIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        DurableWait linked;
        linked.Begin(queue_, DurableWait::Kind::kWrite, To(inbox_), 1, RespValue::Integer(0));
        linked.Add(0, 3);
      },
      "a durable wait freed with a node linked or a post on its way");
}
#endif

}  // namespace
}  // namespace abyss::core
