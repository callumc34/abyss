#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/ops.h"
#include "abyss/engine/io_pool.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/tiering_engine.h"
#include "event_inbox.h"
#include "latch.h"
#include "sequencer_fixture.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using Kind = core::Outcome::Kind;

// Holds the notifier in Post until released: the waiter is unlinked,
// its wake taken and not yet delivered.
class GatedSink final : public core::CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    entered.Open();
    (void)release.Wait();
    const std::scoped_lock lock(mu_);
    result_ = events.back().result;
  }
  core::Result<void> Result() {
    const std::scoped_lock lock(mu_);
    return result_;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  std::mutex mu_;
  core::Result<void> result_;
};

struct Conn {
  Conn(core::CompletionSink& sink, uint64_t id) : sink(&sink), client(id) {}
  core::CompletionSink* sink;
  core::Slot slot;
  std::unique_ptr<core::Task> task;
  uint64_t client;
  uint64_t seq = 0;
};

// The engine's Dispatch over the in-memory queue, whose admission
// window wakes one waiter per opening.
class DispatchUnitTest : public testing::SequencerFixture {
 protected:
  DispatchUnitTest()
      : pool_(IoPoolConfig{.threads = 2}),
        reads_(*hot_, *loader_, pool_, ReadPathConfig{.fill_doorkeeper = false}),
        engine_(reads_, *sequencer_, pool_) {}
  void TearDown() override { pool_.Shutdown(); }

  core::Outcome Dispatch(Conn& conn, std::vector<std::string> args) {
    conn.slot.completion =
        core::Completion{.sink = conn.sink, .client_id = conn.client, .slot_seq = ++conn.seq};
    return engine_.Dispatch(core::Request{.kind = core::Request::Kind::kWrite,
                                          .cmd = core::RespCommand{.args = std::move(args)}},
                            conn.slot, conn.task);
  }
  std::optional<std::string> HotString(std::string_view key) {
    auto read = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    if (!read.result.has_value() || !read.result->IsBulkString()) return std::nullopt;
    return read.result->AsString();
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  IoPool pool_;
  ReadPath reads_;
  TieringEngine engine_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// The window opens and wakes only the front waiter; its connection's
// deadline passes after the notifier took it, before the wake arrives.
// The abandoned write replies at once, then its late wake passes the
// opening on, so the next waiter is admitted with no further opening.
TEST_F(DispatchUnitTest, AnAbandonAfterTheNotifierTookTheWakeHandsItOn) {
  queue_.SetAdmissible(false);
  GatedSink gated;
  abyss::testing::EventInbox inbox;
  Conn first(gated, 1);
  Conn second(inbox, 2);
  ASSERT_EQ(Dispatch(first, {"SET", "a", "1"}).kind, Kind::kPending);
  ASSERT_EQ(Dispatch(second, {"SET", "b", "2"}).kind, Kind::kPending);
  ASSERT_EQ(queue_.AdmissionWaiters(), 2U);

  std::thread notifier([this] { queue_.SetAdmissible(true); });
  ASSERT_TRUE(gated.entered.Wait());
  const auto reply = first.task->Abandon(first.slot);
  ASSERT_FALSE(reply.has_value());
  EXPECT_EQ(reply.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_FALSE(first.task->Quiescent()) << "its wake was taken, so it is owed";

  gated.release.Open();
  notifier.join();
  (void)first.task->Run(first.slot, gated.Result());
  EXPECT_TRUE(first.task->Quiescent());
  EXPECT_EQ(HotString("a"), std::nullopt) << "the abandoned write applied";

  std::optional<core::CompletionEvent> event = inbox.Take(core::SteadyClock::now() + 2s);
  ASSERT_TRUE(event.has_value()) << "the opening died with the abandoned write";
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  core::Outcome admitted = second.task->Run(second.slot, std::move(event->result));
  ASSERT_EQ(admitted.kind, Kind::kReplied);
  EXPECT_EQ(Describe(*admitted.reply), "OK");
  EXPECT_EQ(HotString("b"), "2");
  EXPECT_EQ(queue_.AdmissionWaiters(), 0U);

  // The next command on the abandoned write's connection is unaffected.
  core::Outcome next = Dispatch(first, {"SET", "c", "3"});
  ASSERT_EQ(next.kind, Kind::kReplied);
  EXPECT_EQ(Describe(*next.reply), "OK");
  EXPECT_EQ(PublishedCount(), 2U);
}

}  // namespace
}  // namespace abyss::engine
