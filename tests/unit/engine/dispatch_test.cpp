#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
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
  DispatchUnitTest() { Wire(); }
  void TearDown() override { Unwire(); }

  void Wire() {
    pool_ = std::make_unique<IoPool>(IoPoolConfig{.threads = 2});
    reads_ = std::make_unique<ReadPath>(*hot_, *loader_, *pool_,
                                        ReadPathConfig{.fill_doorkeeper = false});
    engine_ = std::make_unique<TieringEngine>(*reads_, *sequencer_, *pool_);
  }
  void Unwire() {
    if (pool_) pool_->Shutdown();
    engine_.reset();
    reads_.reset();
    pool_.reset();
  }
  void Rebuild(Options options) {
    Unwire();
    Build(options);
    Wire();
  }

  core::Outcome Dispatch(Conn& conn, std::vector<std::string> args) {
    conn.slot.completion =
        core::Completion{.sink = conn.sink, .client_id = conn.client, .slot_seq = ++conn.seq};
    return engine_->Dispatch(core::Request{.kind = core::Request::Kind::kWrite,
                                           .cmd = core::RespCommand{.args = std::move(args)}},
                             conn.slot, conn.task);
  }
  std::optional<std::string> HotString(std::string_view key) {
    auto read = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    if (!read.result.has_value() || !read.result->IsBulkString()) return std::nullopt;
    return read.result->AsString();
  }
  // Fails each of the next `times` reservations with `code`.
  void FailReserves(core::ErrorCode code, int times) {
    auto left = std::make_shared<std::atomic<int>>(times);
    queue_.SetReserveFault(
        // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
        [left, code](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
          if (left->fetch_sub(1) <= 0) return std::nullopt;
          return core::Error{code, "refused"};
        });
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<IoPool> pool_;
  std::unique_ptr<ReadPath> reads_;
  std::unique_ptr<TieringEngine> engine_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// What a write waits for that one event makes room for one waiter.
enum class Room : uint8_t { kAdmission, kSpare, kBackpressure };

class HandOffTest : public DispatchUnitTest, public ::testing::WithParamInterface<Room> {
 protected:
  // Two writes made to wait, on keys of shard 0.
  void Block() {
    switch (GetParam()) {
      case Room::kAdmission:
        queue_.SetAdmissible(false);
        return;
      case Room::kSpare:
        FailReserves(core::ErrorCode::kUnavailable, 2);
        queue_.SetSpareReady(false);
        return;
      case Room::kBackpressure:
        // 16 KiB a shard, over its limit past 20 KiB, nothing drained.
        Rebuild(Options{.max_memory_bytes = 64 << 10, .drain_gated = true});
        for (int i = 0; i < 3; ++i) {
          ASSERT_EQ(Reply({"SET", KeyOn(0, i, "fill"), std::string(8 << 10, 'f')}), "OK");
        }
        return;
    }
  }
  size_t Waiters() const {
    switch (GetParam()) {
      case Room::kAdmission:
        return queue_.AdmissionWaiters();
      case Room::kSpare:
        return queue_.SpareWaiters();
      case Room::kBackpressure:
        return router_.RoomWaiters();
    }
    return 0;
  }
  // Room for one: the front waiter only is woken.
  void MakeRoomForOne() {
    switch (GetParam()) {
      case Room::kAdmission:
        queue_.SetAdmissible(true);
        return;
      case Room::kSpare:
        queue_.ReadySpareOne();
        return;
      case Room::kBackpressure:
        router_.Advance(100);
        return;
    }
  }
};

// Room comes and wakes only the front waiter; its connection's deadline
// passes after the notifier took it, before the wake arrives. The
// abandoned write replies at once, then its late wake passes the room
// on, so the next waiter goes on with no more room made.
TEST_P(HandOffTest, AnAbandonAfterTheNotifierTookTheWakeHandsItOn) {
  ASSERT_NO_FATAL_FAILURE(Block());
  const size_t published = PublishedCount();
  GatedSink gated;
  abyss::testing::EventInbox inbox;
  Conn first(gated, 1);
  Conn second(inbox, 2);
  const std::string a = KeyOn(0, 0, "a");
  const std::string b = KeyOn(0, 0, "b");
  ASSERT_EQ(Dispatch(first, {"SET", a, "1"}).kind, Kind::kPending);
  ASSERT_EQ(Dispatch(second, {"SET", b, "2"}).kind, Kind::kPending);
  ASSERT_EQ(Waiters(), 2U);

  std::thread notifier([this] { MakeRoomForOne(); });
  ASSERT_TRUE(gated.entered.Wait());
  const auto reply = first.task->Abandon(first.slot);
  EXPECT_TRUE(!reply.has_value() || reply->IsError()) << "the abandoned write replied ok";
  EXPECT_FALSE(first.task->Quiescent()) << "its wake was taken, so it is owed";

  gated.release.Open();
  notifier.join();
  (void)first.task->Run(first.slot, gated.Result());
  EXPECT_TRUE(first.task->Quiescent());
  EXPECT_EQ(HotString(a), std::nullopt) << "the abandoned write applied";

  std::optional<core::CompletionEvent> event = inbox.Take(core::SteadyClock::now() + 2s);
  ASSERT_TRUE(event.has_value()) << "the room died with the abandoned write";
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  core::Outcome next = second.task->Run(second.slot, std::move(event->result));
  ASSERT_EQ(next.kind, Kind::kReplied);
  EXPECT_EQ(Describe(*next.reply), "OK");
  EXPECT_EQ(HotString(b), "2");
  EXPECT_EQ(Waiters(), 0U);

  // The next command on the abandoned write's connection is unaffected.
  core::Outcome later = Dispatch(first, {"SET", KeyOn(1, 0, "c"), "3"});
  ASSERT_EQ(later.kind, Kind::kReplied);
  EXPECT_EQ(Describe(*later.reply), "OK");
  EXPECT_EQ(PublishedCount(), published + 2);
}

INSTANTIATE_TEST_SUITE_P(Rooms, HandOffTest,
                         ::testing::Values(Room::kAdmission, Room::kSpare, Room::kBackpressure),
                         [](const ::testing::TestParamInfo<Room>& info) {
                           switch (info.param) {
                             case Room::kAdmission:
                               return std::string("Admission");
                             case Room::kSpare:
                               return std::string("Spare");
                             case Room::kBackpressure:
                               return std::string("Backpressure");
                           }
                           return std::string("Unknown");
                         });

#ifndef NDEBUG
// A task carries one command at a time: the next starts only once the
// last is quiescent.
TEST_F(DispatchUnitTest, ACommandStartedOnABusyTaskIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        queue_.SetAdmissible(false);
        abyss::testing::EventInbox inbox;
        Conn conn(inbox, 1);
        (void)Dispatch(conn, {"SET", "a", "1"});
        (void)Dispatch(conn, {"SET", "b", "2"});
      },
      "a command started on a busy task");
}
#endif

}  // namespace
}  // namespace abyss::engine
