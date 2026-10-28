#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"
#include "abyss/engine/pending_write.h"
#include "abyss/engine/sequencer.h"
#include "abyss/metrics/names.h"
#include "fatal_capture.h"
#include "latch.h"
#include "recording_sink.h"
#include "sequencer_fixture.h"

namespace abyss::engine {
namespace {

using metrics::RedecideReason;
using namespace std::chrono_literals;

// A sink whose Post holds the notifier until released: the waiter is
// unlinked, its wake not yet posted.
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

class SequencerAttemptTest : public testing::SequencerFixture {
 protected:
  // A write whose wakes this test's sink collects, as slot `slot`.
  std::unique_ptr<PendingWrite> Write(std::vector<std::string> args,
                                      std::chrono::milliseconds timeout = 5s, uint64_t slot = 0) {
    return std::make_unique<PendingWrite>(core::RespCommand{.args = std::move(args)},
                                          core::PredicateFlags::kNone, sink_.For(slot),
                                          core::SteadyClock::now() + timeout);
  }
  // An attempt; a linearised write hands its reply to durable_, and
  // replies now if it is durable already.
  Step Attempt(PendingWrite& write) {
    Step step = sequencer_->Attempt(write);
    if (step.kind != Step::Kind::kSequenced) return step;
    auto now = sequencer_->Sequence(write, durable_, write.completion());
    if (!now.has_value()) return step;
    return Step{.kind = Step::Kind::kReplied, .reply = *std::move(now)};
  }
  // The next attempt, once the `n`th post has come.
  Step Resume(PendingWrite& write, size_t n) {
    if (!sink_.WaitFor(n)) return Step{.kind = Step::Kind::kWait};
    write.Woken(sink_.Events().at(n - 1).result);
    return Attempt(write);
  }
  static std::string Replied(const Step& step) {
    if (step.kind != Step::Kind::kReplied) return "not replied";
    if (!step.reply.has_value()) return "error: " + step.reply.error().message();
    return Describe(*step.reply);
  }
  uint64_t Redecides(RedecideReason reason) const {
    return sequencer_->Snapshot().redecides.at(static_cast<size_t>(reason));
  }
  void FailReserves(core::ErrorCode code, int times) {
    auto left = std::make_shared<std::atomic<int>>(times);
    queue_.SetReserveFault(
        // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
        [left, code](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
          if (left->fetch_sub(1) <= 0) return std::nullopt;
          return core::Error{code, "refused"};
        });
  }
  std::optional<std::string> HotString(std::string_view key) {
    auto read = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    if (!read.result.has_value() || !read.result->IsBulkString()) return std::nullopt;
    return read.result->AsString();
  }
  std::string HotRead(const core::ops::ReadOp& op) {
    auto read = hot_->Read(op);
    if (!read.result.has_value()) return "error: " + read.result.error().message();
    return Describe(*read.result);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::RecordingSink sink_;
  core::DurableWait durable_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Admission ---

TEST_F(SequencerAttemptTest, AnAdmissionWaitRegistersResumesAndCompletes) {
  queue_.SetAdmissible(false);
  auto write = Write({"SET", "k", "v"});
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  EXPECT_EQ(step.reason, WaitReason::kAdmission);
  EXPECT_EQ(step.shard, ShardOf("k"));
  EXPECT_EQ(queue_.AdmissionWaiters(), 1U) << "registered before it returned";
  EXPECT_EQ(HotString("k"), std::nullopt);

  queue_.SetAdmissible(true);
  step = Resume(*write, 1);
  EXPECT_EQ(sink_.Events().at(0).kind, core::CompletionKind::kResume);
  EXPECT_EQ(Replied(step), "OK");
  EXPECT_EQ(HotString("k"), "v");
  EXPECT_EQ(queue_.AdmissionWaiters(), 0U);
}

// Refused under the lock, a write waits for room, then decides again.
TEST_F(SequencerAttemptTest, AnAdmissionWaitAfterARefusalReDecides) {
  auto left = std::make_shared<std::atomic<int>>(1);
  queue_.SetReserveFault(
      // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
      [this, left](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
        if (left->fetch_sub(1) <= 0) return std::nullopt;
        queue_.SetAdmissible(false);
        return core::Error{core::ErrorCode::kResourceExhausted, "window full"};
      });
  auto write = Write({"SET", "k", "v"});
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  EXPECT_EQ(step.reason, WaitReason::kAdmission);
  EXPECT_EQ(Redecides(RedecideReason::kAdmission), 1U);
  EXPECT_EQ(HotString("k"), std::nullopt) << "the refused decision was restored, not applied";

  queue_.SetAdmissible(true);
  EXPECT_EQ(Replied(Resume(*write, 1)), "OK");
  EXPECT_EQ(Logged(ShardOf("k")), (std::vector<std::vector<std::string>>{{"SET", "k", "v"}}));
}

TEST_F(SequencerAttemptTest, AnAdmissionWaitTimesOutUnappliedWithTheWindowText) {
  Build(Options{.command_timeout = 50ms});
  queue_.SetAdmissible(false);
  auto result = Run({"SET", "k", "v"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_TRUE(result.error().message().starts_with(
      "WAL durability window full: the device is not keeping up with writes"))
      << result.error().message();
  EXPECT_EQ(queue_.AdmissionWaiters(), 0U);
  EXPECT_EQ(PublishedCount(), 0U);
  EXPECT_EQ(HotString("k"), std::nullopt);
}

// N attempts wait on one window. Opening it wakes one; each that passes
// wakes the next, so all complete with no further notify.
TEST_F(SequencerAttemptTest, OneOpeningAdmitsEveryWaiterByHandOff) {
  constexpr size_t kWriters = 100;
  queue_.SetAdmissible(false);
  std::vector<std::unique_ptr<PendingWrite>> writes;
  for (size_t i = 0; i < kWriters; ++i) {
    writes.push_back(Write(
        {"SET", KeyOn(static_cast<core::ShardId>(i % kShards), static_cast<int>(i)), "v"}, 10s, i));
    ASSERT_EQ(Attempt(*writes.back()).kind, Step::Kind::kWait);
  }
  ASSERT_EQ(queue_.AdmissionWaiters(), kWriters);

  queue_.SetAdmissible(true);
  for (size_t seen = 0; seen < kWriters; ++seen) {
    ASSERT_TRUE(sink_.WaitFor(seen + 1)) << "the baton stopped after " << seen;
    const core::CompletionEvent event = sink_.Events().at(seen);
    PendingWrite& write = *writes.at(event.slot_seq);
    write.Woken(event.result);
    EXPECT_EQ(Replied(Attempt(write)), "OK");
  }
  EXPECT_EQ(sink_.Events().size(), kWriters) << "one wake each";
  EXPECT_EQ(queue_.AdmissionWaiters(), 0U);
  EXPECT_EQ(PublishedCount(), kWriters);
}

// --- Spare ---

TEST_F(SequencerAttemptTest, ASpareWaitRegistersResumesReDecidesAndCompletes) {
  FailReserves(core::ErrorCode::kUnavailable, 1);
  queue_.SetSpareReady(false);
  auto write = Write({"SET", "k", "a"});
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  EXPECT_EQ(step.reason, WaitReason::kSpare);
  EXPECT_EQ(step.shard, ShardOf("k"));
  EXPECT_EQ(queue_.SpareWaiters(), 1U);

  queue_.SetSpareReady(true);
  EXPECT_EQ(Replied(Resume(*write, 1)), "OK");
  EXPECT_EQ(Redecides(RedecideReason::kSpare), 1U);
  EXPECT_EQ(Logged(ShardOf("k")), (std::vector<std::vector<std::string>>{{"SET", "k", "a"}}));
}

TEST_F(SequencerAttemptTest, ASpareWaitTimesOutUnappliedWithTheSpareText) {
  Build(Options{.command_timeout = 50ms});
  FailReserves(core::ErrorCode::kUnavailable, 1'000'000);
  queue_.SetSpareReady(false);
  auto result = Run({"SET", "k", "v"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(result.error().message(), "write timed out waiting for a spare WAL segment");
  EXPECT_EQ(queue_.SpareWaiters(), 0U);
  EXPECT_EQ(PublishedCount(), 0U);
  EXPECT_EQ(HotString("k"), std::nullopt);
}

// --- Another caller's load ---

TEST_F(SequencerAttemptTest, AForeignLoadWaitResumesReDecidesAndCompletes) {
  PutCold("k", core::ColdKeyState{.type = core::KeyType::kSet, .value = core::StringSet{"a"}});
  const hot::LoadStart theirs = hot_->BeginLoad("k");
  ASSERT_TRUE(theirs.started());
  auto write = Write({"SADD", "k", "b"});
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  EXPECT_EQ(step.reason, WaitReason::kForeignLoad);
  EXPECT_EQ(step.key, "k");
  EXPECT_EQ(hot_->LoadWaitersForTesting("k"), 1U);

  // Theirs ends without installing, so this write loads the key itself.
  hot_->AbortLoad("k", theirs.token);
  step = Resume(*write, 1);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  EXPECT_EQ(step.reason, WaitReason::kLoads);
  sequencer_->Load(*write);
  EXPECT_EQ(Replied(Attempt(*write)), ":1");
  EXPECT_EQ(HotRead(core::ops::ReadOp{core::ops::SetCard{.key = "k"}}), ":2");
  EXPECT_EQ(loads_, 1);
}

TEST_F(SequencerAttemptTest, AForeignLoadWaitTimesOutUnappliedAndLeavesTheirLoad) {
  Build(Options{.command_timeout = 50ms});
  const hot::LoadStart theirs = hot_->BeginLoad("k");
  ASSERT_TRUE(theirs.started());
  auto result = Run({"SADD", "k", "b"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(result.error().message(), "write timed out awaiting a load of its key");
  EXPECT_EQ(hot_->LoadWaitersForTesting("k"), 0U);
  EXPECT_TRUE(hot_->LoadPending("k")) << "their load is theirs to end";
  EXPECT_EQ(PublishedCount(), 0U);
  hot_->AbortLoad("k", theirs.token);
}

// --- Own loads ---

// Loads that land past the deadline still install, but the write that
// took them is never applied.
TEST_F(SequencerAttemptTest, AWriteAbandonedAfterItsLoadsInstallsThemUnapplied) {
  PutCold("k", core::ColdKeyState{.type = core::KeyType::kSet, .value = core::StringSet{"a"}});
  auto write = Write({"SADD", "k", "b"}, 30ms);
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  ASSERT_EQ(step.reason, WaitReason::kLoads);
  sequencer_->Load(*write);
  std::this_thread::sleep_until(write->deadline() + 5ms);
  step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kReplied);
  ASSERT_FALSE(step.reply.has_value());
  EXPECT_EQ(step.reply.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(step.reply.error().message(), "write timed out loading its keys");
  EXPECT_EQ(HotRead(core::ops::ReadOp{core::ops::SetIsMember{.key = "k", .member = "a"}}), ":1")
      << "the load installed";
  EXPECT_EQ(HotRead(core::ops::ReadOp{core::ops::SetIsMember{.key = "k", .member = "b"}}), ":0");
  EXPECT_EQ(PublishedCount(), 0U);
}

// --- Abandonment ---

// A wake that comes after the deadline only cleans up: the write is not
// applied, and the wake it took goes on to the next waiter.
TEST_F(SequencerAttemptTest, AWakePastTheDeadlineAbandonsTheWriteAndHandsOn) {
  queue_.SetAdmissible(false);
  auto late = Write({"SET", "a", "1"}, 20ms, 0);
  auto next = Write({"SET", "b", "2"}, 10s, 1);
  ASSERT_EQ(Attempt(*late).kind, Step::Kind::kWait);
  ASSERT_EQ(Attempt(*next).kind, Step::Kind::kWait);
  std::this_thread::sleep_until(late->deadline() + 5ms);

  queue_.SetAdmissible(true);
  ASSERT_TRUE(sink_.WaitFor(1));
  ASSERT_EQ(sink_.Events().at(0).slot_seq, 0U) << "the front waiter only";
  late->Woken(sink_.Events().at(0).result);
  const Step abandoned = Attempt(*late);
  ASSERT_EQ(abandoned.kind, Step::Kind::kReplied);
  ASSERT_FALSE(abandoned.reply.has_value());
  EXPECT_EQ(abandoned.reply.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(HotString("a"), std::nullopt);

  ASSERT_TRUE(sink_.WaitFor(2)) << "the abandoned write kept its wake";
  EXPECT_EQ(sink_.Events().at(1).slot_seq, 1U);
  next->Woken(sink_.Events().at(1).result);
  EXPECT_EQ(Replied(Attempt(*next)), "OK");
  EXPECT_EQ(HotString("b"), "2");
  EXPECT_EQ(PublishedCount(), 1U);
}

// The notifier has taken the write but not yet posted its wake when
// the deadline expires it: Cancel loses, and the wake that follows
// finds the write abandoned.
TEST_F(SequencerAttemptTest, AWakeRacingTheDeadlineLosesTheCancelAndIsNeverApplied) {
  GatedSink gated;
  queue_.SetAdmissible(false);
  PendingWrite write(core::RespCommand{.args = {"SET", "k", "v"}}, core::PredicateFlags::kNone,
                     core::Completion{.sink = &gated}, core::SteadyClock::now() + 20ms);
  ASSERT_EQ(Attempt(write).kind, Step::Kind::kWait);
  std::thread notifier([this] { queue_.SetAdmissible(true); });
  ASSERT_TRUE(gated.entered.Wait());
  std::this_thread::sleep_until(write.deadline() + 5ms);
  const auto reply = sequencer_->Abandon(write);
  ASSERT_FALSE(reply.has_value());
  EXPECT_EQ(reply.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_FALSE(write.Quiescent()) << "the notifier had it, so its wake is owed";
  gated.release.Open();
  notifier.join();
  write.Woken(gated.Result());
  const Step step = Attempt(write);
  ASSERT_EQ(step.kind, Step::Kind::kReplied);
  EXPECT_TRUE(write.Quiescent());
  EXPECT_EQ(HotString("k"), std::nullopt);
  EXPECT_EQ(PublishedCount(), 0U);
  EXPECT_EQ(queue_.AdmissionWaiters(), 0U);
}

TEST_F(SequencerAttemptTest, AnExpiredWaitIsCancelledAndRepliesWithItsTimeout) {
  queue_.SetAdmissible(false);
  auto write = Write({"SET", "k", "v"}, 10ms);
  ASSERT_EQ(Attempt(*write).kind, Step::Kind::kWait);
  std::this_thread::sleep_until(write->deadline() + 1ms);
  const auto reply = sequencer_->Abandon(*write);
  EXPECT_TRUE(write->Quiescent()) << "cancelled: nothing is owed";
  EXPECT_EQ(queue_.AdmissionWaiters(), 0U);
  ASSERT_FALSE(reply.has_value());
  EXPECT_EQ(reply.error().code(), core::ErrorCode::kResourceExhausted);
  queue_.SetAdmissible(true);
  EXPECT_TRUE(sink_.Events().empty()) << "a cancelled wait is never woken";
}

// --- After linearisation ---

// The durable wait is past the linearisation point: its deadline keeps
// the write applied and says so.
TEST_F(SequencerAttemptTest, ADeadlineInTheDurableWaitKeepsTheWriteApplied) {
  Build(Options{.command_timeout = 50ms});
  queue_.HoldDurable();
  auto result = Run({"SET", "k", "v"});
  queue_.ReleaseDurable();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_TRUE(result.error().message().starts_with(
      "write durable wait exceeded server timeout; the write is applied"))
      << result.error().message();
  EXPECT_EQ(HotString("k"), "v");
  EXPECT_EQ(Logged(ShardOf("k")), (std::vector<std::vector<std::string>>{{"SET", "k", "v"}}));
}

// A write over two shards replies once both streams are durable: each
// posts kDurable, and the reply goes with the last.
TEST_F(SequencerAttemptTest, ASequencedWriteRepliesOnceEveryStreamIsDurable) {
  const std::string a = KeyOn(0);
  const std::string b = KeyOn(1);
  queue_.HoldDurable();
  auto write = Write({"MSET", a, "1", b, "2"});
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kSequenced);
  EXPECT_TRUE(write->Quiescent()) << "the durable wait is the slot's, not the write's";
  EXPECT_EQ(HotString(a), "1") << "applied before it is durable";
  EXPECT_TRUE(sink_.Events().empty());

  queue_.ReleaseDurable();
  ASSERT_TRUE(sink_.WaitFor(2));
  EXPECT_EQ(sink_.Events().at(0).kind, core::CompletionKind::kDurable);
  EXPECT_EQ(sink_.Events().at(1).kind, core::CompletionKind::kDurable);
  EXPECT_FALSE(durable_.Arrived(sink_.Events().at(0).result).has_value()) << "one stream left";
  auto reply = durable_.Arrived(sink_.Events().at(1).result);
  ASSERT_TRUE(reply.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  ASSERT_TRUE(reply->has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(Describe(**reply), "OK");
  EXPECT_TRUE(durable_.Quiescent());
}

TEST_F(SequencerAttemptTest, TheFastPathRepliesInOneAttempt) {
  auto write = Write({"SET", "k", "v"});
  EXPECT_EQ(Replied(Attempt(*write)), "OK");
  EXPECT_TRUE(sink_.Events().empty());
}

#ifndef NDEBUG
TEST_F(SequencerAttemptTest, AnAttemptOffItsOwnersThreadIsFatal) {
  auto write = Write({"SET", "k", "v"});
  std::string reason;
  std::thread other([&] {
    const abyss::testing::ScopedFatalCapture capture;
    try {
      (void)Attempt(*write);
    } catch (const abyss::testing::FatalCalled& fatal) {
      reason = fatal.reason;
    }
  });
  other.join();
  EXPECT_EQ(reason, "a write attempted off its owner's thread");
  EXPECT_EQ(Replied(Attempt(*write)), "OK");
}

TEST_F(SequencerAttemptTest, AWriteDestroyedWhileWaitingIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        queue_.SetAdmissible(false);
        auto write = Write({"SET", "k", "v"});
        (void)Attempt(*write);
        write.reset();
      },
      "a write destroyed with a wait or a load not cleaned up");
}
#endif

// --- Memory backpressure ---

class SequencerAttemptBackpressureTest : public SequencerAttemptTest {
 protected:
  void SetUp() override {
    // 16 KiB a shard, over its limit past 20 KiB; nothing drains, so
    // no fill is evictable.
    Build(Options{.max_memory_bytes = 64 << 10, .command_timeout = 300ms, .drain_gated = true});
    for (int i = 0; i < 3; ++i) {
      ASSERT_EQ(Reply({"SET", KeyOn(0, i, "fill"), std::string(8 << 10, 'f')}), "OK");
    }
  }
};

TEST_F(SequencerAttemptBackpressureTest, AWaitResumesOnADrainAndCompletes) {
  auto write = Write({"SET", KeyOn(0, 9, "new"), "v"});
  Step step = Attempt(*write);
  ASSERT_EQ(step.kind, Step::Kind::kWait);
  EXPECT_EQ(step.reason, WaitReason::kBackpressure);
  EXPECT_EQ(step.shard, 0U);
  EXPECT_EQ(router_.RoomWaiters(), 1U);
  EXPECT_EQ(Redecides(RedecideReason::kBackpressure), 1U);

  router_.Advance(100);
  EXPECT_EQ(Replied(Resume(*write, 1)), "OK");
  EXPECT_EQ(sequencer_->Snapshot().backpressure_waits, 1U);
  EXPECT_EQ(sequencer_->Snapshot().backpressure_rejections, 0U);
}

// One advance wakes one waiter; each that finds room hands on.
TEST_F(SequencerAttemptBackpressureTest, ADrainWakesOneWaiterWhichHandsOn) {
  auto first = Write({"SET", KeyOn(0, 9, "new"), "v"}, 5s, 0);
  auto second = Write({"SET", KeyOn(0, 10, "new"), "w"}, 5s, 1);
  ASSERT_EQ(Attempt(*first).kind, Step::Kind::kWait);
  ASSERT_EQ(Attempt(*second).kind, Step::Kind::kWait);
  ASSERT_EQ(router_.RoomWaiters(), 2U);

  router_.Advance(100);
  ASSERT_TRUE(sink_.WaitFor(1));
  EXPECT_EQ(sink_.Events().size(), 1U) << "one wake per advance";
  first->Woken(sink_.Events().at(0).result);
  EXPECT_EQ(Replied(Attempt(*first)), "OK");
  ASSERT_TRUE(sink_.WaitFor(2)) << "the first did not hand on";
  second->Woken(sink_.Events().at(1).result);
  EXPECT_EQ(Replied(Attempt(*second)), "OK");
}

TEST_F(SequencerAttemptBackpressureTest, ConsumersStoppingFailTheWaitWithUnavailable) {
  auto write = Write({"SET", KeyOn(0, 9, "new"), "v"});
  ASSERT_EQ(Attempt(*write).kind, Step::Kind::kWait);
  router_.Stop();
  const Step step = Resume(*write, 1);
  ASSERT_EQ(step.kind, Step::Kind::kReplied);
  ASSERT_FALSE(step.reply.has_value());
  EXPECT_EQ(step.reply.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_EQ(router_.RoomWaiters(), 0U);
  EXPECT_EQ(Logged(0).size(), 3U);
}

// Once cold has stopped a write over the limit fails at once, rather
// than retrying eviction until its deadline.
TEST_F(SequencerAttemptBackpressureTest, AfterTheConsumersStopAWriteFailsAtOnce) {
  router_.Stop();
  const auto start = std::chrono::steady_clock::now();
  auto result = Run({"SET", KeyOn(0, 9, "new"), "v"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 200ms);
}

TEST_F(SequencerAttemptBackpressureTest, AWaitTimesOutUnappliedWithTheOomText) {
  const std::string key = KeyOn(0, 9, "new");
  EXPECT_EQ(Reply({"SET", key, "v"}),
            "-OOM command not allowed when hot memory is over its limit and cold is behind");
  EXPECT_EQ(sequencer_->Snapshot().backpressure_rejections, 1U);
  EXPECT_EQ(router_.RoomWaiters(), 0U);
  EXPECT_EQ(HotString(key), std::nullopt);
  EXPECT_EQ(Logged(0).size(), 3U);
}

}  // namespace
}  // namespace abyss::engine
