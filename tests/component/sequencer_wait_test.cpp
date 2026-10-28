#include <gtest/gtest.h>

#include "abyss/engine/sequencer.h"

#ifdef ABYSS_HAVE_ROCKSDB

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/durability.h"
#include "abyss/core/durable_wait.h"
#include "abyss/core/types.h"
#include "abyss/engine/pending_write.h"
#include "abyss/metrics/names.h"
#include "recording_sink.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

// Each wait over the real queue, hot store and cold consumers: it
// resumes when its owner makes room, and a timeout leaves nothing a
// restart could bring back.
class SequencerWaitTest : public SequencedEngineTest {
 protected:
  std::unique_ptr<PendingWrite> Pending(std::vector<std::string> args,
                                        std::chrono::milliseconds timeout, uint64_t slot = 0) {
    return std::make_unique<PendingWrite>(core::RespCommand{.args = std::move(args)},
                                          core::PredicateFlags::kNone, sink_.For(slot),
                                          core::SteadyClock::now() + timeout);
  }
  static std::string Replied(const Step& step) {
    if (step.kind != Step::Kind::kReplied) return "not replied";
    if (!step.reply.has_value()) return "error: " + step.reply.error().message();
    return Describe(*step.reply);
  }
  // Slot `slot`'s durable wait, as a connection's slot holds it.
  core::DurableWait& Durable(uint64_t slot) {
    while (durable_.size() <= slot) durable_.push_back(std::make_unique<core::DurableWait>());
    return *durable_[slot];
  }
  // An attempt's step, a linearised write's durable wait handed to its
  // slot: replied if durable already.
  Step Settle(PendingWrite& write, uint64_t slot, Step step) {
    if (step.kind != Step::Kind::kSequenced) return step;
    auto now = sequencer_->Sequence(write, Durable(slot), write.completion());
    if (!now.has_value()) return step;
    return Step{.kind = Step::Kind::kReplied, .reply = *std::move(now)};
  }
  // Attempts each write's next step as its posts come, until every one
  // has replied; their replies by slot.
  std::vector<std::string> Pump(std::vector<std::unique_ptr<PendingWrite>>& writes,
                                std::vector<Step> steps) {
    std::vector<std::string> replies(writes.size());
    size_t open = 0;
    for (size_t i = 0; i < steps.size(); ++i) {
      if (steps[i].kind == Step::Kind::kReplied) {
        replies[i] = Replied(steps[i]);
      } else {
        ++open;
      }
    }
    while (open > 0) {
      if (!sink_.WaitFor(seen_ + 1)) break;
      const core::CompletionEvent event = sink_.Events().at(seen_++);
      if (event.kind == core::CompletionKind::kDurable) {
        auto reply = Durable(event.slot_seq).Arrived(event.result);
        if (reply.has_value()) {
          replies.at(event.slot_seq) =
              Replied(Step{.kind = Step::Kind::kReplied, .reply = *std::move(reply)});
          --open;
        }
        continue;
      }
      PendingWrite& write = *writes.at(event.slot_seq);
      write.Woken(event.result);
      const Step step = Settle(write, event.slot_seq, sequencer_->Attempt(write));
      if (step.kind == Step::Kind::kReplied) {
        replies.at(event.slot_seq) = Replied(step);
        --open;
      }
    }
    return replies;
  }
  // Abandons a wait at its deadline. A wake its notifier had already
  // taken still comes, and only cleans up.
  Step Expired(PendingWrite& write) {
    std::this_thread::sleep_until(write.deadline() + 5ms);
    core::Result<core::RespValue> reply = sequencer_->Abandon(write);
    if (!write.Quiescent() && sink_.WaitFor(seen_ + 1)) {
      write.Woken(sink_.Events().at(seen_++).result);
      (void)sequencer_->Attempt(write);
    }
    EXPECT_TRUE(write.Quiescent());
    return Step{.kind = Step::Kind::kReplied, .reply = std::move(reply)};
  }
  uint64_t Redecides(metrics::RedecideReason reason) const {
    return sequencer_->Snapshot().redecides.at(static_cast<size_t>(reason));
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::RecordingSink sink_;
  size_t seen_ = 0;
  std::vector<std::unique_ptr<core::DurableWait>> durable_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Admission: the durability window, held full by a stalled flush ---

// Writes until one waits for admission: its index and the write.
std::pair<int, std::unique_ptr<PendingWrite>> FillWindow(
    const std::function<std::unique_ptr<PendingWrite>(int)>& make, Sequencer& sequencer) {
  for (int i = 0; i < 1000; ++i) {
    auto write = make(i);
    const Step step = sequencer.Attempt(*write);
    if (step.kind == Step::Kind::kWait) {
      EXPECT_EQ(step.reason, WaitReason::kAdmission);
      return {i, std::move(write)};
    }
    EXPECT_EQ(step.kind, Step::Kind::kSequenced);
  }
  ADD_FAILURE() << "the window never filled";
  return {0, nullptr};
}

TEST_F(SequencerWaitTest, AnAdmissionWaitResumesOnceAFlushMakesRoom) {
  Open(Options{.durability_window_bytes = 4096});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  const std::string value(1000, 'x');
  auto [waited, write] =
      FillWindow([&](int i) { return Pending({"SET", KeyOn(0, i), value}, 10s); }, *sequencer_);
  ASSERT_NE(write, nullptr);

  stall.Release();
  std::vector<std::unique_ptr<PendingWrite>> writes;
  writes.push_back(std::move(write));
  EXPECT_EQ(Pump(writes, {Step{.kind = Step::Kind::kWait}}).at(0), "OK");

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Read({"GET", KeyOn(0, waited)}), value);
}

TEST_F(SequencerWaitTest, AnAdmissionTimeoutLeavesNoTraceAcrossARestart) {
  Open(Options{.durability_window_bytes = 4096});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  const std::string value(1000, 'x');
  auto [waited, write] =
      FillWindow([&](int i) { return Pending({"SET", KeyOn(0, i), value}, 100ms); }, *sequencer_);
  ASSERT_NE(write, nullptr);

  const Step step = Expired(*write);
  ASSERT_EQ(step.kind, Step::Kind::kReplied);
  ASSERT_FALSE(step.reply.has_value());
  EXPECT_EQ(step.reply.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_TRUE(step.reply.error().message().starts_with(
      "WAL durability window full: the device is not keeping up with writes (unflushed "))
      << step.reply.error().message();
  EXPECT_FALSE(Resident(KeyOn(0, waited)));
  stall.Release();

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Read({"GET", KeyOn(0, waited)}), "nil");
  EXPECT_EQ(Read({"GET", KeyOn(0, waited - 1)}), value);
  EXPECT_EQ(Logged(0).size(), static_cast<size_t>(waited));
}

// --- Spare: the preparer paused, the spares used up ---

std::pair<int, std::unique_ptr<PendingWrite>> UseUpSpares(
    const std::function<std::unique_ptr<PendingWrite>(int)>& make, Sequencer& sequencer) {
  for (int i = 0; i < 4000; ++i) {
    auto write = make(i);
    const Step step = sequencer.Attempt(*write);
    if (step.kind == Step::Kind::kWait) {
      EXPECT_EQ(step.reason, WaitReason::kSpare);
      return {i, std::move(write)};
    }
    EXPECT_EQ(step.kind, Step::Kind::kSequenced);
  }
  ADD_FAILURE() << "the spares never ran out";
  return {0, nullptr};
}

TEST_F(SequencerWaitTest, ASpareWaitResumesOnceThePreparerCatchesUp) {
  Open();
  queue_->PauseSegmentPreparerForTesting(0, true);
  const std::string value(2048, 's');
  auto [waited, write] =
      UseUpSpares([&](int i) { return Pending({"SET", KeyOn(1, i), value}, 10s); }, *sequencer_);
  ASSERT_NE(write, nullptr);
  EXPECT_EQ(Redecides(metrics::RedecideReason::kSpare), 1U);

  queue_->PauseSegmentPreparerForTesting(0, false);
  std::vector<std::unique_ptr<PendingWrite>> writes;
  writes.push_back(std::move(write));
  EXPECT_EQ(Pump(writes, {Step{.kind = Step::Kind::kWait}}).at(0), "OK");

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Read({"GET", KeyOn(1, waited)}), value);
}

TEST_F(SequencerWaitTest, ASpareTimeoutLeavesNoTraceAcrossARestart) {
  Open();
  queue_->PauseSegmentPreparerForTesting(0, true);
  const std::string value(2048, 's');
  auto [waited, write] =
      UseUpSpares([&](int i) { return Pending({"SET", KeyOn(1, i), value}, 100ms); }, *sequencer_);
  ASSERT_NE(write, nullptr);

  const Step step = Expired(*write);
  EXPECT_EQ(Replied(step), "error: write timed out waiting for a spare WAL segment");
  queue_->PauseSegmentPreparerForTesting(0, false);

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Read({"GET", KeyOn(1, waited)}), "nil");
  EXPECT_EQ(Read({"GET", KeyOn(1, waited - 1)}), value);
  EXPECT_EQ(Logged(1).size(), static_cast<size_t>(waited));
}

// --- Memory: under power_loss a stalled flush keeps cold from draining
// what hot holds, so hot cannot evict. No consumer thread runs: the
// test drains, and so is the notifier. ---

class SequencerBackpressureWaitTest : public SequencerWaitTest {
 protected:
  void SetUp() override {
    Open(Options{.durability = core::Durability::kPowerLoss, .hot_memory_bytes = 256 << 10});
    queue_->SetFlushHookForTesting(stall_.Hook());
  }
  // Fills shard 0 until a write waits on its memory. Every write stays
  // in `writes_`, its first step in `steps_`.
  int FillUntilAWait(std::chrono::milliseconds timeout) {
    for (int i = 0; i < 64; ++i) {
      writes_.push_back(Pending({"SET", KeyOn(0, i), value_}, timeout, writes_.size()));
      steps_.push_back(
          Settle(*writes_.back(), writes_.size() - 1, sequencer_->Attempt(*writes_.back())));
      if (steps_.back().kind == Step::Kind::kWait) {
        EXPECT_EQ(steps_.back().reason, WaitReason::kBackpressure);
        return i;
      }
      EXPECT_EQ(steps_.back().kind, Step::Kind::kSequenced) << "the flush is stalled";
    }
    ADD_FAILURE() << "hot never went over its limit";
    return -1;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  FlushStall stall_;
  const std::string value_ = std::string(8 << 10, 'm');
  std::vector<std::unique_ptr<PendingWrite>> writes_;
  std::vector<Step> steps_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(SequencerBackpressureWaitTest, AWaitResumesOnceColdDrains) {
  const int waited = FillUntilAWait(10s);
  ASSERT_GE(waited, 0);
  EXPECT_EQ(pool_->ConsumerFor(0).RoomWaitersForTesting(), 1U);

  stall_.Release();
  DrainToCold(0);
  const auto replies = Pump(writes_, steps_);
  for (const std::string& reply : replies) EXPECT_EQ(reply, "OK");
  EXPECT_EQ(sequencer_->Snapshot().backpressure_rejections, 0U);

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Read({"GET", KeyOn(0, waited)}), value_);
}

TEST_F(SequencerBackpressureWaitTest, ATimeoutLeavesNoTraceAcrossARestart) {
  const int waited = FillUntilAWait(300ms);
  ASSERT_GE(waited, 0);
  PendingWrite& write = *writes_.back();
  steps_.back() = Expired(write);
  EXPECT_EQ(Replied(steps_.back()),
            "-OOM command not allowed when hot memory is over its limit and cold is behind");
  EXPECT_EQ(sequencer_->Snapshot().backpressure_rejections, 1U);
  EXPECT_EQ(pool_->ConsumerFor(0).RoomWaitersForTesting(), 0U);

  stall_.Release();
  DrainToCold(0);
  const auto replies = Pump(writes_, steps_);
  for (size_t i = 0; i + 1 < replies.size(); ++i) EXPECT_EQ(replies[i], "OK") << i;

  ASSERT_TRUE(Restart().has_value());
  EXPECT_EQ(Read({"GET", KeyOn(0, waited)}), "nil");
  EXPECT_EQ(Logged(0).size(), static_cast<size_t>(waited));
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
