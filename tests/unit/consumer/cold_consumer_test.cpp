#include "abyss/consumer/cold_consumer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "buffer_read.h"
#include "fatal_capture.h"
#include "mock_cold_store.h"
#include "mock_queue.h"
#include "test_clock.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::NiceMock;
using ::testing::Return;

constexpr core::ShardId kShard = 0;
constexpr core::SequenceId kFirst = core::kFirstSeq;

core::QueueEntry MakeWriteEntry(core::SequenceId seq, std::initializer_list<std::string> cmd_args) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Write{.cmd = core::RespCommand{.args = std::vector<std::string>(cmd_args)}},
  };
}

// A power-durable end the test moves. AwaitDurable records each seq and
// answers at once; with `catch_up` set, the wait itself advances the end.
class PowerDurableLog {
 public:
  explicit PowerDurableLog(core::SequenceId end, bool catch_up = false)
      : end_(end), catch_up_(catch_up) {}

  void Install(testing::MockQueue& queue) {
    ON_CALL(queue, DurableEnd(_, core::Durability::kPowerLoss))
        .WillByDefault([this](core::ShardId, core::Durability) {
          return core::Result<core::SequenceId>(end_.load());
        });
    ON_CALL(queue, AwaitDurable(_, _, core::Durability::kPowerLoss, _))
        .WillByDefault(
            [this](core::ShardId, core::SequenceId seq, core::Durability, core::Duration) {
              {
                const std::scoped_lock lock(mu_);
                awaited_.push_back(seq);
              }
              if (catch_up_) SetEnd(std::max(end_.load(), seq + 1));
              return core::Result<bool>(seq < end_.load());
            });
  }

  void SetEnd(core::SequenceId end) { end_.store(end); }

  std::vector<core::SequenceId> Awaited() const {
    const std::scoped_lock lock(mu_);
    return awaited_;
  }

 private:
  std::atomic<core::SequenceId> end_;
  const bool catch_up_;
  mutable std::mutex mu_;
  std::vector<core::SequenceId> awaited_;
};

class ColdConsumerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ON_CALL(queue_, Read(_, _, _, _, _)).WillByDefault(Return(std::vector<core::QueueEntry>{}));
    ON_CALL(queue_, CommitOffset(_, _, _)).WillByDefault(Return(core::Result<void>{}));
    ON_CALL(cold_, ApplyBatch(_, _)).WillByDefault(Return(core::Result<void>{}));
  }

  std::unique_ptr<ColdConsumer> MakeConsumer(ColdConsumer::Config cfg = {}) {
    cfg.rng_seed = 42;
    return std::make_unique<ColdConsumer>(queue_, cold_, kShard, cfg, policy_, clock_.SteadyFn());
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  NiceMock<testing::MockQueue> queue_;
  NiceMock<testing::MockColdStore> cold_;
  testing::TestClock clock_;
  // Outlives every consumer the test fixture builds; consumer holds a const ref.
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Drain path ---------------------------------------------------------------

TEST_F(ColdConsumerTest, DrainAbsorbsWriteEntriesIntoBuffer) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "ka", "va"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "kb", "vb"}));

  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 2);
  auto read = testing::BufferRead(c->Buffer(), "ka");
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "va");
}

// --- XERR-5: cold parse-poison quarantine ------------------------------------

TEST_F(ColdConsumerTest, ParsePoisonDoesNotAdvanceCommitPastUnabsorbedSeq) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  // seq 1 is genuine skew poison: HSET HAS a parser and that parser rejects an
  // odd field/value list, so another tier accepted bytes cold cannot decode.
  // seq 2 is a valid SET that still absorbs. The commit must NOT pass seq 1.
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"HSET", "h", "f"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // The valid SET (seq 2) must still reach cold — the poison quarantines the
  // commit frontier, it does not drop the surrounding writes.
  bool saw_set = false;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([&saw_set](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        for (const auto& op : ops) {
          if (const auto* s = std::get_if<core::ops::StringSet>(&op);
              s != nullptr && s->key == "k") {
            saw_set = true;
          }
        }
        return core::Result<void>{};
      });

  core::SequenceId commit_seq = 0;
  bool committed = false;
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, _))
      .WillRepeatedly(
          [&commit_seq, &committed](core::ConsumerId, core::ShardId, core::SequenceId s) {
            commit_seq = s;
            committed = true;
            return core::Result<void>{};
          });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  // The valid SET absorbed and reached cold (the write is not lost).
  EXPECT_TRUE(saw_set) << "the valid write surrounding the poison was dropped";
  EXPECT_EQ(c->Snapshot().parse_poison, 1U);
  // The drained frontier is pinned below the poison (seq 1 -> floor 0).
  EXPECT_EQ(c->Snapshot().latest_drained_seq, 0U);
  // Either no commit was issued, or it stayed at the floor (never >= poison seq 1).
  if (committed) {
    EXPECT_EQ(commit_seq, 0U) << "commit advanced past the poison entry";
  }
}

TEST_F(ColdConsumerTest, ParsePoisonPinsCommitWithoutRedelivery) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  cfg.loop_initial_backoff = 5ms;
  cfg.loop_max_backoff = 20ms;
  cfg.queue_read_timeout = 1ms;
  auto c = MakeConsumer(cfg);

  // A poison Write at seq 5, then a valid write. The cursor moves past the
  // poison, so it is delivered once; the commit stays pinned below it and
  // the idle loop backs off rather than busy-spinning.
  const std::vector<core::QueueEntry> log{MakeWriteEntry(5, {"HSET", "h", "f"}),
                                          MakeWriteEntry(6, {"SET", "k", "v"})};
  std::atomic<int> read_calls{0};
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillRepeatedly([&log, &read_calls](core::ShardId, core::SequenceId from, size_t max,
                                          core::Duration, core::Durability) {
        read_calls.fetch_add(1, std::memory_order_relaxed);
        return testing::ReadFromLog(log, from, max);
      });

  std::atomic<core::SequenceId> max_commit{0};
  EXPECT_CALL(queue_, CommitOffset(_, _, _))
      .WillRepeatedly([&max_commit](core::ConsumerId, core::ShardId, core::SequenceId s) {
        max_commit.store(std::max(max_commit.load(), s));
        return core::Result<void>{};
      });

  c->Start();
  std::this_thread::sleep_for(150ms);
  c->Stop();

  EXPECT_EQ(c->Snapshot().parse_poison, 1U) << "the poison entry was re-delivered";
  EXPECT_EQ(c->Snapshot().latest_drained_seq, 4U);
  EXPECT_LT(max_commit.load(), 5U) << "commit advanced to or past the poison seq";
  // Capped 5ms->20ms backoff bounds reads well under a busy spin (10k+).
  EXPECT_LE(read_calls.load(), 80) << "idle loop busy-spun instead of backing off";
}

// --- Read cursor -------------------------------------------------------------

// A2: a buffered key pins the commit (its quiet window has not elapsed),
// yet the drain must keep reading past it. When the read position was
// derived from the commit, every Read returned the same drained window and
// the drained frontier froze at the first queue_read_max_count entries.
TEST_F(ColdConsumerTest, DrainAdvancesPastCommitPinnedByBufferedKey) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.queue_read_max_count = 8;
  auto c = MakeConsumer(cfg);

  constexpr core::SequenceId kTail = (2 * 8) + 5;
  std::vector<core::QueueEntry> log;
  for (core::SequenceId seq = kFirst; seq <= kTail; ++seq) {
    log.push_back(MakeWriteEntry(seq, {"SET", "k" + std::to_string(seq), "v"}));
  }
  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) { return testing::ReadFromLog(log, from, max); });
  EXPECT_CALL(queue_, CommitOffset(_, _, _)).Times(0);

  for (int i = 0; i < 2 * static_cast<int>(kTail) && c->LatestDrainedSeq() < kTail; ++i) {
    c->Drain();
    c->Flush();
  }

  EXPECT_EQ(c->LatestDrainedSeq(), kTail);
  EXPECT_EQ(c->Buffer().Size(), static_cast<size_t>(kTail - kFirst) + 1);
}

TEST_F(ColdConsumerTest, CursorResumesAfterCommittedOffset) {
  auto c = MakeConsumer();
  EXPECT_CALL(queue_, CommittedOffset(core::kColdConsumer, kShard))
      .WillRepeatedly(Return(core::Result<std::optional<core::SequenceId>>(41)));
  EXPECT_CALL(queue_, Read(kShard, 42, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWriteEntry(42, {"SET", "k", "v"})}));
  EXPECT_CALL(queue_, Read(kShard, 43, _, _, _))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Drain();

  EXPECT_EQ(c->LatestDrainedSeq(), 42U);
  EXPECT_EQ(c->Snapshot().last_commit_seq, 41U);
}

// A retention consumer reading below FirstSeq means the reaper deleted
// entries above its persisted offset. That is data loss: fail-stop, naming
// everything an operator needs, rather than skip ahead.
TEST_F(ColdConsumerTest, ReadBelowFirstRetainedSeqIsFatal) {
  const testing::ScopedFatalCapture capture;
  auto c = MakeConsumer();
  EXPECT_CALL(queue_, CommittedOffset(core::kColdConsumer, kShard))
      .WillRepeatedly(Return(core::Result<std::optional<core::SequenceId>>(4)));
  EXPECT_CALL(queue_, Read(kShard, 5, _, _, _))
      .WillRepeatedly(Return(core::Result<std::vector<core::QueueEntry>>(
          std::unexpected(core::Error{core::ErrorCode::kOutOfRange, "below first retained seq"}))));
  EXPECT_CALL(queue_, FirstSeq(kShard)).WillRepeatedly(Return(core::Result<core::SequenceId>(100)));

  try {
    c->Drain();
    FAIL() << "a read below the first retained seq must be fatal";
  } catch (const testing::FatalCalled& fatal) {
    EXPECT_NE(fatal.reason.find("cold"), std::string::npos) << fatal.reason;
    EXPECT_NE(fatal.reason.find("shard 0"), std::string::npos) << fatal.reason;
    EXPECT_NE(fatal.reason.find("read seq 5"), std::string::npos) << fatal.reason;
    EXPECT_NE(fatal.reason.find("first retained seq 100"), std::string::npos) << fatal.reason;
  }
}

// --- ENGINE-9: the idle backoff must not outlast a drain waiter's deadline ---

TEST_F(ColdConsumerTest, IdleBackoffIsInterruptedByAWaiterOnADrain) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  cfg.queue_read_timeout = 1ms;
  cfg.loop_initial_backoff = 1ms;
  // Far longer than the waiter's deadline below: if the loop sleeps
  // this out, a write held by backpressure fails on a consumer that is
  // merely idle.
  cfg.loop_max_backoff = 5000ms;
  auto c = MakeConsumer(cfg);

  // Idle until the entry appears, so the loop climbs to its backoff ceiling.
  std::atomic<bool> release{false};
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillRepeatedly([&release](core::ShardId, core::SequenceId from, size_t, core::Duration,
                                 core::Durability) {
        std::vector<core::QueueEntry> out;
        if (release.load(std::memory_order_acquire) && from <= 9) {
          out.push_back(MakeWriteEntry(9, {"SET", "k", "v"}));
        }
        return out;
      });

  c->Start();
  // Let the backoff saturate before anything is available to drain.
  std::this_thread::sleep_for(120ms);
  release.store(true, std::memory_order_release);

  // The waiter's deadline is far shorter than the backoff ceiling. This
  // must still succeed: waiting is what tells the consumer to stop
  // sleeping.
  const bool drained = c->WaitForDrainedSeq(9, 500ms);
  c->Stop();

  EXPECT_TRUE(drained) << "the wait expired while the consumer slept out its idle backoff";
}

// --- XERR-6: a missing parser is a capability gap, not poison -----------------

TEST_F(ColdConsumerTest, WriteWithNoParserIsSkippedNotPoisoned) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  // A WAL entry naming a command this build has no parser for. The frontend can
  // no longer produce one -- the registry only advertises unconditional writes
  // that a parser backs -- but replaying a log written by a build with a wider
  // command surface still can, which is exactly the skew this gate exists for.
  // Quarantining instead would pin the shard's WAL retention forever.
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"INCR", "counter"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId commit_seq = 0;
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&commit_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        commit_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.parse_poison, 0U) << "an unimplemented command was quarantined as poison";
  EXPECT_EQ(snap.unsupported_ops, 1U);
  // The frontier moved past both entries: nothing is pinned.
  EXPECT_EQ(snap.latest_drained_seq, 2U);
  EXPECT_EQ(commit_seq, 2U) << "WAL retention stayed pinned behind an unimplementable write";
}

// Multi-key DEL/MSET WAL entries no longer occur — the engine decomposes
// before queueing. Per-key absorption is covered by the SET path tests.

// --- Flush path ---------------------------------------------------------------

TEST_F(ColdConsumerTest, QuietWindowElapsedTriggersApplyBatch) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  std::vector<std::pair<std::string, std::string>> observed_sets;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillOnce([&observed_sets](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        for (const auto& op : ops) {
          if (const auto* s = std::get_if<core::ops::StringSet>(&op)) {
            observed_sets.emplace_back(std::string(s->key), std::string(s->value));
          }
        }
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  ASSERT_EQ(observed_sets.size(), 1);
  EXPECT_EQ(observed_sets[0].first, "k");
  EXPECT_EQ(observed_sets[0].second, "v");
}

TEST_F(ColdConsumerTest, TombstoneFlushesAsDelToCold) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));
  entries.push_back(MakeWriteEntry(2, {"DEL", "k"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  std::vector<std::string> observed_dels;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillOnce([&observed_dels](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        for (const auto& op : ops) {
          if (const auto* d = std::get_if<core::ops::Del>(&op)) {
            for (auto k : d->keys) observed_dels.emplace_back(k);
          }
        }
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  ASSERT_EQ(observed_dels.size(), 1);
  EXPECT_EQ(observed_dels[0], "k");
}

TEST_F(ColdConsumerTest, FlushRetriesOnTransientApplyFailure) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.retry_initial_backoff = 0ms;  // Avoid blocking the test on sleep.
  cfg.retry_max_backoff = 0ms;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillOnce(Return(std::unexpected(core::Error{core::ErrorCode::kUnavailable, "transient"})))
      .WillOnce(Return(std::unexpected(core::Error{core::ErrorCode::kUnavailable, "transient"})))
      .WillOnce(Return(core::Result<void>{}));

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  // A transient apply failure now reinserts the batch and reports kBackpressure;
  // the loop (here driven manually) re-attempts on each pass. After two transient
  // failures the third pass succeeds.
  c->Flush();  // 1st ApplyBatch: transient, reinserted.
  c->Flush();  // 2nd ApplyBatch: transient, reinserted.
  c->Flush();  // 3rd ApplyBatch: success.

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.apply_failures, 2U);
  EXPECT_GE(snap.retry_attempts, 2U);
  EXPECT_EQ(snap.ops_flushed, 1U);
}

uint64_t WallMs(const testing::TestClock& clock) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
          .count());
}

// A write stamped at or past its own TTL is expired by the log clock, so
// it flushes as a delete: cold may hold an older value that would
// otherwise resurface.
TEST_F(ColdConsumerTest, EntryExpiredByTheLogClockFlushesAsDelete) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  const uint64_t appended_at_ms = WallMs(clock_);
  c->Buffer().Absorb("k",
                     core::ops::WriteOp{core::ops::StringSet{
                         .key = "k", .value = "v", .abs_ttl_ms = appended_at_ms - 1}},
                     core::EvictionTTL{3600}, /*seq=*/1, appended_at_ms);

  std::vector<std::string> observed_dels;
  size_t observed_ops = 0;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillOnce([&](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        observed_ops = ops.size();
        for (const auto& op : ops) {
          if (const auto* d = std::get_if<core::ops::Del>(&op)) {
            for (auto k : d->keys) observed_dels.emplace_back(k);
          }
        }
        return core::Result<void>{};
      });

  clock_.Advance(60s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(observed_ops, 1U);
  EXPECT_EQ(observed_dels, std::vector<std::string>{"k"});
  EXPECT_EQ(c->Snapshot().entries_expired_abs_ttl, 1U);
  EXPECT_EQ(c->Buffer().Size(), 0U);
}

// The wall clock running past a TTL deletes nothing: until the log's
// clock passes it, a later write may still have relied on the key.
TEST_F(ColdConsumerTest, AnEntryExpiredOnlyByTheWallClockFlushesAsWritten) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  const uint64_t appended_at_ms = WallMs(clock_);
  const uint64_t ttl_ms = appended_at_ms + 1000;
  c->Buffer().Absorb(
      "k", core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = ttl_ms}},
      core::EvictionTTL{3600}, /*seq=*/1, appended_at_ms);

  std::vector<core::ops::WriteOp> observed;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillOnce([&](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        observed.assign(ops.begin(), ops.end());
        return core::Result<void>{};
      });

  clock_.Advance(60s);
  c->Drain();
  c->Flush();

  ASSERT_EQ(observed.size(), 1U);
  const auto* set = std::get_if<core::ops::StringSet>(observed.data());
  ASSERT_NE(set, nullptr);
  EXPECT_EQ(set->abs_ttl_ms, ttl_ms);
  EXPECT_EQ(c->Snapshot().entries_expired_abs_ttl, 0U);
  EXPECT_EQ(c->LogClockMs(), appended_at_ms);
}

// --- Mode hysteresis ---------------------------------------------------------

TEST_F(ColdConsumerTest, HighWaterTriggersAggressiveMode) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.buffer_high_water_bytes = 512;  // Small for test.
  cfg.buffer_low_water_bytes = 256;
  auto c = MakeConsumer(cfg);

  // Craft an entry large enough to push bytes past high_water.
  const std::string big_value(1024, 'x');
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k1", big_value}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Expect at least one ApplyBatch when aggressive mode triggers flush.
  EXPECT_CALL(cold_, ApplyBatch(_, _)).Times(AtLeast(1));

  c->Drain();
  c->Flush();
  EXPECT_EQ(c->CurrentMode(), ColdConsumer::Mode::kAggressive);

  // After aggressive flush, buffer should drain below low-water and return to normal.
  c->Drain();
  c->Flush();
  EXPECT_EQ(c->CurrentMode(), ColdConsumer::Mode::kNormal);
  EXPECT_EQ(c->Buffer().Size(), 0);
}

core::QueueEntry MakeFlushEntry(core::SequenceId seq) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Flush{},
  };
}

// The Wipe is persisted state, so it waits for the Flush entry to be
// power-durable. A timeout holds the Flush, uncounted as a failure, and
// the next drain retries it.
TEST_F(ColdConsumerTest, FlushWipeWaitsForPowerDurability) {
  auto c = MakeConsumer();

  constexpr core::SequenceId kFlushSeq = 7;
  PowerDurableLog wal(kFlushSeq);
  wal.Install(queue_);
  core::SequenceId max_commit = 0;
  EXPECT_CALL(queue_, CommitOffset(_, _, _))
      .WillRepeatedly([&max_commit](core::ConsumerId, core::ShardId, core::SequenceId s) {
        max_commit = std::max(max_commit, s);
        return core::Result<void>{};
      });
  int wipes = 0;
  EXPECT_CALL(cold_, Wipe(kShard)).WillRepeatedly([&wipes](core::ShardId) {
    ++wipes;
    return core::Result<void>{};
  });
  const std::vector<core::QueueEntry> log{MakeFlushEntry(kFlushSeq)};
  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) { return testing::ReadFromLog(log, from, max); });

  EXPECT_EQ(c->Drain(), 0U);
  EXPECT_EQ(wipes, 0) << "wiped before the Flush was power-durable";
  EXPECT_EQ(wal.Awaited(), std::vector<core::SequenceId>{kFlushSeq});
  EXPECT_LT(c->LatestDrainedSeq(), kFlushSeq);
  EXPECT_EQ(c->Snapshot().durability_waits_timed_out, 1U);
  EXPECT_EQ(c->Snapshot().apply_failures, 0U);

  wal.SetEnd(kFlushSeq + 1);
  EXPECT_EQ(c->Drain(), 1U);
  EXPECT_EQ(wipes, 1);
  EXPECT_EQ(c->LatestDrainedSeq(), kFlushSeq);
  EXPECT_EQ(max_commit, kFlushSeq);
  EXPECT_EQ(c->Snapshot().durability_waits_timed_out, 1U);
}

// The Flush still advances the committed offset when the WAL allows it.
TEST_F(ColdConsumerTest, FlushCommitsOffsetWhenDurable) {
  auto c = MakeConsumer();

  constexpr core::SequenceId kFlushSeq = 7;
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, kFlushSeq))
      .WillOnce(Return(core::Result<void>{}));
  EXPECT_CALL(cold_, Wipe(kShard)).WillOnce(Return(core::Result<void>{}));
  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeFlushEntry(kFlushSeq)}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();

  EXPECT_EQ(c->LatestDrainedSeq(), kFlushSeq);
  EXPECT_EQ(c->Snapshot().last_commit_seq, kFlushSeq);
}

TEST_F(ColdConsumerTest, LogClockFollowsTheOldestUnflushedWriteThenTheFlush) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  const auto base = clock_.WallNow();
  const uint64_t base_ms = WallMs(clock_);
  auto first = MakeWriteEntry(1, {"SET", "a", "v"});
  first.appended_at = base;
  auto second = MakeWriteEntry(2, {"SET", "b", "v"});
  second.appended_at = base + 5s;
  auto flush = MakeFlushEntry(3);
  flush.appended_at = base + 9s;
  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{first, second}))
      .WillOnce(Return(std::vector<core::QueueEntry>{flush}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));
  EXPECT_CALL(cold_, Wipe(kShard)).WillOnce(Return(core::Result<void>{}));

  EXPECT_EQ(c->LogClockMs(), 0U);
  c->Drain();
  EXPECT_EQ(c->LogClockMs(), base_ms);
  clock_.Advance(60s);
  c->Flush();
  EXPECT_EQ(c->LogClockMs(), base_ms + 5000);

  c->Drain();
  EXPECT_EQ(c->LogClockMs(), base_ms + 9000);
}

// A failed Wipe never lets the consumer pass the Flush: the cursor and
// commit stay below it, no reply is sent, and the next drain retries it.
TEST_F(ColdConsumerTest, FailedWipeHoldsTheFlushUntilARetrySucceeds) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  constexpr core::SequenceId kFlushSeq = 2;
  const std::vector<core::QueueEntry> log{
      MakeWriteEntry(1, {"SET", "a", "v"}),
      MakeFlushEntry(kFlushSeq),
      MakeWriteEntry(3, {"SET", "b", "v"}),
  };
  std::vector<core::SequenceId> read_from;
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillRepeatedly([&log, &read_from](core::ShardId, core::SequenceId from, size_t max,
                                         core::Duration, core::Durability) {
        read_from.push_back(from);
        return testing::ReadFromLog(log, from, max);
      });
  EXPECT_CALL(cold_, Wipe(kShard))
      .WillOnce(Return(core::Result<void>(
          std::unexpected(core::Error{core::ErrorCode::kInternal, "wipe failed (test)"}))))
      .WillOnce(Return(core::Result<void>{}));
  core::SequenceId max_commit = 0;
  EXPECT_CALL(queue_, CommitOffset(_, _, _))
      .WillRepeatedly([&max_commit](core::ConsumerId, core::ShardId, core::SequenceId s) {
        max_commit = std::max(max_commit, s);
        return core::Result<void>{};
      });

  EXPECT_EQ(c->Drain(), 1U) << "only the entry before the Flush is consumed";
  // Hot is already wiped, so the buffer must still serve the newest
  // pre-Flush value rather than let reads fall back to older cold data.
  auto overlay = testing::BufferRead(c->Buffer(), "a");
  ASSERT_TRUE(overlay.has_value()) << "the buffer was dropped before the wipe succeeded";
  EXPECT_EQ(overlay->AsString(), "v");
  c->Flush();
  EXPECT_LT(c->LatestDrainedSeq(), kFlushSeq);
  EXPECT_LT(max_commit, kFlushSeq) << "committed past a Flush whose wipe failed";

  c->Drain();
  ASSERT_GE(read_from.size(), 2U);
  EXPECT_EQ(read_from[1], kFlushSeq) << "the retry did not re-read the Flush";
  EXPECT_EQ(c->LatestDrainedSeq(), 3U);
}

// The Wipe discards what a pre-Flush poison pinned, so the Flush releases
// that pin; a poison after the Flush still holds the commit below it.
TEST_F(ColdConsumerTest, FlushReleasesEarlierPoisonPinOnly) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  constexpr core::SequenceId kFlushSeq = 3;
  constexpr core::SequenceId kLatePoisonSeq = 5;
  const std::vector<core::QueueEntry> log{
      MakeWriteEntry(1, {"HSET", "h", "f"}),
      MakeWriteEntry(2, {"SET", "a", "v"}),
      MakeFlushEntry(kFlushSeq),
      MakeWriteEntry(4, {"SET", "b", "v"}),
      MakeWriteEntry(kLatePoisonSeq, {"HSET", "h", "f"}),
      MakeWriteEntry(6, {"SET", "c", "v"}),
  };
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) { return testing::ReadFromLog(log, from, max); });
  EXPECT_CALL(cold_, Wipe(kShard)).WillOnce(Return(core::Result<void>{}));
  core::SequenceId max_commit = 0;
  EXPECT_CALL(queue_, CommitOffset(_, _, _))
      .WillRepeatedly([&max_commit](core::ConsumerId, core::ShardId, core::SequenceId s) {
        max_commit = std::max(max_commit, s);
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Snapshot().parse_poison, 2U);
  EXPECT_GE(max_commit, kFlushSeq) << "the pre-Flush poison still pins the commit";
  EXPECT_LT(max_commit, kLatePoisonSeq) << "committed past the post-Flush poison";
}

// --- Low-water commit ---------------------------------------------------------

TEST_F(ColdConsumerTest, CommitAdvancesToDrainedSeqWhenBufferEmpty) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId commit_seq = 0;
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&commit_seq](core::ConsumerId /*consumer*/, core::ShardId /*shard*/,
                                    core::SequenceId s) {
        commit_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();  // Drain entry 1.
  clock_.Advance(31s);
  c->Drain();
  c->Flush();  // Flush, commit.

  EXPECT_EQ(commit_seq, 1U);
}

TEST_F(ColdConsumerTest, CommitBoundedByOldestPendingSeq) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  // Two different keys; the second one is drained later so its quiet window
  // hasn't elapsed when we try to flush the first.
  std::vector<core::QueueEntry> first;
  first.push_back(MakeWriteEntry(1, {"SET", "first", "v1"}));
  std::vector<core::QueueEntry> second;
  second.push_back(MakeWriteEntry(2, {"SET", "second", "v2"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(first))
      .WillOnce(Return(second))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId commit_seq = 0;
  EXPECT_CALL(queue_, CommitOffset(_, _, _))
      .WillRepeatedly([&commit_seq](core::ConsumerId /*consumer*/, core::ShardId /*shard*/,
                                    core::SequenceId s) {
        commit_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();  // Drain seq 1.
  clock_.Advance(25s);
  c->Drain();
  c->Flush();           // Drain seq 2 (quiet window for seq 1 not elapsed).
  clock_.Advance(10s);  // Now seq 1's quiet window is past (35s > 30s), seq 2's isn't.
  c->Drain();
  c->Flush();  // Flush seq 1; seq 2 still pending.

  // Oldest pending is 2 after flush, so we commit up to 1 (min of drained=2, oldest-1=1).
  EXPECT_EQ(commit_seq, 1U);
}

// --- Cold-durability commit gate (XDUR-1) ------------------------------------

TEST_F(ColdConsumerTest, CommitGatedOnCheckpointBeforeAdvancing) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.checkpoint_max_flushes = 1;  // Checkpoint every flush so the commit can advance.
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Checkpoint must be issued before the commit advances, and for a seq <= the
  // applied frontier.
  core::SequenceId checkpointed_seq = 0;
  EXPECT_CALL(cold_, Checkpoint(kShard, _))
      .WillRepeatedly([&checkpointed_seq](core::ShardId, core::SequenceId s) {
        checkpointed_seq = s;
        return core::Result<void>{};
      });
  core::SequenceId commit_seq = 0;
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&commit_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        commit_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(checkpointed_seq, 1U);
  EXPECT_EQ(commit_seq, 1U);
  EXPECT_LE(commit_seq, checkpointed_seq) << "commit advanced past the cold checkpoint frontier";
}

TEST_F(ColdConsumerTest, CommitBlockedWhenCheckpointFails) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.checkpoint_max_flushes = 1;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Checkpoint fails — the cold flush is applied (memtable) but not durable, so
  // the commit must NOT advance and the WAL stays pinned.
  EXPECT_CALL(cold_, Checkpoint(kShard, _))
      .WillRepeatedly(Return(std::unexpected(core::Error{core::ErrorCode::kUnavailable, "fsync"})));
  EXPECT_CALL(queue_, CommitOffset(_, _, _)).Times(0);

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Snapshot().last_commit_seq, 0U) << "commit advanced despite a failed checkpoint";
}

TEST_F(ColdConsumerTest, CommitClampedToPowerDurableEnd) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.checkpoint_max_flushes = 1;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "a", "v"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "b", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Only seq 1 is durable in the WAL; the cold commit must clamp to it even though
  // both seqs were drained, checkpointed, and otherwise committable.
  EXPECT_CALL(queue_, DurableEnd(kShard, core::Durability::kPowerLoss))
      .WillRepeatedly(Return(core::Result<core::SequenceId>(2)));
  core::SequenceId commit_seq = 0;
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&commit_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        commit_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(commit_seq, 1U) << "commit advanced past the durable WAL tail";
}

// --- Persistence gate ---------------------------------------------------------

TEST_F(ColdConsumerTest, BatchWaitsUntilItsLastSeqIsPowerDurable) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWriteEntry(1, {"SET", "a", "v"}),
                                                     MakeWriteEntry(2, {"SET", "b", "v"})}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));
  PowerDurableLog wal(2);
  wal.Install(queue_);
  int applies = 0;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([&applies](std::span<const core::ops::WriteOp>, core::SequenceId) {
        ++applies;
        return core::Result<void>{};
      });
  std::vector<core::SequenceId> commits;
  EXPECT_CALL(queue_, CommitOffset(_, _, _))
      .WillRepeatedly([&commits](core::ConsumerId, core::ShardId, core::SequenceId s) {
        commits.push_back(s);
        return core::Result<void>{};
      });

  c->Drain();
  EXPECT_EQ(c->Flush(), ColdConsumer::FlushOutcome::kDurabilityPending);
  EXPECT_EQ(wal.Awaited(), std::vector<core::SequenceId>{2});
  EXPECT_EQ(applies, 0) << "applied a batch above the power-durable end";
  EXPECT_EQ(c->Buffer().Size(), 2U);
  EXPECT_EQ(c->Buffer().HeapDepth(), 2U) << "the held batch was not rescheduled";
  EXPECT_TRUE(testing::BufferRead(c->Buffer(), "a").has_value());

  // Rescheduled, so the next pass selects and waits on it again.
  EXPECT_EQ(c->Flush(), ColdConsumer::FlushOutcome::kDurabilityPending);
  EXPECT_EQ(wal.Awaited().size(), 2U);
  EXPECT_TRUE(commits.empty()) << "committed past a held batch";

  wal.SetEnd(3);
  EXPECT_EQ(c->Flush(), ColdConsumer::FlushOutcome::kProgress);
  EXPECT_EQ(applies, 1);
  EXPECT_EQ(c->Buffer().Size(), 0U);
  EXPECT_EQ(commits, std::vector<core::SequenceId>{2});
  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.durability_waits_timed_out, 2U);
  EXPECT_EQ(snap.apply_failures, 0U);
  EXPECT_EQ(snap.retry_attempts, 0U);
}

// Absorption pauses while the gate waits, so the target is the selected
// batch's last_seq, not the log end: a key rewritten faster than one flush
// still flushes on every pass.
TEST_F(ColdConsumerTest, HotKeyFlushesOncePowerDurablePastItsSelectedWrites) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> log;
  const auto rewrite = [&log] {
    for (int i = 0; i < 5; ++i) {
      const auto seq = static_cast<core::SequenceId>(log.size()) + 1;
      log.push_back(MakeWriteEntry(seq, {"SET", "hot", "v" + std::to_string(seq)}));
    }
  };
  rewrite();
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) { return testing::ReadFromLog(log, from, max); });
  PowerDurableLog wal(1, /*catch_up=*/true);
  wal.Install(queue_);
  std::vector<std::string> applied;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly(
          [&applied, &rewrite](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
            for (const auto& op : ops) {
              if (const auto* set = std::get_if<core::ops::StringSet>(&op)) {
                applied.emplace_back(set->value);
              }
            }
            rewrite();  // the writer outruns every flush
            return core::Result<void>{};
          });

  for (int pass = 0; pass < 3; ++pass) {
    c->Drain();
    EXPECT_EQ(c->Flush(), ColdConsumer::FlushOutcome::kProgress) << "pass " << pass;
  }
  EXPECT_EQ(applied, (std::vector<std::string>{"v5", "v10", "v15"}));
  EXPECT_EQ(wal.Awaited(), (std::vector<core::SequenceId>{5, 10, 15}));
  EXPECT_EQ(c->Snapshot().durability_waits_timed_out, 0U);
}

// The wait paces a held batch, so the loop retries on the next pass rather
// than sleeping on the no-progress backoff ladder.
TEST_F(ColdConsumerTest, DurabilityPendingRetriesWithoutTheBackoffLadder) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  cfg.queue_read_timeout = 5ms;
  cfg.loop_initial_backoff = 50ms;
  cfg.loop_max_backoff = 1000ms;
  auto c = MakeConsumer(cfg);

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWriteEntry(1, {"SET", "k", "v"})}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));
  std::atomic<core::SequenceId> end{1};
  std::atomic<int> waits{0};
  EXPECT_CALL(queue_, DurableEnd(_, core::Durability::kPowerLoss))
      .WillRepeatedly([&end](core::ShardId, core::Durability) {
        return core::Result<core::SequenceId>(end.load());
      });
  EXPECT_CALL(queue_, AwaitDurable(_, _, core::Durability::kPowerLoss, _))
      .WillRepeatedly([&end, &waits](core::ShardId, core::SequenceId seq, core::Durability,
                                     core::Duration timeout) {
        std::this_thread::sleep_for(timeout);
        waits.fetch_add(1);
        return core::Result<bool>(seq < end.load());
      });
  std::atomic<int> applies{0};
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([&applies](std::span<const core::ops::WriteOp>, core::SequenceId) {
        applies.fetch_add(1);
        return core::Result<void>{};
      });

  const auto start = std::chrono::steady_clock::now();
  c->Start();
  // Ten 5 ms waits take ~50 ms; the 50 ms-doubling ladder would take seconds.
  while (waits.load() < 10 && std::chrono::steady_clock::now() - start < 1500ms) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_GE(waits.load(), 10);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1000ms) << "the held batch backed off";
  EXPECT_EQ(applies.load(), 0);

  end.store(2);
  while (applies.load() == 0 && std::chrono::steady_clock::now() - start < 5s) {
    std::this_thread::sleep_for(1ms);
  }
  c->Stop();
  EXPECT_EQ(applies.load(), 1);
  EXPECT_EQ(c->Snapshot().apply_failures, 0U);
}

// --- Loop backoff (XRES-5) ---------------------------------------------------

TEST_F(ColdConsumerTest, PoisonedBatchBacksOffInsteadOfBusySpin) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;  // Flush eagerly so the poison is hit each loop.
  cfg.jitter_fraction = 0.0;
  cfg.loop_initial_backoff = 5ms;
  cfg.loop_max_backoff = 20ms;
  cfg.queue_read_timeout = 1ms;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Terminal failure on every apply: without backoff the loop would re-apply
  // thousands of times over a short window; with capped backoff it is bounded.
  std::atomic<int> apply_calls{0};
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([&apply_calls](std::span<const core::ops::WriteOp>, core::SequenceId) {
        apply_calls.fetch_add(1, std::memory_order_relaxed);
        return core::Result<void>(
            std::unexpected(core::Error{core::ErrorCode::kCorruption, "poison"}));
      });

  c->Start();
  std::this_thread::sleep_for(150ms);
  c->Stop();

  // 150ms with a 5ms->20ms capped backoff bounds attempts well under a busy
  // spin (which would be 10k+). A generous ceiling keeps the test non-flaky.
  EXPECT_LE(apply_calls.load(), 60) << "poisoned batch busy-spun instead of backing off";
  EXPECT_GE(apply_calls.load(), 1);
  EXPECT_GT(c->Snapshot().apply_poisoned, 0U);
}

// --- Queue-read error handling (C2) ------------------------------------------

TEST_F(ColdConsumerTest, QueueReadUnavailableStopsLoop) {
  auto c = MakeConsumer();

  std::atomic<int> read_call_count{0};
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillRepeatedly([&read_call_count](
                          core::ShardId, core::SequenceId, size_t, core::Duration,
                          core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
        read_call_count.fetch_add(1, std::memory_order_relaxed);
        return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
      });

  // The mock always returns kUnavailable, which makes the loop self-terminate.
  // Stop()'s Join blocks until the thread exits, so if the loop were spinning
  // this test would time out at the ctest TIMEOUT rather than pass.
  c->Start();
  c->Stop();

  EXPECT_LE(read_call_count.load(), 1)
      << "Consumer kept polling after kUnavailable — loop did not exit";
}

TEST_F(ColdConsumerTest, QueueReadTransientErrorIncrementsCounter) {
  auto c = MakeConsumer();

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(std::unexpected(core::Error{core::ErrorCode::kInternal, "transient"})))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  EXPECT_EQ(c->Snapshot().queue_read_failures, 1U);
}

// --- Terminal retry classification (C3) --------------------------------------

TEST_F(ColdConsumerTest, FlushDoesNotRetryOnTerminalError) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.retry_initial_backoff = 0ms;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // kCorruption is terminal — retrying won't help.
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillOnce(Return(std::unexpected(core::Error{core::ErrorCode::kCorruption, "bad data"})));

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.apply_poisoned, 1U);
  EXPECT_EQ(snap.retry_attempts, 0U);
  EXPECT_EQ(snap.ops_flushed, 0U);
  // Entries reinserted into buffer so the failure is re-surfaced.
  EXPECT_GT(c->Buffer().Size(), 0U);
}

// --- Flush trigger classification (O4) ---------------------------------------

TEST_F(ColdConsumerTest, QuietFlushIncrementsQuietCounter) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.safety_margin = 300s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();  // quiet window not yet elapsed — nothing flushes
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.flushes_quiet, 1U);
  EXPECT_EQ(snap.flushes_deadline, 0U);
  EXPECT_EQ(snap.flushes_aggressive, 0U);
}

TEST_F(ColdConsumerTest, DeadlineFlushIncrementsDeadlineCounter) {
  // Short eviction + long quiet_threshold → eviction_deadline always dominates.
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 1000s;
  cfg.safety_margin = 10s;
  cfg.jitter_fraction = 0.0;
  const core::EvictionPolicy short_policy{core::EvictionTTL{20}};
  auto c =
      std::make_unique<ColdConsumer>(queue_, cold_, kShard, cfg, short_policy, clock_.SteadyFn());

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();  // deadline is t=10s (first_seen + 20s eviction - 10s safety margin)
  clock_.Advance(11s);
  c->Drain();
  c->Flush();

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.flushes_deadline, 1U);
  EXPECT_EQ(snap.flushes_quiet, 0U);
  EXPECT_EQ(snap.flushes_aggressive, 0U);
}

// --- Thread lifecycle (C4) ---------------------------------------------------

TEST_F(ColdConsumerTest, StopIsIdempotent) {
  auto c = MakeConsumer();
  c->Start();
  EXPECT_TRUE(c->IsRunning());
  c->Stop();
  EXPECT_FALSE(c->IsRunning());
  c->Stop();  // second call is a no-op
  EXPECT_FALSE(c->IsRunning());
}

TEST_F(ColdConsumerTest, DoubleStartIsNoop) {
  auto c = MakeConsumer();
  c->Start();
  c->Start();  // Second call returns early.
  EXPECT_TRUE(c->IsRunning());
  c->Stop();
}

TEST_F(ColdConsumerTest, StopBeforeStartIsSafe) {
  auto c = MakeConsumer();
  c->Stop();
  EXPECT_FALSE(c->IsRunning());
}

// --- Graceful drain on shutdown (G6) -----------------------------------------

TEST_F(ColdConsumerTest, DrainAndFlushPersistsBufferOnGracefulStop) {
  ColdConsumer::Config cfg;
  // Long quiet threshold and a small safety margin (well under the fixture's
  // 3600s eviction TTL) so neither the quiet nor the eviction-deadline trigger
  // fires within the test window — the buffered entries survive only because
  // the graceful drain flushes them.
  cfg.quiet_threshold = 3600s;
  cfg.safety_margin = 60s;
  cfg.jitter_fraction = 0.0;
  cfg.queue_read_timeout = 5ms;
  auto c = MakeConsumer(cfg);

  constexpr int kWrites = 5;
  std::vector<core::QueueEntry> entries;
  entries.reserve(kWrites);
  for (int i = 0; i < kWrites; ++i) {
    entries.push_back(MakeWriteEntry(static_cast<core::SequenceId>(i) + 1,
                                     {"SET", "k" + std::to_string(i), "v"}));
  }
  // Deliver the batch once, then nothing — the loop absorbs all kWrites into the
  // buffer and (quiet=3600s) leaves them unflushed.
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  std::atomic<int> applied{0};
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([&applied](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        applied.fetch_add(static_cast<int>(ops.size()));
        return core::Result<void>{};
      });
  std::atomic<int> checkpoints{0};
  EXPECT_CALL(cold_, Checkpoint(kShard, _))
      .WillRepeatedly([&checkpoints](core::ShardId, core::SequenceId) {
        checkpoints.fetch_add(1);
        return core::Result<void>{};
      });
  std::atomic<core::SequenceId> commit_seq{0};
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&commit_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        commit_seq.store(s);
        return core::Result<void>{};
      });
  // Nothing is power-durable until the drain waits; the flusher catches up.
  PowerDurableLog wal(1, /*catch_up=*/true);
  wal.Install(queue_);

  c->Start();
  // Let the loop absorb the batch.
  for (int i = 0; i < 200 && c->Buffer().Size() < static_cast<size_t>(kWrites); ++i) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(c->Buffer().Size(), static_cast<size_t>(kWrites))
      << "steady loop should have buffered all writes without flushing";

  // Graceful stop with a generous deadline: the drain must flush everything.
  c->RequestStopAndDrain(std::chrono::steady_clock::now() + 5s);
  c->Join();

  EXPECT_EQ(c->Buffer().Size(), 0U) << "graceful drain must empty the buffer";
  EXPECT_EQ(applied.load(), kWrites) << "every buffered write must reach cold";
  EXPECT_GE(checkpoints.load(), 1) << "drain must checkpoint before advancing the commit";
  // The drained slice is durable, so the commit advanced past it — a reopen would
  // NOT need to replay these seqs.
  EXPECT_EQ(commit_seq.load(), static_cast<core::SequenceId>(kWrites));
  EXPECT_EQ(c->Snapshot().last_commit_seq, static_cast<core::SequenceId>(kWrites));
  EXPECT_FALSE(wal.Awaited().empty()) << "the drain persisted without the power gate";
  EXPECT_EQ(c->Snapshot().durability_waits_timed_out, 0U);
}

TEST_F(ColdConsumerTest, DrainDeadlineTruncatesAndReportsWithoutBlocking) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 3600s;
  cfg.safety_margin = 60s;
  cfg.jitter_fraction = 0.0;
  cfg.queue_read_timeout = 5ms;
  // Force one entry per flush batch so the wedged ApplyBatch is hit while the
  // buffer still has work, exercising the deadline path mid-drain.
  cfg.max_flush_batch_size = 1;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.reserve(4);
  for (int i = 0; i < 4; ++i) {
    entries.push_back(MakeWriteEntry(static_cast<core::SequenceId>(i) + 1,
                                     {"SET", "k" + std::to_string(i), "v"}));
  }
  EXPECT_CALL(queue_, Read(_, _, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // ApplyBatch sleeps longer than the drain budget, so the deadline elapses
  // before the buffer empties — the drain must abandon the rest, not block.
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([](std::span<const core::ops::WriteOp>, core::SequenceId) {
        std::this_thread::sleep_for(60ms);
        return core::Result<void>{};
      });

  c->Start();
  for (int i = 0; i < 200 && c->Buffer().Size() < 4; ++i) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(c->Buffer().Size(), 4U);

  const auto start = std::chrono::steady_clock::now();
  c->RequestStopAndDrain(std::chrono::steady_clock::now() + 50ms);
  c->Join();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  // Bounded: Join returns near the deadline (allow one in-flight ApplyBatch +
  // slack), never an unbounded block on the full buffer.
  EXPECT_LT(elapsed, 400ms) << "drain must be deadline-bounded, not block on the wedged store";
  // The remaining slice was left in the buffer for WAL replay (no silent loss).
  EXPECT_GT(c->Buffer().Size(), 0U) << "truncated drain must leave the rest for replay";
}

// --- Replay fed by a scan ----------------------------------------------------

core::Result<void> WipeFails(core::ShardId /*shard*/) {
  return std::unexpected(core::Error{core::ErrorCode::kInternal, "wipe failed (test)"});
}

TEST_F(ColdConsumerTest, BeginReplayStartsPastTheCommittedOffset) {
  ON_CALL(queue_, CommittedOffset(core::kColdConsumer, kShard))
      .WillByDefault(Return(core::Result<std::optional<core::SequenceId>>(4)));
  auto c = MakeConsumer();

  auto begun = c->BeginReplay();
  ASSERT_TRUE(begun.has_value());
  EXPECT_EQ(*begun, 5U);
  EXPECT_EQ(c->LatestDrainedSeq(), 4U);
}

// With nothing committed, the cursor starts at the first seq.
TEST_F(ColdConsumerTest, AReplayBatchMustStartAtTheCursor) {
  auto c = MakeConsumer();
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  const std::atomic<bool> cancel{false};

  const std::vector<core::QueueEntry> batch{MakeWriteEntry(3, {"SET", "k", "v"})};
  auto applied = c->ApplyReplayBatch(batch, cancel);
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kInternal);
  EXPECT_EQ(c->Buffer().Size(), 0U);
}

// A failed wipe holds the batch at its Flush; the retry resumes there
// and consumes the rest.
TEST_F(ColdConsumerTest, AReplayBatchRetriesAFailedWipeThenConsumesTheRest) {
  ColdConsumer::Config cfg;
  cfg.loop_max_backoff = 2ms;
  auto c = MakeConsumer(cfg);
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  EXPECT_CALL(cold_, Wipe(kShard))
      .WillOnce(WipeFails)
      .WillOnce(WipeFails)
      .WillOnce(Return(core::Result<void>{}));
  const std::vector<core::QueueEntry> batch{
      MakeWriteEntry(kFirst, {"SET", "a", "v"}),
      MakeFlushEntry(kFirst + 1),
      MakeWriteEntry(kFirst + 2, {"SET", "b", "v"}),
  };
  const std::atomic<bool> cancel{false};

  ASSERT_TRUE(c->ApplyReplayBatch(batch, cancel).has_value());
  EXPECT_EQ(c->LatestDrainedSeq(), kFirst + 2);
  EXPECT_FALSE(testing::BufferRead(c->Buffer(), "a").has_value())
      << "the wipe left a pre-Flush write";
  EXPECT_TRUE(testing::BufferRead(c->Buffer(), "b").has_value());
}

// A wipe that keeps failing gives up after drain_grace, holding the
// cursor at the Flush, so recovery fails instead of hanging.
TEST_F(ColdConsumerTest, AReplayBatchGivesUpOnAWipeThatKeepsFailing) {
  ColdConsumer::Config cfg;
  cfg.drain_grace = 20ms;
  cfg.loop_max_backoff = 5ms;
  auto c = MakeConsumer(cfg);
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  EXPECT_CALL(cold_, Wipe(kShard)).WillRepeatedly(WipeFails);
  const std::vector<core::QueueEntry> batch{
      MakeWriteEntry(kFirst, {"SET", "a", "v"}),
      MakeFlushEntry(kFirst + 1),
      MakeWriteEntry(kFirst + 2, {"SET", "b", "v"}),
  };
  const std::atomic<bool> cancel{false};

  auto applied = c->ApplyReplayBatch(batch, cancel);
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(c->LatestDrainedSeq(), kFirst);
  EXPECT_FALSE(testing::BufferRead(c->Buffer(), "b").has_value());
}

TEST_F(ColdConsumerTest, ACancelStopsAWipeRetry) {
  ColdConsumer::Config cfg;
  cfg.drain_grace = std::chrono::hours{1};
  auto c = MakeConsumer(cfg);
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  std::atomic<bool> cancel{false};
  EXPECT_CALL(cold_, Wipe(kShard)).WillRepeatedly([&cancel](core::ShardId shard) {
    cancel.store(true);
    return WipeFails(shard);
  });

  const std::vector<core::QueueEntry> batch{MakeFlushEntry(kFirst)};
  auto applied = c->ApplyReplayBatch(batch, cancel);
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), core::ErrorCode::kUnavailable);
}

TEST_F(ColdConsumerTest, FinishReplayFlushesTheBufferAndCommits) {
  auto c = MakeConsumer();
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  EXPECT_CALL(cold_, ApplyBatch(_, _)).WillOnce(Return(core::Result<void>{}));
  EXPECT_CALL(queue_, CommitOffset(core::kColdConsumer, kShard, kFirst + 1))
      .WillOnce(Return(core::Result<void>{}));
  const std::vector<core::QueueEntry> batch{
      MakeWriteEntry(kFirst, {"SET", "a", "v"}),
      MakeWriteEntry(kFirst + 1, {"SET", "b", "v"}),
  };
  const std::atomic<bool> cancel{false};
  ASSERT_TRUE(c->ApplyReplayBatch(batch, cancel).has_value());

  ASSERT_TRUE(c->FinishReplay(kFirst + 2, cancel).has_value());
  EXPECT_EQ(c->Buffer().Size(), 0U);
  EXPECT_EQ(c->Snapshot().last_commit_seq, kFirst + 1);
}

// The Scan must bring the cursor to the end it was asked for; short of
// it, recovery fails rather than serve a cold store behind the log.
TEST_F(ColdConsumerTest, FinishReplayShortOfItsEndFails) {
  auto c = MakeConsumer();
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  const std::vector<core::QueueEntry> batch{MakeWriteEntry(kFirst, {"SET", "a", "v"})};
  const std::atomic<bool> cancel{false};
  ASSERT_TRUE(c->ApplyReplayBatch(batch, cancel).has_value());

  auto finished = c->FinishReplay(kFirst + 2, cancel);
  ASSERT_FALSE(finished.has_value());
  EXPECT_EQ(finished.error().code(), core::ErrorCode::kInternal);
  EXPECT_NE(finished.error().message().find("short of its end"), std::string::npos);
}

// Replay's drain request: everything at or below the seq leaves the
// buffer, through cold, whatever its flush schedule.
TEST_F(ColdConsumerTest, FlushThroughEmptiesTheBufferUpToItsSeq) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 3600s;
  cfg.max_flush_batch_size = 1;
  auto c = MakeConsumer(cfg);
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  std::vector<std::string> applied;
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly([&applied](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
        for (const auto& op : ops) applied.emplace_back(core::ops::PrimaryKey(op));
        return core::Result<void>{};
      });
  const std::vector<core::QueueEntry> batch{
      MakeWriteEntry(kFirst, {"SET", "a", "v"}),
      MakeWriteEntry(kFirst + 1, {"SET", "b", "v"}),
      MakeWriteEntry(kFirst + 2, {"SET", "c", "v"}),
  };
  const std::atomic<bool> cancel{false};
  ASSERT_TRUE(c->ApplyReplayBatch(batch, cancel).has_value());

  ASSERT_TRUE(c->FlushThrough(kFirst + 1).has_value());
  EXPECT_GE(applied.size(), 2U);
  const auto oldest = c->Buffer().OldestPendingSeq();
  EXPECT_TRUE(!oldest.has_value() || *oldest > kFirst + 1);
}

// A forced flush that cold keeps refusing fails within drain_grace, so
// recovery fails loudly instead of hanging.
TEST_F(ColdConsumerTest, FlushThroughGivesUpOnAColdThatKeepsFailing) {
  ColdConsumer::Config cfg;
  cfg.drain_grace = 20ms;
  cfg.loop_max_backoff = 5ms;
  auto c = MakeConsumer(cfg);
  ASSERT_EQ(c->BeginReplay().value(), kFirst);
  EXPECT_CALL(cold_, ApplyBatch(_, _))
      .WillRepeatedly(Return(core::Result<void>(
          std::unexpected(core::Error{core::ErrorCode::kUnavailable, "cold down (test)"}))));
  const std::vector<core::QueueEntry> batch{MakeWriteEntry(kFirst, {"SET", "a", "v"})};
  const std::atomic<bool> cancel{false};
  ASSERT_TRUE(c->ApplyReplayBatch(batch, cancel).has_value());

  auto flushed = c->FlushThrough(kFirst);
  ASSERT_FALSE(flushed.has_value());
  EXPECT_EQ(flushed.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(c->Buffer().Size(), 1U) << "the refused batch stays buffered";
}

// --- COLDC-5: oldest_unflushed_age lag signal ---------------------------------

TEST_F(ColdConsumerTest, OldestUnflushedAgeIsZeroWhenBufferEmpty) {
  auto c = MakeConsumer();
  EXPECT_EQ(c->Snapshot().oldest_unflushed_age, 0ms);
}

TEST_F(ColdConsumerTest, OldestUnflushedAgePopulated) {
  auto c = MakeConsumer();

  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWriteEntry(1, {"SET", "ka", "va"})}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  ASSERT_EQ(c->Buffer().Size(), 1U);
  EXPECT_EQ(c->Snapshot().oldest_unflushed_age, 0ms);

  clock_.Advance(5s);
  EXPECT_EQ(c->Snapshot().oldest_unflushed_age, 5000ms);
}

TEST_F(ColdConsumerTest, OldestUnflushedAgeTracksNextOldestAfterFlush) {
  auto c = MakeConsumer();

  EXPECT_CALL(queue_, Read(kShard, _, _, _, _))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWriteEntry(1, {"SET", "ka", "va"})}))
      .WillOnce(Return(std::vector<core::QueueEntry>{MakeWriteEntry(2, {"SET", "kb", "vb"})}))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  clock_.Advance(10s);
  c->Drain();
  ASSERT_EQ(c->Buffer().Size(), 2U);

  // Past "ka"'s quiet deadline (30s + <=3s jitter) but short of "kb"'s.
  clock_.Advance(24s);
  EXPECT_EQ(c->Snapshot().oldest_unflushed_age, 34000ms);

  ASSERT_EQ(c->Flush(), ColdConsumer::FlushOutcome::kProgress);
  ASSERT_EQ(c->Buffer().Size(), 1U);
  // Not latched at the flushed entry's age: the signal tracks the live minimum.
  EXPECT_EQ(c->Snapshot().oldest_unflushed_age, 24000ms);
}

}  // namespace
}  // namespace abyss::consumer
