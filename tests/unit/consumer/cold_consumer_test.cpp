#include "abyss/consumer/cold_consumer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
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

core::QueueEntry MakeWriteEntry(core::SequenceId seq, std::initializer_list<std::string> cmd_args) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Write{.cmd = core::RespCommand{.args = std::vector<std::string>(cmd_args)}},
  };
}

core::QueueEntry MakeResolvedEntry(core::SequenceId seq, core::Decision decision,
                                   // NOLINTNEXTLINE(readability-named-parameter)
                                   std::optional<std::vector<std::string>> materialised) {
  core::entry::Resolved r;
  r.decision = decision;
  if (materialised.has_value()) {
    r.materialised_op = core::RespCommand{.args = std::move(*materialised)};
  }
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallClock::now(),
      .payload = std::move(r),
  };
}

class ColdConsumerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ON_CALL(queue_, Read(_, _, _, _)).WillByDefault(Return(std::vector<core::QueueEntry>{}));
    ON_CALL(queue_, Ack(_, _, _)).WillByDefault(Return(core::Result<void>{}));
    ON_CALL(cold_, ApplyBatch(_)).WillByDefault(Return(core::Result<void>{}));
  }

  std::unique_ptr<ColdConsumer> MakeConsumer(ColdConsumer::Config cfg = {}) {
    cfg.rng_seed = 42;
    return std::make_unique<ColdConsumer>(queue_, cold_, kShard, cfg,
                                          core::EvictionPolicy{core::EvictionTTL{3600}},
                                          clock_.SteadyFn(), clock_.WallFn());
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  NiceMock<testing::MockQueue> queue_;
  NiceMock<testing::MockColdStore> cold_;
  testing::TestClock clock_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Drain path ---------------------------------------------------------------

TEST_F(ColdConsumerTest, DrainAbsorbsWriteEntriesIntoBuffer) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "ka", "va"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "kb", "vb"}));

  EXPECT_CALL(queue_, Read(core::kColdConsumer, kShard, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 2);
  auto read = c->Buffer().Read("ka");
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "va");
}

TEST_F(ColdConsumerTest, DrainHandlesResolvedApplyDecision) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(
      MakeResolvedEntry(5, core::Decision::kApply, std::vector<std::string>{"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 1);
}

TEST_F(ColdConsumerTest, DrainSkipsResolvedSkipDecision) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeResolvedEntry(5, core::Decision::kSkip, std::nullopt));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 0);
}

TEST_F(ColdConsumerTest, DrainCountsParseFailuresAndContinues) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"BOGUS", "key"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 1);
  EXPECT_EQ(c->Snapshot().parse_failures, 1U);
}

TEST_F(ColdConsumerTest, DrainExpandsMultiKeyDel) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"DEL", "a", "b", "c"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 3);
}

TEST_F(ColdConsumerTest, DrainExpandsMset) {
  auto c = MakeConsumer();

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"MSET", "a", "1", "b", "2"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Buffer().Size(), 2);
}

// --- Flush path ---------------------------------------------------------------

TEST_F(ColdConsumerTest, QuietWindowElapsedTriggersApplyBatch) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  std::vector<std::pair<std::string, std::string>> observed_sets;
  EXPECT_CALL(cold_, ApplyBatch(_))
      .WillOnce([&observed_sets](std::span<const core::ops::WriteOp> ops) {
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  std::vector<std::string> observed_dels;
  EXPECT_CALL(cold_, ApplyBatch(_))
      .WillOnce([&observed_dels](std::span<const core::ops::WriteOp> ops) {
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  EXPECT_CALL(cold_, ApplyBatch(_))
      .WillOnce(Return(std::unexpected(core::Error{core::ErrorCode::kUnavailable, "transient"})))
      .WillOnce(Return(std::unexpected(core::Error{core::ErrorCode::kUnavailable, "transient"})))
      .WillOnce(Return(core::Result<void>{}));

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();  // Retries inside ApplyBatchWithRetry until success.

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.apply_failures, 2U);
  EXPECT_GE(snap.retry_attempts, 2U);
  EXPECT_EQ(snap.ops_flushed, 1U);
}

TEST_F(ColdConsumerTest, AbsTtlExpiredEntryIsDroppedWithoutApply) {
  // ParseWriteOp("SET EX ...") reads real wall-clock, so we bypass RESP parsing.
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  const auto now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count());
  const uint64_t ttl_ms = now_ms + 1000;  // 1s in the future.

  c->Buffer().Absorb(
      "k", core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = ttl_ms}},
      core::EvictionTTL{3600}, /*seq=*/1);

  EXPECT_CALL(cold_, ApplyBatch(_)).Times(0);

  clock_.Advance(60s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Snapshot().entries_dropped_abs_ttl, 1U);
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Expect at least one ApplyBatch when aggressive mode triggers flush.
  EXPECT_CALL(cold_, ApplyBatch(_)).Times(AtLeast(1));

  c->Drain();
  c->Flush();
  EXPECT_EQ(c->CurrentMode(), ColdConsumer::Mode::kAggressive);

  // After aggressive flush, buffer should drain below low-water and return to normal.
  c->Drain();
  c->Flush();
  EXPECT_EQ(c->CurrentMode(), ColdConsumer::Mode::kNormal);
  EXPECT_EQ(c->Buffer().Size(), 0);
}

// --- Low-water ack ------------------------------------------------------------

TEST_F(ColdConsumerTest, AckAdvancesToDrainedSeqWhenBufferEmpty) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId ack_seq = 0;
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, _))
      .WillRepeatedly(
          [&ack_seq](core::ConsumerId /*consumer*/, core::ShardId /*shard*/, core::SequenceId s) {
            ack_seq = s;
            return core::Result<void>{};
          });

  c->Drain();
  c->Flush();  // Drain entry 1.
  clock_.Advance(31s);
  c->Drain();
  c->Flush();  // Flush, ack.

  EXPECT_EQ(ack_seq, 1U);
}

TEST_F(ColdConsumerTest, AckBoundedByOldestPendingSeq) {
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(first))
      .WillOnce(Return(second))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId ack_seq = 0;
  EXPECT_CALL(queue_, Ack(_, _, _))
      .WillRepeatedly(
          [&ack_seq](core::ConsumerId /*consumer*/, core::ShardId /*shard*/, core::SequenceId s) {
            ack_seq = s;
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

  // Oldest pending is 2 after flush, so we ack up to 1 (min of drained=2, oldest-1=1).
  EXPECT_EQ(ack_seq, 1U);
}

// --- Queue-read error handling (C2) ------------------------------------------

TEST_F(ColdConsumerTest, QueueReadUnavailableStopsLoop) {
  auto c = MakeConsumer();

  std::atomic<int> read_call_count{0};
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillRepeatedly([&read_call_count](
                          core::ConsumerId, core::ShardId, size_t,
                          core::Duration) -> core::Result<std::vector<core::QueueEntry>> {
        read_call_count.fetch_add(1, std::memory_order_relaxed);
        return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
      });

  c->Start();
  // A healthy consumer would spin on Read every queue_read_timeout (default 50ms).
  // With C2, it bails after the first kUnavailable.
  std::this_thread::sleep_for(300ms);
  c->Stop();

  EXPECT_LE(read_call_count.load(), 3)
      << "Consumer kept polling after kUnavailable — loop did not exit";
}

TEST_F(ColdConsumerTest, QueueReadTransientErrorIncrementsCounter) {
  auto c = MakeConsumer();

  EXPECT_CALL(queue_, Read(_, _, _, _))
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // kCorruption is terminal — retrying won't help.
  EXPECT_CALL(cold_, ApplyBatch(_))
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
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
  auto c = std::make_unique<ColdConsumer>(queue_, cold_, kShard, cfg,
                                          core::EvictionPolicy{core::EvictionTTL{20}},
                                          clock_.SteadyFn(), clock_.WallFn());

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
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

}  // namespace
}  // namespace abyss::consumer
