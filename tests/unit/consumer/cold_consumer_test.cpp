#include "abyss/consumer/cold_consumer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/consumer_rpc.h"
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
    r.materialised_ops.push_back(core::RespCommand{.args = std::move(*materialised)});
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
    ON_CALL(cold_, ApplyBatch(_, _)).WillByDefault(Return(core::Result<void>{}));
  }

  std::unique_ptr<ColdConsumer> MakeConsumer(ColdConsumer::Config cfg = {}) {
    cfg.rng_seed = 42;
    return std::make_unique<ColdConsumer>(queue_, cold_, kShard, cfg, policy_, rpc_,
                                          clock_.SteadyFn(), clock_.WallFn());
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  NiceMock<testing::MockQueue> queue_;
  NiceMock<testing::MockColdStore> cold_;
  testing::TestClock clock_;
  // Outlives every consumer the test fixture builds; consumer holds a const ref.
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  core::ConsumerRpc rpc_;
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

// --- XERR-5: cold parse-poison quarantine ------------------------------------

TEST_F(ColdConsumerTest, ParsePoisonDoesNotAdvanceAckPastUnabsorbedSeq) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  // seq 1 is genuine skew poison: HSET HAS a parser and that parser rejects an
  // odd field/value list, so another tier accepted bytes cold cannot decode.
  // seq 2 is a valid SET that still absorbs. The ack must NOT pass seq 1.
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"HSET", "h", "f"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // The valid SET (seq 2) must still reach cold — the poison quarantines the
  // ack frontier, it does not drop the surrounding writes.
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

  core::SequenceId ack_seq = 0;
  bool acked = false;
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&ack_seq, &acked](core::ConsumerId, core::ShardId, core::SequenceId s) {
        ack_seq = s;
        acked = true;
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
  // Either no ack was issued, or it stayed at the floor (never >= poison seq 1).
  if (acked) EXPECT_EQ(ack_seq, 0U) << "ack advanced past the poison entry";
}

TEST_F(ColdConsumerTest, ParsePoisonEmitsCriticalMetricAndStalls) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  cfg.loop_initial_backoff = 5ms;
  cfg.loop_max_backoff = 20ms;
  cfg.queue_read_timeout = 1ms;
  auto c = MakeConsumer(cfg);

  // A single poison Write at seq 5 re-delivered every loop (the ack floor never
  // passes it). The loop must back off rather than busy-spin.
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(5, {"HSET", "h", "f"}));
  std::atomic<int> read_calls{0};
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillRepeatedly(
          [&entries, &read_calls](core::ConsumerId, core::ShardId, size_t, core::Duration) {
            read_calls.fetch_add(1, std::memory_order_relaxed);
            return entries;  // queue keeps re-delivering the un-acked poison
          });

  core::SequenceId max_ack = 0;
  EXPECT_CALL(queue_, Ack(_, _, _))
      .WillRepeatedly([&max_ack](core::ConsumerId, core::ShardId, core::SequenceId s) {
        max_ack = std::max(max_ack, s);
        return core::Result<void>{};
      });

  c->Start();
  std::this_thread::sleep_for(150ms);
  c->Stop();

  EXPECT_GT(c->Snapshot().parse_poison, 0U);
  EXPECT_LT(max_ack, 5U) << "ack advanced to or past the poison seq";
  // Capped 5ms->20ms backoff bounds reads well under a busy spin (10k+).
  EXPECT_LE(read_calls.load(), 80) << "poison entry busy-spun instead of backing off";
}

// --- ENGINE-9: the idle backoff must not outlast a reader's gate deadline ----

TEST_F(ColdConsumerTest, IdleBackoffIsInterruptedByAReaderWaitingToDrain) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  cfg.queue_read_timeout = 1ms;
  cfg.loop_initial_backoff = 1ms;
  // Far longer than the reader's deadline below: if the loop sleeps this out,
  // the read-consistency gate fails closed on a consumer that is merely idle.
  cfg.loop_max_backoff = 5000ms;
  auto c = MakeConsumer(cfg);

  // Idle until the entry appears, so the loop climbs to its backoff ceiling.
  std::atomic<bool> release{false};
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillRepeatedly([&release](core::ConsumerId, core::ShardId, size_t, core::Duration) {
        std::vector<core::QueueEntry> out;
        if (release.load(std::memory_order_acquire)) {
          out.push_back(MakeWriteEntry(9, {"SET", "k", "v"}));
        }
        return out;
      });

  c->Start();
  // Let the backoff saturate before anything is available to drain.
  std::this_thread::sleep_for(120ms);
  release.store(true, std::memory_order_release);

  // A reader's gate deadline is far shorter than the backoff ceiling. This must
  // still succeed: waiting is what tells the consumer to stop sleeping.
  const bool drained = c->WaitForDrainedSeq(9, 500ms);
  c->Stop();

  EXPECT_TRUE(drained) << "reader's wait expired while the consumer slept out its idle backoff";
}

// --- XERR-6: a missing parser is a capability gap, not poison -----------------

TEST_F(ColdConsumerTest, WriteWithNoParserIsSkippedNotPoisoned) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 0s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  // INCR is registered as a write command but no tier implements a typed op for
  // it, so hot could not materialise it either. Quarantining here would let any
  // client pin this shard's WAL retention forever with one ordinary command.
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"INCR", "counter"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "k", "v"}));

  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId ack_seq = 0;
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&ack_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        ack_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();

  const auto snap = c->Snapshot();
  EXPECT_EQ(snap.parse_poison, 0U) << "an unimplemented command was quarantined as poison";
  EXPECT_EQ(snap.unsupported_ops, 1U);
  // The frontier moved past both entries: nothing is pinned.
  EXPECT_EQ(snap.latest_drained_seq, 2U);
  EXPECT_EQ(ack_seq, 2U) << "WAL retention stayed pinned behind an unimplementable write";
}

TEST_F(ColdConsumerTest, ResolvedMaterialisedOpPoisonClampsAck) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  auto c = MakeConsumer(cfg);

  // A Resolved whose materialised op fails ParseWriteOp; the poison floors the
  // ack at ref-1 and increments parse_poison.
  std::vector<core::QueueEntry> entries;
  entries.push_back(
      MakeResolvedEntry(7, core::Decision::kApply, std::vector<std::string>{"HSET", "h", "f"}));
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  core::SequenceId max_ack = 0;
  EXPECT_CALL(queue_, Ack(_, _, _))
      .WillRepeatedly([&max_ack](core::ConsumerId, core::ShardId, core::SequenceId s) {
        max_ack = std::max(max_ack, s);
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Snapshot().parse_poison, 1U);
  EXPECT_LT(max_ack, 7U) << "ack advanced to or past the resolved poison ref";
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
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

  EXPECT_CALL(queue_, Read(_, _, _, _))
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

  EXPECT_CALL(cold_, ApplyBatch(_, _)).Times(0);

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

// Pins "queue.Ack precedes rpc.Fulfill" for Flush: the engine returns FLUSHDB
// +OK on RPC fulfilment, so the per-shard ack must be durable first (ADP-006).
TEST_F(ColdConsumerTest, FlushAckPersistedBeforeRpcFulfilled) {
  auto c = MakeConsumer();

  constexpr core::SequenceId kFlushSeq = 7;
  const core::RpcId rpc_id = core::MakeFlushRpcId(core::kColdConsumer, kShard, kFlushSeq);
  auto fut = rpc_.Register(rpc_id);

  bool ack_observed = false;
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, kFlushSeq))
      .WillOnce([&](core::ConsumerId, core::ShardId, core::SequenceId) {
        EXPECT_NE(fut.wait_for(0ms), std::future_status::ready)
            << "Flush RPC fulfilled before its ack was persisted";
        ack_observed = true;
        return core::Result<void>{};
      });

  EXPECT_CALL(cold_, Wipe(kShard)).WillOnce(Return(core::Result<void>{}));

  std::vector<core::QueueEntry> entries;
  entries.push_back(core::QueueEntry{
      .seq = kFlushSeq,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Flush{},
  });
  EXPECT_CALL(queue_, Read(core::kColdConsumer, kShard, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  c->Drain();

  EXPECT_TRUE(ack_observed) << "Flush did not persist an ack";
  ASSERT_EQ(fut.wait_for(0ms), std::future_status::ready) << "Flush RPC not fulfilled after Drain";
  EXPECT_TRUE(fut.get().IsSimpleString());
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

// --- Cold-durability ack gate (XDUR-1) ---------------------------------------

TEST_F(ColdConsumerTest, AckGatedOnCheckpointBeforeAdvancing) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.checkpoint_max_flushes = 1;  // Checkpoint every flush so the ack can advance.
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Checkpoint must be issued before the ack advances, and for a seq <= the
  // applied frontier.
  core::SequenceId checkpointed_seq = 0;
  EXPECT_CALL(cold_, Checkpoint(kShard, _))
      .WillRepeatedly([&checkpointed_seq](core::ShardId, core::SequenceId s) {
        checkpointed_seq = s;
        return core::Result<void>{};
      });
  core::SequenceId ack_seq = 0;
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&ack_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        ack_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(checkpointed_seq, 1U);
  EXPECT_EQ(ack_seq, 1U);
  EXPECT_LE(ack_seq, checkpointed_seq) << "ack advanced past the cold checkpoint frontier";
}

TEST_F(ColdConsumerTest, AckBlockedWhenCheckpointFails) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.checkpoint_max_flushes = 1;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "k", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Checkpoint fails — the cold flush is applied (memtable) but not durable, so
  // the ack must NOT advance and the WAL stays pinned.
  EXPECT_CALL(cold_, Checkpoint(kShard, _))
      .WillRepeatedly(Return(std::unexpected(core::Error{core::ErrorCode::kUnavailable, "fsync"})));
  EXPECT_CALL(queue_, Ack(_, _, _)).Times(0);

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(c->Snapshot().last_ack_seq, 0U) << "ack advanced despite a failed checkpoint";
}

TEST_F(ColdConsumerTest, AckClampedToDurableSeq) {
  ColdConsumer::Config cfg;
  cfg.quiet_threshold = 30s;
  cfg.jitter_fraction = 0.0;
  cfg.checkpoint_max_flushes = 1;
  auto c = MakeConsumer(cfg);

  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeWriteEntry(1, {"SET", "a", "v"}));
  entries.push_back(MakeWriteEntry(2, {"SET", "b", "v"}));
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillOnce(Return(entries))
      .WillRepeatedly(Return(std::vector<core::QueueEntry>{}));

  // Only seq 1 is durable in the WAL; the cold ack must clamp to it even though
  // both seqs were drained, checkpointed, and otherwise ackable.
  EXPECT_CALL(queue_, DurableSeq(kShard)).WillRepeatedly(Return(core::Result<core::SequenceId>(1)));
  core::SequenceId ack_seq = 0;
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&ack_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        ack_seq = s;
        return core::Result<void>{};
      });

  c->Drain();
  c->Flush();
  clock_.Advance(31s);
  c->Drain();
  c->Flush();

  EXPECT_EQ(ack_seq, 1U) << "ack advanced past the durable WAL tail";
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
  EXPECT_CALL(queue_, Read(_, _, _, _))
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
  EXPECT_CALL(queue_, Read(_, _, _, _))
      .WillRepeatedly([&read_call_count](
                          core::ConsumerId, core::ShardId, size_t,
                          core::Duration) -> core::Result<std::vector<core::QueueEntry>> {
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
  const core::EvictionPolicy short_policy{core::EvictionTTL{20}};
  auto c = std::make_unique<ColdConsumer>(queue_, cold_, kShard, cfg, short_policy, rpc_,
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
  EXPECT_CALL(queue_, Read(_, _, _, _))
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
  std::atomic<core::SequenceId> ack_seq{0};
  EXPECT_CALL(queue_, Ack(core::kColdConsumer, kShard, _))
      .WillRepeatedly([&ack_seq](core::ConsumerId, core::ShardId, core::SequenceId s) {
        ack_seq.store(s);
        return core::Result<void>{};
      });

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
  EXPECT_GE(checkpoints.load(), 1) << "drain must checkpoint before advancing the ack";
  // The drained slice is durable, so the ack advanced past it — a reopen would
  // NOT need to replay these seqs.
  EXPECT_EQ(ack_seq.load(), static_cast<core::SequenceId>(kWrites));
  EXPECT_EQ(c->Snapshot().last_ack_seq, static_cast<core::SequenceId>(kWrites));
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
  EXPECT_CALL(queue_, Read(_, _, _, _))
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

// --- COLDC-5: oldest_unflushed_age lag signal ---------------------------------

TEST_F(ColdConsumerTest, OldestUnflushedAgeIsZeroWhenBufferEmpty) {
  auto c = MakeConsumer();
  EXPECT_EQ(c->Snapshot().oldest_unflushed_age, 0ms);
}

TEST_F(ColdConsumerTest, OldestUnflushedAgePopulated) {
  auto c = MakeConsumer();

  EXPECT_CALL(queue_, Read(core::kColdConsumer, kShard, _, _))
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

  EXPECT_CALL(queue_, Read(core::kColdConsumer, kShard, _, _))
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
