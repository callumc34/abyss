#include "abyss/consumer/hot_consumer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/hot/eviction_worker.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;

class HotConsumerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::make_unique<testing::TempDir>("hot_consumer");

    hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = 16UL * 1024UL * 1024UL,
        .shard_count = 1,
    });

    auto queue_result = queue::WalQueue::Open(queue::WalConfig{
        .wal_path = dir_->String(),
        .segment_size_bytes = 4096,
        .shard_count = 1,
        .durability = core::Durability::kPowerLoss,
        .min_retention = 10s,
    });
    ASSERT_TRUE(queue_result.has_value()) << queue_result.error().message();
    queue_ = std::move(*queue_result);
  }

  void TearDown() override {
    if (consumer_) consumer_->Stop();
    queue_.reset();
    hot_.reset();
    dir_.reset();
  }

  // Start a consumer on the shared queue/store pointing at shard 0.
  void StartConsumer(core::EvictionTTL eviction = core::EvictionTTL{86400}) {
    policy_ = core::EvictionPolicy{eviction};
    consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_, apply_notifier_,
                                              HotConsumer::Config{
                                                  .shard = 0,
                                                  .read_batch_size = 32,
                                                  .read_timeout = core::Duration{10},
                                              },
                                              policy_);
    consumer_->Start();
  }

  // Build (without starting) a consumer with a fixed wall-clock function and
  // configurable eviction. Used by ReplayUntil-driven skip-stale tests.
  void BuildConsumerWithClock(core::WallClockFn clock,
                              core::EvictionTTL eviction = core::EvictionTTL{86400},
                              core::ShardId shard = 0) {
    policy_ = core::EvictionPolicy{eviction};
    consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_, apply_notifier_,
                                              HotConsumer::Config{
                                                  .shard = shard,
                                                  .read_batch_size = 32,
                                                  .replay_batch_size = 32,
                                                  .read_timeout = core::Duration{10},
                                                  .wall_clock = std::move(clock),
                                              },
                                              policy_);
  }

  // Recovery replays a log Open has already synced; under power_loss an
  // unflushed tail would read as empty and end the replay early.
  core::SequenceId DurableTail() {
    const core::SequenceId tail = queue_->TailSeq(0).value();
    EXPECT_TRUE(queue_->AwaitDurable(0, tail, core::Durability::kPowerLoss, 5s).value());
    return tail;
  }

  core::QueueEntry MakeWrite(std::vector<std::string> args) {
    core::QueueEntry e;
    e.appended_at = core::WallClock::now();
    e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
    return e;
  }

  // Publishes an arbitrary entry payload and returns its assigned seq.
  core::SequenceId AppendPayload(std::variant<core::entry::Write, core::entry::Conditional,
                                              core::entry::Resolved, core::entry::Flush>
                                     payload) {
    core::QueueEntry e;
    e.appended_at = core::WallClock::now();
    e.payload = std::move(payload);
    auto pending = queue_->BeginAppend(0, std::move(e));
    if (!pending.has_value()) {
      ADD_FAILURE() << pending.error().message();
      return 0;
    }
    const core::SequenceId seq = pending->seq();
    pending->Publish();
    EXPECT_TRUE(pending->durable().get().has_value());
    return seq;
  }

  // BeginAppend → Register → Publish, matching the engine's write path.
  std::future<core::RespValue> AppendWithRpc(std::vector<std::string> args) {
    auto pending = queue_->BeginAppend(0, MakeWrite(std::move(args)));
    if (!pending.has_value()) {
      ADD_FAILURE() << pending.error().message();
      return {};
    }
    auto future = rpc_.Register(pending->seq());
    pending->Publish();
    EXPECT_TRUE(pending->durable().get().has_value());
    return future;
  }

  // Same as AppendWithRpc but also returns the assigned sequence id. Used by
  // tests that need to assert against the seq the consumer will settle.
  struct AppendedRpc {
    core::SequenceId seq;
    std::future<core::RespValue> future;
  };
  AppendedRpc AppendWithRpcAndSeq(std::vector<std::string> args) {
    auto pending = queue_->BeginAppend(0, MakeWrite(std::move(args)));
    if (!pending.has_value()) {
      ADD_FAILURE() << pending.error().message();
      return {};
    }
    const core::SequenceId seq = pending->seq();
    auto future = rpc_.Register(seq);
    pending->Publish();
    EXPECT_TRUE(pending->durable().get().has_value());
    return {seq, std::move(future)};
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<testing::TempDir> dir_;
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  core::ConsumerRpc rpc_;
  core::ApplyNotifier apply_notifier_;
  // Outlives consumer_ — consumer holds a const ref into this slot.
  core::EvictionPolicy policy_{core::EvictionTTL{86400}};
  std::unique_ptr<HotConsumer> consumer_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(HotConsumerTest, AppliesWriteAndFulfillsOk) {
  StartConsumer();
  auto future = AppendWithRpc({"SET", "key", "value"});
  auto value = future.get();
  EXPECT_TRUE(value.IsSimpleString());
  EXPECT_EQ(value.AsString(), "OK");

  auto read = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "key"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "value");
}

// Under power_loss hot shows only flushed entries, so no reply can
// reflect a write a power loss could drop. The flush wakes the reader.
TEST_F(HotConsumerTest, PowerLossAppliesOnlyFlushedWrites) {
  struct Stall {
    std::mutex mu;
    std::condition_variable cv;
    bool released = false;
  };
  auto stall = std::make_shared<Stall>();
  queue_->SetFlushHookForTesting([stall](core::ShardId) -> core::Result<void> {
    std::unique_lock lock(stall->mu);
    stall->cv.wait(lock, [&stall] { return stall->released; });
    return {};
  });
  const auto release = [&stall] {
    {
      const std::scoped_lock lock(stall->mu);
      stall->released = true;
    }
    stall->cv.notify_all();
  };
  StartConsumer();

  auto pending = queue_->BeginAppend(0, MakeWrite({"SET", "key", "value"}));
  ASSERT_TRUE(pending.has_value());
  auto reply = rpc_.Register(pending->seq());
  pending->Publish();
  EXPECT_EQ(reply.wait_for(100ms), std::future_status::timeout);
  EXPECT_FALSE(hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "key"}}).has_value());

  release();
  ASSERT_EQ(reply.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(reply.get().AsString(), "OK");
  ASSERT_TRUE(pending->durable().get().has_value());
  auto read = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "key"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "value");
}

// The flush wakes the power_loss reader, so a lone SET acks one flush
// after publish, not at the reader's 100 ms timeout.
TEST_F(HotConsumerTest, PowerLossLoneSetAckTracksTheFlushNotTheReadTimeout) {
  policy_ = core::EvictionPolicy{core::EvictionTTL{86400}};
  consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_, apply_notifier_,
                                            HotConsumer::Config{.shard = 0}, policy_);
  ASSERT_EQ(HotConsumer::Config{}.read_timeout, core::Duration{100});
  consumer_->Start();

  constexpr int kSamples = 9;
  std::vector<std::chrono::steady_clock::duration> flushes;
  std::vector<std::chrono::steady_clock::duration> acks;
  for (int i = 0; i < kSamples; ++i) {
    const auto key = "k" + std::to_string(i);
    const auto flush_start = std::chrono::steady_clock::now();
    auto flushed = queue_->BeginAppend(0, MakeWrite({"SET", "calibrate", key}));
    ASSERT_TRUE(flushed.has_value());
    flushed->Publish();
    ASSERT_TRUE(flushed->durable().get().has_value());
    flushes.push_back(std::chrono::steady_clock::now() - flush_start);

    const auto ack_start = std::chrono::steady_clock::now();
    auto pending = queue_->BeginAppend(0, MakeWrite({"SET", key, "v"}));
    ASSERT_TRUE(pending.has_value());
    auto reply = rpc_.Register(pending->seq());
    pending->Publish();
    ASSERT_EQ(reply.wait_for(5s), std::future_status::ready);
    acks.push_back(std::chrono::steady_clock::now() - ack_start);
    EXPECT_EQ(reply.get().AsString(), "OK");
  }
  std::ranges::sort(flushes);
  std::ranges::sort(acks);
  const auto flush = flushes[kSamples / 2];
  const auto ack = acks[kSamples / 2];
  EXPECT_LT(ack, 3 * flush + 20ms)
      << "median ack " << std::chrono::duration_cast<std::chrono::microseconds>(ack).count()
      << "us vs median flush "
      << std::chrono::duration_cast<std::chrono::microseconds>(flush).count() << "us";
}

TEST_F(HotConsumerTest, AppliesEntriesInQueueOrder) {
  StartConsumer();
  auto f1 = AppendWithRpc({"SET", "k", "1"});
  auto f2 = AppendWithRpc({"SET", "k", "2"});
  auto f3 = AppendWithRpc({"SET", "k", "3"});

  EXPECT_EQ(f1.get().AsString(), "OK");
  EXPECT_EQ(f2.get().AsString(), "OK");
  EXPECT_EQ(f3.get().AsString(), "OK");

  auto read = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "3");
}

// The engine's WaitForBufferConsistency gate (ADP-006 §Read Path) reads
// HighestSettledSeq the instant a write's RPC future resolves; the hot consumer
// must therefore publish settled-seq BEFORE calling Fulfill. C++ guarantees
// every write a producer makes before std::promise::set_value is visible to the
// waiter after std::future::get, so this assertion is sound under the fixed
// ordering. A regression that swaps the lines back exposes a race window that
// preemption between Fulfill and MarkSettled can land inside — the loop is sized
// to surface it on contended runners.
TEST_F(HotConsumerTest, SettledSeqVisibleWhenWriteRpcResolves) {
  StartConsumer();
  for (uint64_t i = 1; i <= 64; ++i) {
    auto [seq, future] = AppendWithRpcAndSeq({"SET", "k" + std::to_string(i), "v"});
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready) << "iter=" << i;
    auto value = future.get();
    ASSERT_TRUE(value.IsSimpleString()) << "iter=" << i;
    EXPECT_GE(consumer_->HighestSettledSeq(), seq)
        << "iter=" << i << " seq=" << seq
        << " — settled-seq must be visible at the moment the RPC future resolves";
  }
}

// HOTC-7: HighestSettledSeq is the settled FLOOR, not a raw max. With a
// Conditional pending at seq M and a later Write applied at seq N>M, the floor
// must clamp to M-1 (never N), because M's outcome is still undecided. Once the
// matching Resolved for M applies, the floor advances past N.
TEST_F(HotConsumerTest, HighestSettledSeqClampedBehindPendingConditional) {
  StartConsumer();

  // Advance settled past seq 0 first so the pending Conditional below lands at a
  // seq M > 0 — the unsigned-seq-0 clamp guard cannot clamp below seq 0.
  auto warmup = AppendWithRpcAndSeq({"SET", "warm", "0"});
  ASSERT_EQ(warmup.future.wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(warmup.future.get().IsSimpleString());

  // A Conditional that the consumer holds pending (no Resolved emitted by hot;
  // the resolver would normally emit it).
  const core::SequenceId m = AppendPayload(core::entry::Conditional{
      .cmd = core::RespCommand{{"SETNX", "k", "v"}},
      .flags = core::PredicateFlags::kNx,
  });
  ASSERT_GT(m, 0U);

  // A later unconditional Write at seq N > M, fulfilled via its RPC so we know
  // it has been applied.
  auto [n, future] = AppendWithRpcAndSeq({"SET", "other", "1"});
  ASSERT_GT(n, m);
  ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(future.get().IsSimpleString());

  // Wait for the consumer to observe the pending Conditional, then assert the
  // floor never exceeds M-1 despite N being applied.
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (consumer_->PendingConditionalCount() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(2ms);
  }
  ASSERT_EQ(consumer_->PendingConditionalCount(), 1U);
  EXPECT_EQ(consumer_->HighestSettledSeq(), m - 1)
      << "floor must clamp behind the pending Conditional at " << m;

  // Now the matching Resolved for M applies (kSkip is fine — it just settles M).
  AppendPayload(core::entry::Resolved{
      .ref = m,
      .decision = core::Decision::kSkip,
      .materialised_ops = {},
      .return_value = core::RespValue::Integer(0),
  });

  const auto deadline2 = std::chrono::steady_clock::now() + 2s;
  while (consumer_->HighestSettledSeq() < n && std::chrono::steady_clock::now() < deadline2) {
    std::this_thread::sleep_for(2ms);
  }
  EXPECT_GE(consumer_->HighestSettledSeq(), n)
      << "floor must advance past N once the Conditional at M resolves";
}

TEST_F(HotConsumerTest, WrongTypeFlowsThroughRpcAndAcks) {
  StartConsumer();
  auto f1 = AppendWithRpc({"SET", "k", "v"});
  EXPECT_EQ(f1.get().AsString(), "OK");

  // SADD against a string key → WRONGTYPE from the hot store.
  auto f2 = AppendWithRpc({"SADD", "k", "m"});
  auto value = f2.get();
  EXPECT_TRUE(value.IsError());
  EXPECT_EQ(value.AsString(), "WRONGTYPE Operation against a key holding the wrong kind of value");

  // Subsequent entries must still apply — a poison entry mustn't wedge the
  // consumer.
  auto f3 = AppendWithRpc({"SET", "k2", "v2"});
  EXPECT_EQ(f3.get().AsString(), "OK");
}

TEST_F(HotConsumerTest, StopIsIdempotentAndSafeAfterWrites) {
  StartConsumer();
  auto f = AppendWithRpc({"SET", "k", "v"});
  EXPECT_EQ(f.get().AsString(), "OK");

  consumer_->Stop();
  EXPECT_FALSE(consumer_->Running());
  consumer_->Stop();  // idempotent
}

TEST_F(HotConsumerTest, MetricsAdvanceOnApply) {
  StartConsumer();
  auto f_ok = AppendWithRpc({"SET", "k", "v"});
  EXPECT_EQ(f_ok.get().AsString(), "OK");
  auto f_wrong = AppendWithRpc({"SADD", "k", "m"});
  EXPECT_TRUE(f_wrong.get().IsError());

  const auto snap = consumer_->Snapshot();
  EXPECT_EQ(snap.applied, 1U);
  EXPECT_EQ(snap.apply_failures, 1U);
  EXPECT_EQ(snap.parse_failures, 0U);
  EXPECT_EQ(snap.queue_read_failures, 0U);
  EXPECT_EQ(snap.commit_failures, 0U);
}

TEST_F(HotConsumerTest, ConcurrentWritersAlwaysSeeRegisteredEntries) {
  StartConsumer();

  constexpr int kWriters = 8;
  constexpr int kWritesPerWriter = 50;
  std::atomic<int> broken_futures{0};

  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w]() {
      for (int i = 0; i < kWritesPerWriter; ++i) {
        auto future = AppendWithRpc(
            {"SET", "w" + std::to_string(w) + "_" + std::to_string(i), std::to_string(i)});
        // Bound the wait: an orphaned promise from a Fulfill-before-Register
        // race would block forever otherwise.
        if (future.wait_for(5s) != std::future_status::ready) {
          broken_futures.fetch_add(1, std::memory_order_relaxed);
        } else {
          EXPECT_TRUE(future.get().IsSimpleString());
        }
      }
    });
  }
  for (auto& t : writers) t.join();

  EXPECT_EQ(broken_futures.load(), 0);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(HotConsumerTest, ReplayUntilSkipsEntriesPastEvictionWindow) {
  // Append two entries via direct queue Append (no client-side RPC). One
  // entry's appended_at sits 25h in the past (> default 24h eviction); the
  // second is fresh. Replay must skip the stale one but apply the fresh one.
  const auto stale_at = core::WallClock::now() - std::chrono::hours{25};
  const auto fresh_at = core::WallClock::now();

  auto stale_entry = MakeWrite({"SET", "stale", "v"});
  stale_entry.appended_at = stale_at;
  ASSERT_TRUE(queue_->Append(0, stale_entry).has_value());

  auto fresh_entry = MakeWrite({"SET", "fresh", "v"});
  fresh_entry.appended_at = fresh_at;
  ASSERT_TRUE(queue_->Append(0, fresh_entry).has_value());

  BuildConsumerWithClock([] { return core::WallClock::now(); });
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(consumer_->ReplayUntil(DurableTail(), cancel).has_value());

  EXPECT_FALSE(hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "stale"}}).has_value());
  auto fresh = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "fresh"}});
  ASSERT_TRUE(fresh.has_value());
  EXPECT_EQ(fresh->AsString(), "v");

  EXPECT_EQ(consumer_->Snapshot().replay_skipped_eviction, 1U);
  EXPECT_EQ(consumer_->Snapshot().applied, 1U);
}

TEST_F(HotConsumerTest, ReplayUntilSkipsEntriesWithExpiredAbsoluteTtl) {
  // PXAT carries an absolute Unix-ms deadline that the parser preserves
  // verbatim (unlike EX/PX which currently anchor to parse time). A deadline
  // already in the past at replay time should fire the abs-TTL skip even
  // when the eviction window from appended_at has not elapsed.
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          core::WallClock::now().time_since_epoch())
                          .count();
  const auto past_ms = now_ms - 5000;
  auto entry = MakeWrite({"SET", "k", "v", "PXAT", std::to_string(past_ms)});
  ASSERT_TRUE(queue_->Append(0, entry).has_value());

  BuildConsumerWithClock([] { return core::WallClock::now(); });
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(consumer_->ReplayUntil(DurableTail(), cancel).has_value());

  EXPECT_FALSE(hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).has_value());
  EXPECT_EQ(consumer_->Snapshot().replay_skipped_abs_ttl, 1U);
  EXPECT_EQ(consumer_->Snapshot().applied, 0U);
}

TEST_F(HotConsumerTest, SteadyStateAppliesEvenWhenAppendedAtIsAncient) {
  // The skip-stale checks gate on replay_mode_, not on entry age alone.
  // Steady-state Run() must apply an entry regardless of its appended_at —
  // that's a writer's right-now intent (typically only seconds old in
  // practice but not in tests that backdate).
  StartConsumer();

  auto entry = MakeWrite({"SET", "k", "v"});
  entry.appended_at = core::WallClock::now() - std::chrono::hours{48};
  auto pending = queue_->BeginAppend(0, entry);
  ASSERT_TRUE(pending.has_value());
  auto fut = rpc_.Register(pending->seq());
  pending->Publish();
  ASSERT_TRUE(pending->durable().get().has_value());

  EXPECT_EQ(fut.get().AsString(), "OK");
  auto read = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}});
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "v");
  EXPECT_EQ(consumer_->Snapshot().replay_skipped_eviction, 0U);
}

// Hot commits no offset: a fresh consumer over a blank store rebuilds the
// whole retained log, not just what arrives after it starts.
TEST_F(HotConsumerTest, NewConsumerRebuildsFromRetainedLog) {
  StartConsumer();
  auto f1 = AppendWithRpc({"SET", "a", "1"});
  auto f2 = AppendWithRpc({"SET", "b", "2"});
  (void)f1.get();
  (void)f2.get();
  consumer_->Stop();

  hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = 16UL * 1024UL * 1024UL,
      .shard_count = 1,
  });

  StartConsumer();
  auto f3 = AppendWithRpc({"SET", "c", "3"});
  EXPECT_EQ(f3.get().AsString(), "OK");

  auto r_a = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "a"}});
  ASSERT_TRUE(r_a.has_value());
  EXPECT_EQ(r_a->AsString(), "1");
  auto r_c = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "c"}});
  ASSERT_TRUE(r_c.has_value());
  EXPECT_EQ(r_c->AsString(), "3");
}

// Run continues from the cursor ReplayUntil left, never re-applying.
TEST_F(HotConsumerTest, RunResumesWhereReplayStopped) {
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "r" + std::to_string(i), "v"})).has_value());
  }
  BuildConsumerWithClock([] { return core::WallClock::now(); });
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(consumer_->ReplayUntil(DurableTail(), cancel).has_value());
  ASSERT_EQ(consumer_->Snapshot().applied, 3U);

  consumer_->Start();
  auto f = AppendWithRpc({"SET", "after", "v"});
  ASSERT_EQ(f.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(f.get().AsString(), "OK");
  EXPECT_EQ(consumer_->Snapshot().applied, 4U) << "Run re-applied entries replay already applied";
}

// A1: with a Conditional pending at X, more than read_batch_size writes
// land before its Resolved. When the read position followed the clamped
// commit, every Read re-delivered from X: writes were re-applied and, past
// one batch, the Resolved was never reached (a shard write outage).
TEST_F(HotConsumerTest, PendingConditionalNeverRedeliversLaterWrites) {
  policy_ = core::EvictionPolicy{core::EvictionTTL{86400}};
  consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_, apply_notifier_,
                                            HotConsumer::Config{
                                                .shard = 0,
                                                .read_batch_size = 256,
                                                .read_timeout = core::Duration{10},
                                            },
                                            policy_);
  consumer_->Start();

  auto warmup = AppendWithRpc({"SET", "warm", "0"});
  ASSERT_EQ(warmup.wait_for(5s), std::future_status::ready);
  const core::SequenceId x = AppendPayload(core::entry::Conditional{
      .cmd = core::RespCommand{{"SETNX", "cond", "v"}},
      .flags = core::PredicateFlags::kNx,
  });
  ASSERT_GT(x, 0U);

  // Batches keep each append inside one 4 KiB segment.
  constexpr int kWrites = 300;
  constexpr int kPerBatch = 50;
  for (int b = 0; b < kWrites / kPerBatch; ++b) {
    std::vector<core::QueueEntry> batch;
    batch.reserve(kPerBatch);
    for (int i = 0; i < kPerBatch; ++i) {
      batch.push_back(MakeWrite({"SET", "w" + std::to_string((b * kPerBatch) + i), "v"}));
    }
    auto appended = queue_->AppendBatch(0, batch);
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
    ASSERT_TRUE(appended->durable.get().has_value());
  }
  AppendPayload(core::entry::Resolved{
      .ref = x,
      .decision = core::Decision::kSkip,
      .materialised_ops = {},
      .return_value = core::RespValue::Integer(0),
  });

  const core::SequenceId tail = queue_->TailSeq(0).value();
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while ((consumer_->PendingConditionalCount() != 0 || consumer_->HighestSettledSeq() < tail) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(2ms);
  }

  EXPECT_EQ(consumer_->PendingConditionalCount(), 0U) << "the Resolved was never reached";
  EXPECT_EQ(consumer_->HighestSettledSeq(), tail);
  EXPECT_EQ(consumer_->Snapshot().applied, static_cast<uint64_t>(kWrites) + 1)
      << "a write behind the pending Conditional was applied more than once";
}

// Hot is a volatile view: once the reaper reclaimed the head of the log, a
// fresh hot consumer rebuilds from the first retained seq, not failing.
TEST_F(HotConsumerTest, RebuildsFromFirstRetainedSeqAfterReclaim) {
  queue_.reset();
  const testing::TempDir dir("hot_consumer_reaped");
  auto opened = queue::WalQueue::Open(queue::WalConfig{
      .wal_path = dir.String(),
      .segment_size_bytes = 256,
      .shard_count = 1,
      .durability = core::Durability::kPowerLoss,
      .min_retention = 0s,
      .retention_consumers = {core::kColdConsumer},
  });
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  queue_ = std::move(*opened);

  for (int i = 0; i < 30; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k" + std::to_string(i), "v"}));
    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(r->durable.get().has_value());
  }
  const core::SequenceId tail = queue_->TailSeq(0).value();
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, tail).has_value());
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  const core::SequenceId first = queue_->FirstSeq(0).value();
  ASSERT_GT(first, 0U) << "the reaper reclaimed nothing";

  BuildConsumerWithClock([] { return core::WallClock::now(); });
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(consumer_->ReplayUntil(tail, cancel).has_value());
  EXPECT_EQ(consumer_->Snapshot().applied, tail - first + 1);
  consumer_.reset();
  queue_.reset();
}

// --- Memory-pressure during/after replay (HOT-1) ---

TEST_F(HotConsumerTest, MemoryPressureSuppressedDuringReplay) {
  // Size a budget far below the replayed working set. During replay every
  // entry must apply (no kResourceExhausted, no memory apply_failures) so the
  // rebuilt state matches the pre-crash state; a subsequent eviction-worker
  // tick reconverges used_bytes <= budget (deterministic replay, invariant 4).
  constexpr int kEntries = 20;
  const std::string value(256, 'v');

  // Probe one entry's footprint to size the per-shard budget below the set.
  hot::ShardedHotStore probe{hot::ShardedHotStoreConfig{.max_memory_bytes = 0, .shard_count = 1}};
  ASSERT_TRUE(probe.Apply(core::ops::WriteOp{core::ops::StringSet{.key = "p", .value = value}}, 0)
                  .has_value());
  const uint64_t per_entry = probe.Stats()->used_bytes;

  hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = per_entry * 4,  // holds ~4, replay writes 20
      .shard_count = 1,
  });

  for (int i = 0; i < kEntries; ++i) {
    auto entry = MakeWrite({"SET", "k" + std::to_string(i), value});
    ASSERT_TRUE(queue_->Append(0, entry).has_value());
  }

  BuildConsumerWithClock([] { return core::WallClock::now(); });
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(consumer_->ReplayUntil(DurableTail(), cancel).has_value());

  // All entries applied; none rejected for memory.
  EXPECT_EQ(consumer_->Snapshot().applied, static_cast<uint64_t>(kEntries));
  EXPECT_EQ(consumer_->Snapshot().apply_failures, 0U);
  EXPECT_EQ(hot_->Stats()->key_count, static_cast<uint64_t>(kEntries));
  EXPECT_GT(hot_->Stats()->used_bytes, hot_->Stats()->max_bytes)
      << "over budget during replay (suppressed); enforced only afterward";

  // After replay, the eviction-worker tick reconverges the ceiling.
  hot::EvictionWorker worker(*hot_, hot::EvictionWorker::Config{.tick = 50ms});
  worker.TickOnce();
  EXPECT_LE(hot_->Stats()->used_bytes, hot_->Stats()->max_bytes);
}

TEST_F(HotConsumerTest, OverBudgetWriteSurfacesOomButStaysDurable) {
  // A live steady-state write that cannot be admitted yields -OOM to the client
  // (MapApplyError path) while the queue entry remains durable and the seq is
  // settled/NotifyApplied — the consumer does not wedge (invariant 1/2).
  hot::ShardedHotStore probe{hot::ShardedHotStoreConfig{.max_memory_bytes = 0, .shard_count = 1}};
  ASSERT_TRUE(probe.Apply(core::ops::WriteOp{core::ops::StringSet{.key = "small", .value = "v"}}, 0)
                  .has_value());
  const uint64_t small_entry = probe.Stats()->used_bytes;

  hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      // Holds a handful of small entries, but a single 1500-char value is far
      // larger than the whole budget — so "big" can never be admitted.
      .max_memory_bytes = small_entry * 4,
      .shard_count = 1,
  });

  StartConsumer();
  // Large enough to dwarf the budget but within the WAL segment size.
  auto fut = AppendWithRpc({"SET", "big", std::string(1500, 'x')});
  auto reply = fut.get();
  // The -OOM reply itself proves the entry was durably queued and applied (the
  // consumer read it from the WAL and produced an apply result); the write is
  // not lost — it stays in the queue/cold (invariant 2).
  ASSERT_TRUE(reply.IsError());
  EXPECT_EQ(reply.ErrorPrefixOf(), core::ErrorPrefix::kOom);

  // The seq still settled (no wedge); a follow-up write is fulfilled normally.
  auto ok = AppendWithRpc({"SET", "small", "v"});
  EXPECT_FALSE(ok.get().IsError());
  EXPECT_EQ(consumer_->PendingConditionalCount(), 0U);
}

}  // namespace
}  // namespace abyss::consumer
