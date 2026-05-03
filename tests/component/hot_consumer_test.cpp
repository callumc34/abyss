#include "abyss/consumer/hot_consumer.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/fsync_policy.h"
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
        .commit = {.policy = queue::FsyncPolicy::kGroupCommit,
                   .interval = std::chrono::microseconds{500},
                   .max_bytes = 1024UL * 1024UL},
        .min_retention = 10s,
        .retention_consumers = {core::kHotConsumer},
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
    consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_, apply_notifier_,
                                              HotConsumer::Config{
                                                  .shard = 0,
                                                  .read_batch_size = 32,
                                                  .read_timeout = core::Duration{10},
                                              },
                                              core::EvictionPolicy{eviction});
    consumer_->Start();
  }

  // Build (without starting) a consumer with a fixed wall-clock function and
  // configurable eviction. Used by ReplayUntil-driven skip-stale tests.
  void BuildConsumerWithClock(core::WallClockFn clock,
                              core::EvictionTTL eviction = core::EvictionTTL{86400},
                              core::ShardId shard = 0) {
    consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_, apply_notifier_,
                                              HotConsumer::Config{
                                                  .shard = shard,
                                                  .read_batch_size = 32,
                                                  .replay_batch_size = 32,
                                                  .read_timeout = core::Duration{10},
                                                  .wall_clock = std::move(clock),
                                              },
                                              core::EvictionPolicy{eviction});
  }

  core::QueueEntry MakeWrite(std::vector<std::string> args) {
    core::QueueEntry e;
    e.appended_at = core::WallClock::now();
    e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
    return e;
  }

  // BeginAppend → Register → Publish, matching the engine's write path.
  std::future<core::RespValue> AppendWithRpc(std::vector<std::string> args) {
    auto pending = queue_->BeginAppend(0, MakeWrite(std::move(args)));
    EXPECT_TRUE(pending.has_value());
    auto future = rpc_.Register(pending->seq());
    pending->Publish();
    EXPECT_TRUE(pending->durable().get().has_value());
    return future;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<testing::TempDir> dir_;
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  core::ConsumerRpc rpc_;
  core::ApplyNotifier apply_notifier_;
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
  EXPECT_EQ(snap.ack_failures, 0U);
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
  ASSERT_TRUE(consumer_->ReplayUntil(queue_->TailSeq(0).value(), cancel).has_value());

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
  ASSERT_TRUE(consumer_->ReplayUntil(queue_->TailSeq(0).value(), cancel).has_value());

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

TEST_F(HotConsumerTest, ResumesFromAckOffsetAcrossRestart) {
  StartConsumer();
  auto f1 = AppendWithRpc({"SET", "a", "1"});
  auto f2 = AppendWithRpc({"SET", "b", "2"});
  (void)f1.get();
  (void)f2.get();
  consumer_->Stop();

  // Simulate restart: blank hot store, same WAL (acks persisted). Prior
  // entries were acked so they must not replay.
  hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = 16UL * 1024UL * 1024UL,
      .shard_count = 1,
  });

  StartConsumer();
  auto f3 = AppendWithRpc({"SET", "c", "3"});
  EXPECT_EQ(f3.get().AsString(), "OK");

  auto r_a = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "a"}});
  EXPECT_FALSE(r_a.has_value());
  auto r_c = hot_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "c"}});
  ASSERT_TRUE(r_c.has_value());
  EXPECT_EQ(r_c->AsString(), "3");
}

}  // namespace
}  // namespace abyss::consumer
