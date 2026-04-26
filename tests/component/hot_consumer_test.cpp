#include "abyss/consumer/hot_consumer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

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
    consumer_ = std::make_unique<HotConsumer>(*queue_, *hot_, rpc_,
                                              HotConsumer::Config{
                                                  .shard = 0,
                                                  .read_batch_size = 32,
                                                  .read_timeout = core::Duration{10},
                                              },
                                              core::EvictionPolicy{eviction});
    consumer_->Start();
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
