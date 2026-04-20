#include "abyss/queue/wal_queue.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/group_commit.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

core::QueueEntry MakeWrite(std::vector<std::string> args) {
  core::QueueEntry e;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return e;
}

class WalQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_wal_XXXXXX";
    std::string s = tmpl.string();
    ASSERT_NE(::mkdtemp(s.data()), nullptr);
    tmp_dir_ = s;
  }

  void TearDown() override {
    queue_.reset();
    if (!tmp_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_dir_, ec);
    }
  }

  WalConfig DefaultConfig() const {
    return WalConfig{
        .wal_path = tmp_dir_,
        .segment_size_bytes = 4096,
        .shard_count = 2,
        .commit = {.policy = FsyncPolicy::kGroupCommit,
                   .interval = std::chrono::microseconds{1000},
                   .max_bytes = size_t{1024} * 1024},
        .min_retention = 1s,
        .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
    };
  }

  void OpenWith(WalConfig config) {
    auto result = WalQueue::Open(std::move(config));
    ASSERT_TRUE(result.has_value()) << result.error().message();
    queue_ = std::move(*result);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::string tmp_dir_;
  std::unique_ptr<WalQueue> queue_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(WalQueueTest, AppendAssignsMonotonicSeqs) {
  OpenWith(DefaultConfig());

  auto r1 = queue_->Append(0, MakeWrite({"SET", "a", "1"}));
  auto r2 = queue_->Append(0, MakeWrite({"SET", "b", "2"}));
  ASSERT_TRUE(r1.has_value());
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r1->seq, 0U);
  EXPECT_EQ(r2->seq, 1U);
}

TEST_F(WalQueueTest, DurabilityFutureResolvesOk) {
  OpenWith(DefaultConfig());
  auto r = queue_->Append(0, MakeWrite({"SET", "a", "1"}));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->durable.get().has_value());
}

TEST_F(WalQueueTest, FsyncNonePolicyResolvesImmediately) {
  auto cfg = DefaultConfig();
  cfg.commit.policy = FsyncPolicy::kNone;
  OpenWith(cfg);

  auto r = queue_->Append(0, MakeWrite({"SET", "a", "1"}));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->durable.wait_for(0ms), std::future_status::ready);
}

TEST_F(WalQueueTest, FsyncPerWritePolicyAppendsCleanly) {
  auto cfg = DefaultConfig();
  cfg.commit.policy = FsyncPolicy::kPerWrite;
  OpenWith(cfg);

  for (int i = 0; i < 5; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
  }
}

TEST_F(WalQueueTest, ShardsAreIndependent) {
  OpenWith(DefaultConfig());

  auto a = queue_->Append(0, MakeWrite({"SET", "a", "0"}));
  auto b = queue_->Append(1, MakeWrite({"SET", "b", "0"}));
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(a->seq, 0U);
  EXPECT_EQ(b->seq, 0U);
}

TEST_F(WalQueueTest, InvalidShardRejected) {
  OpenWith(DefaultConfig());
  auto result = queue_->Append(99, MakeWrite({"SET", "a", "0"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(WalQueueTest, ReadReturnsAppendedEntries) {
  OpenWith(DefaultConfig());

  for (int i = 0; i < 5; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
  }

  auto read = queue_->Read(core::kHotConsumer, 0, 100, 100ms);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 5U);
  for (size_t i = 0; i < read->size(); ++i) {
    EXPECT_EQ((*read)[i].seq, i);
  }
}

TEST_F(WalQueueTest, ReadBlocksOnTimeout) {
  OpenWith(DefaultConfig());
  auto start = std::chrono::steady_clock::now();
  auto read = queue_->Read(core::kHotConsumer, 0, 10, 50ms);
  auto elapsed = std::chrono::steady_clock::now() - start;
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->empty());
  EXPECT_GE(elapsed, 40ms);
}

TEST_F(WalQueueTest, ReadReturnsFastWhenAppendRacesWithRead) {
  OpenWith(DefaultConfig());

  std::thread producer([this] {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
    ASSERT_TRUE(r.has_value());
  });

  auto start = std::chrono::steady_clock::now();
  auto read = queue_->Read(core::kHotConsumer, 0, 10, 5s);
  auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  EXPECT_LT(elapsed, 1s);
  producer.join();
}

TEST_F(WalQueueTest, ReadAdvancesByAck) {
  OpenWith(DefaultConfig());

  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)})).has_value());
  }

  auto read1 = queue_->Read(core::kHotConsumer, 0, 10, 100ms);
  ASSERT_TRUE(read1.has_value());
  ASSERT_EQ(read1->size(), 4U);

  ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 1).has_value());

  auto read2 = queue_->Read(core::kHotConsumer, 0, 10, 100ms);
  ASSERT_TRUE(read2.has_value());
  ASSERT_EQ(read2->size(), 2U);
  EXPECT_EQ((*read2)[0].seq, 2U);
  EXPECT_EQ((*read2)[1].seq, 3U);
}

TEST_F(WalQueueTest, AppendBatchAtomic) {
  OpenWith(DefaultConfig());

  std::vector<core::QueueEntry> batch;
  batch.reserve(5);
  for (int i = 0; i < 5; ++i) {
    batch.push_back(MakeWrite({"SET", "k", std::to_string(i)}));
  }

  auto r = queue_->AppendBatch(0, batch);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->first_seq, 0U);
  EXPECT_EQ(r->last_seq, 4U);
  EXPECT_TRUE(r->durable.get().has_value());

  auto read = queue_->Read(core::kHotConsumer, 0, 100, 100ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->size(), 5U);
}

TEST_F(WalQueueTest, SegmentRotationPreservesOrder) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  OpenWith(cfg);

  for (int i = 0; i < 10; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(20, 'x')}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
  }

  auto read = queue_->Read(core::kHotConsumer, 0, 100, 100ms);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 10U);
  for (size_t i = 0; i < read->size(); ++i) {
    EXPECT_EQ((*read)[i].seq, i);
  }
}

TEST_F(WalQueueTest, BackToBackRotationsPreserveDurability) {
  // Each append forces a rotation because the segment capacity holds barely
  // one entry. The committer must be torn down and rebuilt per rotation
  // without losing any durability future.
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 140;
  OpenWith(cfg);

  constexpr int kWrites = 32;
  std::vector<queue::DurabilityFuture> futures;
  futures.reserve(kWrites);
  for (int i = 0; i < kWrites; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(20, 'x')}));
    ASSERT_TRUE(r.has_value());
    futures.push_back(std::move(r->durable));
  }
  for (auto& f : futures) {
    EXPECT_TRUE(f.get().has_value());
  }

  auto read = queue_->Read(core::kHotConsumer, 0, 1000, 100ms);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), static_cast<size_t>(kWrites));
  for (size_t i = 0; i < read->size(); ++i) {
    EXPECT_EQ((*read)[i].seq, i);
  }
}

TEST_F(WalQueueTest, ConcurrentAppendsAllDurable) {
  OpenWith(DefaultConfig());

  constexpr int kThreads = 4;
  constexpr int kPerThread = 50;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([this, t, &failures] {
      for (int i = 0; i < kPerThread; ++i) {
        auto r = queue_->Append(0, MakeWrite({"SET", "t" + std::to_string(t), std::to_string(i)}));
        if (!r.has_value() || !r->durable.get().has_value()) {
          failures.fetch_add(1);
        }
      }
    });
  }
  for (auto& th : threads) th.join();

  EXPECT_EQ(failures.load(), 0);

  auto read = queue_->Read(core::kHotConsumer, 0, 10000, 100ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->size(), kThreads * kPerThread);

  std::vector<core::SequenceId> seqs;
  seqs.reserve(read->size());
  for (auto& e : *read) seqs.push_back(e.seq);
  for (size_t i = 0; i < seqs.size(); ++i) {
    EXPECT_EQ(seqs[i], i);
  }
}

TEST_F(WalQueueTest, RecoveryPreservesEntries) {
  {
    OpenWith(DefaultConfig());
    for (int i = 0; i < 3; ++i) {
      auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
      ASSERT_TRUE(r.has_value());
      EXPECT_TRUE(r->durable.get().has_value());
    }
    queue_.reset();
  }

  OpenWith(DefaultConfig());
  auto read = queue_->Read(core::kHotConsumer, 0, 100, 100ms);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 3U);
}

TEST_F(WalQueueTest, RecoveryPreservesOffsets) {
  {
    OpenWith(DefaultConfig());
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
    ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 0).has_value());
    queue_.reset();
  }

  OpenWith(DefaultConfig());
  auto read = queue_->Read(core::kHotConsumer, 0, 100, 100ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->empty());
}

TEST_F(WalQueueTest, MissingMiddleSegmentRejectedAsCorruption) {
  // ADP-009 invariant 8: base_seq[i+1] == last_seq[i] + 1. Deleting a middle
  // segment must surface as corruption on Open rather than silently producing
  // a gap in the sequence space that consumers would read across.
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;

  {
    OpenWith(cfg);
    for (int i = 0; i < 10; ++i) {
      auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(20, 'x')}));
      ASSERT_TRUE(r.has_value());
      EXPECT_TRUE(r->durable.get().has_value());
    }
    queue_.reset();
  }

  // Find and delete a middle (non-first, non-last) segment file.
  const auto shard_dir = std::filesystem::path(tmp_dir_) / "shard-0000";
  std::vector<std::filesystem::path> seg_paths;
  for (const auto& entry : std::filesystem::directory_iterator(shard_dir)) {
    if (entry.path().extension() == ".log") seg_paths.push_back(entry.path());
  }
  std::ranges::sort(seg_paths);
  ASSERT_GE(seg_paths.size(), 3U);
  // NOLINTNEXTLINE(modernize-avoid-c-arrays)
  std::filesystem::remove(seg_paths[seg_paths.size() / 2]);

  auto result = WalQueue::Open(cfg);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kCorruption);
}

TEST_F(WalQueueTest, RecoveryAcrossRotation) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;

  {
    OpenWith(cfg);
    for (int i = 0; i < 10; ++i) {
      auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(20, 'x')}));
      ASSERT_TRUE(r.has_value());
      EXPECT_TRUE(r->durable.get().has_value());
    }
    queue_.reset();
  }

  OpenWith(cfg);
  auto read = queue_->Read(core::kHotConsumer, 0, 100, 100ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->size(), 10U);
  for (size_t i = 0; i < read->size(); ++i) {
    EXPECT_EQ((*read)[i].seq, i);
  }
}

TEST_F(WalQueueTest, StatsReflectsState) {
  OpenWith(DefaultConfig());
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)})).has_value());
    ASSERT_TRUE(queue_->Append(1, MakeWrite({"SET", "k", std::to_string(i)})).has_value());
  }

  auto stats = queue_->Stats();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->total_entries, 10U);
  EXPECT_GT(stats->total_bytes, 0U);
  EXPECT_EQ(stats->head_seq, 5U);
}

TEST_F(WalQueueTest, OldestRetainedTracksAcks) {
  OpenWith(DefaultConfig());
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)})).has_value());
  }

  ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 1).has_value());
  ASSERT_TRUE(queue_->Ack(core::kColdConsumer, 0, 0).has_value());

  auto oldest = queue_->OldestRetained(0);
  ASSERT_TRUE(oldest.has_value());
  EXPECT_EQ(*oldest, 0U);  // cold is behind, so min is 0
}

TEST_F(WalQueueTest, AppendTooLargeRejected) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 128;
  OpenWith(cfg);

  auto r = queue_->Append(0, MakeWrite({"SET", "k", std::string(10000, 'x')}));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(WalQueueTest, IsRecoveringFalseAfterOpen) {
  OpenWith(DefaultConfig());
  EXPECT_FALSE(queue_->IsRecovering());
}

TEST_F(WalQueueTest, ConcurrentProducersAndConsumers) {
  OpenWith(DefaultConfig());

  constexpr int kProducers = 4;
  constexpr int kPerProducer = 200;
  std::atomic<int> produced{0};
  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int t = 0; t < kProducers; ++t) {
    producers.emplace_back([this, &produced] {
      for (int i = 0; i < kPerProducer; ++i) {
        auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
        if (r.has_value() && r->durable.get().has_value()) {
          produced.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  std::atomic<bool> stop{false};
  std::vector<core::SequenceId> observed_seqs;
  std::thread consumer([this, &stop, &observed_seqs] {
    core::SequenceId ack = 0;
    bool first = true;
    while (!stop.load(std::memory_order_acquire)) {
      auto read = queue_->Read(core::kHotConsumer, 0, 128, 50ms);
      if (!read.has_value() || read->empty()) continue;
      for (const auto& e : *read) {
        observed_seqs.push_back(e.seq);
      }
      ack = read->back().seq;
      if (first || ack > 0) {
        ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, ack).has_value());
        first = false;
      }
    }
    for (;;) {
      auto read = queue_->Read(core::kHotConsumer, 0, 128, 10ms);
      if (!read.has_value() || read->empty()) break;
      for (const auto& e : *read) {
        observed_seqs.push_back(e.seq);
      }
      ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, read->back().seq).has_value());
    }
  });

  for (auto& t : producers) t.join();
  stop.store(true, std::memory_order_release);
  consumer.join();

  EXPECT_EQ(produced.load(), kProducers * kPerProducer);
  EXPECT_EQ(observed_seqs.size(), static_cast<size_t>(kProducers * kPerProducer));
  for (size_t i = 0; i < observed_seqs.size(); ++i) {
    EXPECT_EQ(observed_seqs[i], i);
  }
}

TEST_F(WalQueueTest, LaggingConsumerKeepsOldSegmentsAlive) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;  // retention window doesn't interfere.
  OpenWith(cfg);

  for (int i = 0; i < 20; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(20, 'x')}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
  }

  // Hot caught up; cold is far behind. Segments must stay because cold hasn't
  // acked past them.
  ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->Ack(core::kColdConsumer, 0, 1).has_value());

  auto sealed_before = queue_->ListSealedSegments();
  ASSERT_FALSE(sealed_before.empty());

  // Hot acks again — nothing new should get reclaimed because cold lags.
  ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 19).has_value());
  auto sealed_after = queue_->ListSealedSegments();
  EXPECT_EQ(sealed_after.size(), sealed_before.size());
}

TEST_F(WalQueueTest, AckAcrossRotationReclaimsOldSegment) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;
  OpenWith(cfg);

  for (int i = 0; i < 20; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(20, 'x')}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
  }

  auto sealed_before = queue_->ListSealedSegments();
  ASSERT_GT(sealed_before.size(), 0U);

  // Both consumers catch up past the sealed segments.
  ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->Ack(core::kColdConsumer, 0, 19).has_value());

  auto sealed_after = queue_->ListSealedSegments();
  EXPECT_EQ(sealed_after.size(), 0U);
}

TEST_F(WalQueueTest, ActiveSegmentNeverDeleted) {
  auto cfg = DefaultConfig();
  cfg.min_retention = 0s;
  OpenWith(cfg);

  ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "a", "1"})).has_value());
  ASSERT_TRUE(queue_->Ack(core::kHotConsumer, 0, 0).has_value());
  ASSERT_TRUE(queue_->Ack(core::kColdConsumer, 0, 0).has_value());

  // The active segment (which contains seq 0) must not be in the sealed list
  // and must not be removed.
  auto sealed = queue_->ListSealedSegments();
  EXPECT_TRUE(sealed.empty());

  // And reads still work for ack'd-but-not-read consumers.
  auto read = queue_->Read(core::kHotConsumer, 0, 10, 10ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->empty());  // all ack'd already.
}

TEST_F(WalQueueTest, BeginAppendPublishMakesEntryVisible) {
  OpenWith(DefaultConfig());

  auto pending = queue_->BeginAppend(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(pending.has_value());
  EXPECT_EQ(pending->seq(), 0U);

  pending->Publish();
  ASSERT_TRUE(pending->durable().get().has_value());

  auto read = queue_->Read(core::kHotConsumer, 0, 10, 100ms);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  EXPECT_EQ(read->front().seq, 0U);
}

// Auto-publish on drop: forgetting Publish() is a latency bug, not data loss.
TEST_F(WalQueueTest, DroppingPendingAppendAutoPublishes) {
  OpenWith(DefaultConfig());

  DurabilityFuture durable_future;
  core::SequenceId seq = 0;
  {
    auto pending = queue_->BeginAppend(0, MakeWrite({"SET", "k", "v"}));
    ASSERT_TRUE(pending.has_value());
    seq = pending->seq();
    durable_future = std::move(pending->durable());
    // No Publish() — destructor runs as the scope ends.
  }

  ASSERT_TRUE(durable_future.get().has_value());
  auto read = queue_->Read(core::kHotConsumer, 0, 10, 100ms);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  EXPECT_EQ(read->front().seq, seq);
}

TEST_F(WalQueueTest, BeginAppendBatchPublishMakesEntriesVisible) {
  OpenWith(DefaultConfig());

  std::vector<core::QueueEntry> batch;
  batch.reserve(3);
  for (int i = 0; i < 3; ++i) {
    batch.push_back(MakeWrite({"SET", "k", std::to_string(i)}));
  }

  auto pending = queue_->BeginAppendBatch(0, batch);
  ASSERT_TRUE(pending.has_value());
  EXPECT_EQ(pending->first_seq(), 0U);
  EXPECT_EQ(pending->last_seq(), 2U);
  EXPECT_EQ(pending->size(), 3U);
  EXPECT_EQ(pending->seq_at(1), 1U);

  pending->Publish();
  ASSERT_TRUE(pending->durable().get().has_value());

  auto read = queue_->Read(core::kHotConsumer, 0, 10, 100ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->size(), 3U);
}

// Hangs or fails if Publish ever became visible to readers before Register.
TEST_F(WalQueueTest, TwoPhaseWritePathEliminatesFulfillBeforeRegisterRace) {
  OpenWith(DefaultConfig());
  core::ConsumerRpc rpc;

  constexpr int kWrites = 500;
  std::atomic<bool> stop_consumer{false};

  std::thread consumer([&]() {
    core::SequenceId next = 0;
    auto drain = [&]() -> bool {
      auto read = queue_->Read(core::kHotConsumer, 0, 32, 10ms);
      if (!read.has_value() || read->empty()) return false;
      for (auto& e : *read) {
        EXPECT_EQ(e.seq, next++);
        rpc.Fulfill(e.seq, core::RespValue::SimpleString("OK"));
        [[maybe_unused]] auto ack = queue_->Ack(core::kHotConsumer, 0, e.seq);
      }
      return true;
    };
    while (!stop_consumer.load(std::memory_order_acquire)) (void)drain();
    while (drain()) {
    }
  });

  for (int i = 0; i < kWrites; ++i) {
    auto pending = queue_->BeginAppend(0, MakeWrite({"SET", "k", std::to_string(i)}));
    ASSERT_TRUE(pending.has_value());
    auto future = rpc.Register(pending->seq());
    pending->Publish();

    ASSERT_TRUE(pending->durable().get().has_value());
    auto value = future.get();
    EXPECT_TRUE(value.IsSimpleString());
  }

  stop_consumer.store(true, std::memory_order_release);
  consumer.join();
  EXPECT_EQ(rpc.PendingCount(), 0U);
}

TEST_F(WalQueueTest, AppendBatchCrashMidBatchLosesWholeBatch) {
  auto cfg = DefaultConfig();
  // `kNone` means pwritten bytes are in the page cache but not fsynced.
  // Dropping the queue without shutting down cleanly mimics a crash where
  // the batch-closing entry failed to land.
  cfg.commit.policy = FsyncPolicy::kNone;
  OpenWith(cfg);

  std::vector<core::QueueEntry> batch;
  batch.reserve(5);
  for (int i = 0; i < 5; ++i) {
    batch.push_back(MakeWrite({"SET", "k", std::to_string(i)}));
  }
  auto r = queue_->AppendBatch(0, batch);
  ASSERT_TRUE(r.has_value());

  // Truncate the segment mid-batch before reopening. This models the crash:
  // some of the batch's bytes hit disk, the closing entry did not.
  queue_.reset();
  const auto shard_dir = std::filesystem::path(tmp_dir_) / "shard-0000";
  std::string segment_path;
  for (const auto& entry : std::filesystem::directory_iterator(shard_dir)) {
    if (entry.path().extension() == ".log") {
      segment_path = entry.path().string();
      break;
    }
  }
  ASSERT_FALSE(segment_path.empty());

  struct stat st{};
  ASSERT_EQ(::stat(segment_path.c_str(), &st), 0);
  // Cut the file somewhere inside the mid-batch entries so the closing
  // entry at seq 4 cannot be recovered.
  ASSERT_EQ(::truncate(segment_path.c_str(), st.st_size / 3), 0);

  OpenWith(DefaultConfig());
  auto read = queue_->Read(core::kHotConsumer, 0, 100, 50ms);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->empty());
}

}  // namespace
}  // namespace abyss::queue
