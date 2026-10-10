#include "abyss/queue/wal_queue.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/log/testing.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/offset_checkpoint.h"
#include "abyss/queue/segment_header.h"
#include "temp_dir.h"

#ifdef _WIN32
#include "abyss/platform/fs.h"
#endif

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

core::QueueEntry MakeWrite(std::vector<std::string> args) {
  core::QueueEntry e;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return e;
}

std::string ValueOf(const core::QueueEntry& entry) {
  const auto* w = std::get_if<core::entry::Write>(&entry.payload);
  return w != nullptr && w->cmd.args.size() >= 3 ? w->cmd.args[2] : std::string{};
}

std::string ReadFileBytes(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

// A flush hook that holds every WAL sync until released. The state is
// shared with the hook, so it outlives the test body if the queue does.
class FlushStall {
 public:
  FlushStall() = default;
  ~FlushStall() { Release(); }
  FlushStall(const FlushStall&) = delete;
  FlushStall& operator=(const FlushStall&) = delete;
  FlushStall(FlushStall&&) = delete;
  FlushStall& operator=(FlushStall&&) = delete;

  FlushHook Hook() const {
    return [state = state_](core::ShardId) -> core::Result<void> {
      std::unique_lock lock(state->mu);
      ++state->entered;
      state->cv.notify_all();
      state->cv.wait(lock, [&state] { return state->released; });
      return {};
    };
  }

  bool AwaitEntered(std::chrono::milliseconds timeout = 5s) const {
    std::unique_lock lock(state_->mu);
    return state_->cv.wait_for(lock, timeout, [this] { return state_->entered > 0; });
  }

  void Release() const {
    {
      const std::scoped_lock lock(state_->mu);
      state_->released = true;
    }
    state_->cv.notify_all();
  }

 private:
  struct State {
    std::mutex mu;
    std::condition_variable cv;
    int entered = 0;
    bool released = false;
  };
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

class WalQueueTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = std::make_unique<testing::TempDir>("wal_queue"); }

  void TearDown() override {
    queue_.reset();
    dir_.reset();
  }

  WalConfig DefaultConfig() const {
    return WalConfig{
        .wal_path = dir_->String(),
        .segment_size_bytes = 4096,
        .shard_count = 2,
        .durability = core::Durability::kPowerLoss,
        .min_retention = 1s,
        .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
        // Long enough that only FlushOffsets or teardown persists.
        .offset_fsync_interval = std::chrono::hours{1},
    };
  }

  void OpenWith(WalConfig config) {
    auto result = WalQueue::Open(std::move(config));
    ASSERT_TRUE(result.has_value()) << result.error().message();
    queue_ = std::move(*result);
  }

  // Appends `n` single-entry writes to shard 0, each awaited durable.
  void AppendDurable(int n, size_t value_bytes = 20) {
    for (int i = 0; i < n; ++i) {
      auto r = queue_->Append(0, MakeWrite({"SET", "key", std::string(value_bytes, 'x')}));
      ASSERT_TRUE(r.has_value()) << r.error().message();
      ASSERT_TRUE(r->durable.get().has_value());
    }
  }

  std::filesystem::path CheckpointPath() const {
    return dir_->Path() / "offsets" / std::string(OffsetCheckpoint::kFileName);
  }

  void RunRandomReadProperty(size_t segment_bytes, size_t max_value_bytes, size_t min_sealed);

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<testing::TempDir> dir_;
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

TEST_F(WalQueueTest, ProcessCrashFutureIsReadyAtPublish) {
  auto cfg = DefaultConfig();
  cfg.durability = core::Durability::kProcessCrash;
  OpenWith(cfg);
  EXPECT_EQ(queue_->AckDurability(), core::Durability::kProcessCrash);

  auto r = queue_->Append(0, MakeWrite({"SET", "a", "1"}));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->durable.wait_for(0ms), std::future_status::ready);
  EXPECT_TRUE(r->durable.get().has_value());
}

TEST_F(WalQueueTest, PowerLossFutureMeansPowerDurable) {
  OpenWith(DefaultConfig());
  EXPECT_EQ(queue_->AckDurability(), core::Durability::kPowerLoss);

  for (int i = 0; i < 5; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
    EXPECT_GT(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), r->seq);
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

// --- Read contract ------------------------------------------------------------

TEST_F(WalQueueTest, ReadReturnsAppendedEntries) {
  OpenWith(DefaultConfig());

  for (int i = 0; i < 5; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->durable.get().has_value());
  }

  auto read = queue_->Read(0, 0, 100, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 5U);
  for (size_t i = 0; i < read->size(); ++i) {
    EXPECT_EQ((*read)[i].seq, i);
  }
}

TEST_F(WalQueueTest, ReadOnEmptyQueueTimesOut) {
  OpenWith(DefaultConfig());
  auto start = std::chrono::steady_clock::now();
  auto read = queue_->Read(0, 0, 10, 50ms, core::Durability::kProcessCrash);
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
  auto read = queue_->Read(0, 0, 10, 5s, core::Durability::kProcessCrash);
  auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  EXPECT_LT(elapsed, 1s);
  producer.join();
}

TEST_F(WalQueueTest, ReadAtHeadWaitsForTheNextAppend) {
  OpenWith(DefaultConfig());
  AppendDurable(3);

  std::promise<void> reading;
  std::thread producer([this, &reading] {
    reading.get_future().wait();
    auto r = queue_->Append(0, MakeWrite({"SET", "k", "next"}));
    ASSERT_TRUE(r.has_value());
  });
  reading.set_value();
  auto read = queue_->Read(0, 3, 10, 5s, core::Durability::kProcessCrash);
  producer.join();

  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  EXPECT_EQ(read->front().seq, 3U);
  EXPECT_EQ(ValueOf(read->front()), "next");
}

TEST_F(WalQueueTest, ReadFromSeqStartsThere) {
  OpenWith(DefaultConfig());
  AppendDurable(4);

  auto read = queue_->Read(0, 2, 10, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 2U);
  EXPECT_EQ((*read)[0].seq, 2U);
  EXPECT_EQ((*read)[1].seq, 3U);
}

TEST_F(WalQueueTest, ReadRespectsMaxCount) {
  OpenWith(DefaultConfig());
  AppendDurable(10);

  auto read = queue_->Read(0, 4, 3, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 3U);
  EXPECT_EQ((*read)[0].seq, 4U);
  EXPECT_EQ((*read)[2].seq, 6U);
}

TEST_F(WalQueueTest, ReadSpansSealedAndActiveSegments) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  OpenWith(cfg);
  AppendDurable(10);
  ASSERT_GE(queue_->ListSealedSegments().size(), 2U);

  for (core::SequenceId from = 0; from < 10; ++from) {
    auto read = queue_->Read(0, from, 100, 100ms, core::Durability::kProcessCrash);
    ASSERT_TRUE(read.has_value()) << read.error().message();
    ASSERT_EQ(read->size(), 10 - from) << "from " << from;
    for (size_t i = 0; i < read->size(); ++i) EXPECT_EQ((*read)[i].seq, from + i);
  }
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

  auto read = queue_->Read(0, 0, 100, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->size(), 5U);
}

TEST_F(WalQueueTest, ReadReturnsBatchEntriesFromTheMiddle) {
  OpenWith(DefaultConfig());
  AppendDurable(2);
  std::vector<core::QueueEntry> batch;
  batch.reserve(4);
  for (int i = 0; i < 4; ++i) batch.push_back(MakeWrite({"SET", "b", std::to_string(i)}));
  auto r = queue_->AppendBatch(0, batch);
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->durable.get().has_value());

  auto read = queue_->Read(0, 3, 10, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 3U);
  EXPECT_EQ((*read)[0].seq, 3U);
  EXPECT_EQ(ValueOf((*read)[0]), "1");
  EXPECT_EQ(ValueOf((*read)[2]), "3");
}

TEST_F(WalQueueTest, ReadBelowReclaimedFloorIsOutOfRange) {
  metrics::testing::Reset();
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;
  OpenWith(cfg);
  AppendDurable(20);

  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->FlushOffsets().has_value());

  const core::SequenceId first = queue_->FirstSeq(0).value();
  ASSERT_GT(first, 0U) << "nothing was reclaimed";
  EXPECT_TRUE(queue_->ListSealedSegments().empty());

  auto below = queue_->Read(0, first - 1, 10, 10ms, core::Durability::kProcessCrash);
  ASSERT_FALSE(below.has_value()) << "a read below the reclaimed floor must not be clamped";
  EXPECT_EQ(below.error().code(), core::ErrorCode::kOutOfRange);
  EXPECT_EQ(
      metrics::testing::GetCounterValue(metrics::names::kQueueReadOutOfRangeTotal).value_or(0.0),
      1.0);

  auto at_floor = queue_->Read(0, first, 100, 10ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(at_floor.has_value());
  ASSERT_FALSE(at_floor->empty());
  EXPECT_EQ(at_floor->front().seq, first);
  EXPECT_EQ(at_floor->back().seq, 19U);
}

TEST_F(WalQueueTest, FirstSeqTracksTheOldestRetainedSegment) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;
  OpenWith(cfg);
  EXPECT_EQ(queue_->FirstSeq(0).value(), 0U);
  AppendDurable(20);
  EXPECT_EQ(queue_->FirstSeq(0).value(), 0U) << "nothing committed, nothing reclaimed";

  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->FlushOffsets().has_value());

  const core::SequenceId first = queue_->FirstSeq(0).value();
  EXPECT_GT(first, 0U);
  EXPECT_LE(first, 19U);
  EXPECT_EQ(queue_->Stats()->first_seq, 0U) << "shard 1 still holds seq 0";
}

// Random single and batch appends; every random Read(from, max) must match
// a reference copy of what was appended, both on the live queue and after
// a reopen rebuilds every segment index. `segment_bytes` picks the regime:
// tiny segments rotate constantly, larger ones carry sparse index points.
void WalQueueTest::RunRandomReadProperty(size_t segment_bytes, size_t max_value_bytes,
                                         size_t min_sealed) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = segment_bytes;
  cfg.shard_count = 1;
  OpenWith(cfg);

  std::mt19937_64 rng(0xAB55'5EEDULL ^ segment_bytes);
  std::vector<std::string> reference;
  while (reference.size() < 1000) {
    if (rng() % 4 == 0) {
      std::vector<core::QueueEntry> batch;
      const size_t n = 1 + (rng() % 6);
      for (size_t i = 0; i < n; ++i) {
        const auto value =
            std::to_string(reference.size() + i) + std::string(rng() % (max_value_bytes / 4), 'b');
        batch.push_back(MakeWrite({"SET", "k", value}));
      }
      auto r = queue_->AppendBatch(0, batch);
      ASSERT_TRUE(r.has_value()) << r.error().message();
      for (const auto& e : batch) reference.push_back(ValueOf(e));
    } else {
      const auto value =
          std::to_string(reference.size()) + std::string(rng() % max_value_bytes, 's');
      auto r = queue_->Append(0, MakeWrite({"SET", "k", value}));
      ASSERT_TRUE(r.has_value()) << r.error().message();
      reference.push_back(value);
    }
  }
  ASSERT_GE(queue_->ListSealedSegments().size(), min_sealed);

  auto check_reads = [&](std::string_view phase) {
    for (int i = 0; i < 1500; ++i) {
      const core::SequenceId from = rng() % (reference.size() + 2);
      const size_t max = 1 + (rng() % 64);
      auto read = queue_->Read(0, from, max, 0ms, core::Durability::kProcessCrash);
      ASSERT_TRUE(read.has_value()) << phase << ": " << read.error().message();
      const size_t expected =
          from >= reference.size() ? 0 : std::min<size_t>(max, reference.size() - from);
      ASSERT_EQ(read->size(), expected) << phase << " from=" << from << " max=" << max;
      for (size_t j = 0; j < read->size(); ++j) {
        ASSERT_EQ((*read)[j].seq, from + j) << phase;
        ASSERT_EQ(ValueOf((*read)[j]), reference[from + j]) << phase << " seq=" << from + j;
      }
    }
  };
  check_reads("live");
  queue_.reset();
  OpenWith(cfg);
  check_reads("reopened");
}

TEST_F(WalQueueTest, RandomReadsMatchReferenceAcrossManyRotations) {
  RunRandomReadProperty(/*segment_bytes=*/2048, /*max_value_bytes=*/120, /*min_sealed=*/40);
}

TEST_F(WalQueueTest, RandomReadsMatchReferenceThroughSparseIndex) {
  RunRandomReadProperty(/*segment_bytes=*/size_t{192} * 1024, /*max_value_bytes=*/2048,
                        /*min_sealed=*/3);
}

// --- Recovery -----------------------------------------------------------------

TEST_F(WalQueueTest, SegmentRotationPreservesOrder) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  OpenWith(cfg);
  AppendDurable(10);

  auto read = queue_->Read(0, 0, 100, 100ms, core::Durability::kProcessCrash);
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

  auto read = queue_->Read(0, 0, 1000, 100ms, core::Durability::kProcessCrash);
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

  auto read = queue_->Read(0, 0, 10000, 100ms, core::Durability::kProcessCrash);
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
    AppendDurable(3);
    queue_.reset();
  }

  OpenWith(DefaultConfig());
  auto read = queue_->Read(0, 0, 100, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 3U);
}

TEST_F(WalQueueTest, MissingMiddleSegmentRejectedAsCorruption) {
  // ADP-009 invariant 8: base_seq[i+1] == last_seq[i] + 1. Deleting a middle
  // segment must surface as corruption on Open rather than silently producing
  // a gap in the sequence space that consumers would read across.
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;

  {
    OpenWith(cfg);
    AppendDurable(10);
    queue_.reset();
  }

  // Find and delete a middle (non-first, non-last) segment file.
  const auto shard_dir = dir_->Path() / "shard-0000";
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
    AppendDurable(10);
    queue_.reset();
  }

  OpenWith(cfg);
  auto read = queue_->Read(0, 0, 100, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->size(), 10U);
  for (size_t i = 0; i < read->size(); ++i) {
    EXPECT_EQ((*read)[i].seq, i);
  }
}

// Open always syncs the recovered tail, so under either class every
// recovered entry is power-durable before anything is served.
TEST_F(WalQueueTest, ReopenSetsBothDurableEndsToHead) {
  for (const auto durability : {core::Durability::kProcessCrash, core::Durability::kPowerLoss}) {
    SCOPED_TRACE(core::DurabilityName(durability));
    std::filesystem::remove_all(dir_->Path());
    std::filesystem::create_directories(dir_->Path());
    auto cfg = DefaultConfig();
    cfg.durability = durability;
    {
      OpenWith(cfg);
      for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
      }
      queue_->SkipFinalFlushForTesting();
      queue_.reset();
    }

    const log::testing::CapturingSink logs;
    OpenWith(cfg);
    ASSERT_EQ(queue_->TailSeq(0).value(), 2U);
    EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kProcessCrash).value(), 3U);
    EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 3U);
    EXPECT_TRUE(queue_->AwaitDurable(0, 2, core::Durability::kPowerLoss, 0ms).value());
    EXPECT_FALSE(queue_->AwaitDurable(0, 3, core::Durability::kPowerLoss, 0ms).value());
    EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value());
    const auto records = logs.Records();
    EXPECT_TRUE(std::ranges::any_of(records, [](const log::testing::CapturedRecord& r) {
      return r.msg == "recovered WAL tail synced";
    }));
    queue_.reset();
  }
}

// A crash between creating the newest segment and syncing its header
// leaves an empty or short file; reopening redoes the creation.
TEST_F(WalQueueTest, ReopenRecoversFromAbortedSegmentCreation) {
  for (const size_t stub_bytes : {size_t{0}, size_t{10}, kSegmentHeaderSize}) {
    SCOPED_TRACE(stub_bytes);
    std::filesystem::remove_all(dir_->Path());
    std::filesystem::create_directories(dir_->Path());
    OpenWith(DefaultConfig());
    for (int i = 0; i < 3; ++i) {
      ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
    }
    queue_.reset();

    const auto stub = dir_->Path() / "shard-0000" / "00000000000000000003.log";
    {
      std::ofstream out(stub, std::ios::binary);
      out << std::string(stub_bytes, '\0');
    }
    OpenWith(DefaultConfig());
    EXPECT_EQ(queue_->TailSeq(0).value(), 2U);
    auto next = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->seq, 3U);
    auto read = queue_->Read(0, 0, 10, core::Duration{0}, core::Durability::kProcessCrash);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(read->size(), 4U);
    queue_.reset();
  }
}

// After a power loss the previous segment's unsynced tail can be gone
// while the aborted segment's name survives: recreate at the recovered
// tail, not at the name, since the lost seqs were never durable.
TEST_F(WalQueueTest, AbortedCreationAfterLostTailResumesAtRecoveredTail) {
  OpenWith(DefaultConfig());
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
  }
  queue_.reset();
  const auto shard_dir = dir_->Path() / "shard-0000";
  const auto first = shard_dir / "00000000000000000000.log";
  std::filesystem::resize_file(first, std::filesystem::file_size(first) - 3);
  {
    std::ofstream out(shard_dir / "00000000000000000003.log", std::ios::binary);
  }

  const log::testing::CapturingSink logs;
  OpenWith(DefaultConfig());
  EXPECT_EQ(queue_->TailSeq(0).value(), 1U);
  auto next = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->seq, 2U);
  const auto records = logs.Records();
  EXPECT_TRUE(std::ranges::any_of(records, [](const log::testing::CapturedRecord& r) {
    return r.msg == "unsynced WAL tail lost before an aborted segment creation";
  }));
}

// If the lost tail empties the previous segment, that segment already
// starts at the recovered seq and becomes the active one again.
TEST_F(WalQueueTest, AbortedCreationAfterEmptiedSegmentReopensIt) {
  OpenWith(DefaultConfig());
  for (int i = 0; i < 2; ++i) {
    ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
  }
  queue_.reset();
  const auto shard_dir = dir_->Path() / "shard-0000";
  std::filesystem::resize_file(shard_dir / "00000000000000000000.log", kSegmentHeaderSize + 3);
  {
    std::ofstream out(shard_dir / "00000000000000000002.log", std::ios::binary);
  }

  OpenWith(DefaultConfig());
  auto next = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->seq, 0U);
  EXPECT_FALSE(std::filesystem::exists(shard_dir / "00000000000000000002.log"));
}

// With older segments reaped, the stub's name is the only record of the
// base, so the recreated segment keeps it.
TEST_F(WalQueueTest, AbortedCreationAsOnlySegmentKeepsItsNamedBase) {
  const auto shard_dir = dir_->Path() / "shard-0000";
  std::filesystem::create_directories(shard_dir);
  {
    std::ofstream out(shard_dir / "00000000000000000005.log", std::ios::binary);
  }
  OpenWith(DefaultConfig());
  auto next = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->seq, 5U);
}

// Only the newest segment can be a torn creation; a short older segment
// is corruption.
TEST_F(WalQueueTest, ShortOlderSegmentIsStillCorruption) {
  OpenWith(DefaultConfig());
  ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
  queue_.reset();
  const auto shard_dir = dir_->Path() / "shard-0000";
  std::filesystem::resize_file(shard_dir / "00000000000000000000.log", 10);
  {
    std::ofstream out(shard_dir / "00000000000000000001.log", std::ios::binary);
    out << std::string(kSegmentHeaderSize, '\0');
  }
  auto reopened = WalQueue::Open(DefaultConfig());
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kCorruption);
}

// Commits are gated on the power-durable log, so a persisted offset past
// the recovered head means the log lost synced data: refuse, never clamp.
TEST_F(WalQueueTest, OffsetBeyondRecoveredHeadIsCorruption) {
  for (const auto durability : {core::Durability::kProcessCrash, core::Durability::kPowerLoss}) {
    SCOPED_TRACE(core::DurabilityName(durability));
    std::filesystem::remove_all(dir_->Path());
    std::filesystem::create_directories(dir_->Path());
    auto cfg = DefaultConfig();
    cfg.durability = durability;
    {
      OpenWith(cfg);
      for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
      }
      ASSERT_TRUE(queue_->AwaitDurable(0, 3, core::Durability::kPowerLoss, 5s).value());
      ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 3).has_value());
      ASSERT_TRUE(queue_->FlushOffsets().has_value());
      queue_.reset();
    }
    const auto segment = dir_->Path() / "shard-0000" / "00000000000000000000.log";
    std::filesystem::resize_file(segment, kSegmentHeaderSize);

    auto refused = WalQueue::Open(cfg);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error().code(), core::ErrorCode::kCorruption);
  }
}

// An empty recovered tail syncs nothing, so seq 0 stays uncommittable.
TEST_F(WalQueueTest, ReopenedEmptyShardHasNothingDurable) {
  {
    OpenWith(DefaultConfig());
    queue_.reset();
  }

  OpenWith(DefaultConfig());
  auto rejected = queue_->CommitOffset(core::kColdConsumer, 0, 0);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), core::ErrorCode::kFailedPrecondition);
}

// Sealed segments and the synced tail both feed the reopened end, and
// appends that rotate further keep advancing it.
TEST_F(WalQueueTest, ReopenWatermarkCoversSealedSegmentsAndTail) {
  auto first = DefaultConfig();
  first.segment_size_bytes = 200;
  first.durability = core::Durability::kProcessCrash;
  {
    OpenWith(first);
    for (int i = 0; i < 10; ++i) {
      ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", std::string(20, 'x')})).has_value());
    }
    queue_.reset();
  }

  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  OpenWith(cfg);
  const auto sealed = queue_->ListSealedSegments();
  ASSERT_FALSE(sealed.empty());
  ASSERT_EQ(queue_->TailSeq(0).value(), 9U);
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 10U);
  EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, sealed.front().last_seq).has_value());
  EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 9).has_value());

  const size_t sealed_before = sealed.size();
  AppendDurable(10);
  EXPECT_GT(queue_->ListSealedSegments().size(), sealed_before);
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 20U);
  EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 19).has_value());
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
  EXPECT_EQ(stats->first_seq, 0U);
}

TEST_F(WalQueueTest, AppendAboveMaxValueSizeRejected) {
  auto cfg = DefaultConfig();
  cfg.max_value_size_bytes = 4096;
  OpenWith(cfg);

  // A value above max_value_size_bytes is rejected with kValueTooLarge, the
  // distinct G11 error that references the value ceiling (not segment size).
  auto r = queue_->Append(0, MakeWrite({"SET", "k", std::string(10000, 'x')}));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kValueTooLarge);
}

// G11: a value far larger than what a small segment would hold but within
// max_value_size_bytes is accepted, readable back, and survives reopen.
TEST_F(WalQueueTest, ValueWithinMaxValueSizeAcceptedAndRecovers) {
  auto cfg = DefaultConfig();
  // A 1 MiB value into a fixed-size segment sized to hold it (no jumbo segment).
  cfg.segment_size_bytes = size_t{4} * 1024 * 1024;
  cfg.max_value_size_bytes = size_t{2} * 1024 * 1024;
  cfg.min_retention = 0s;
  const std::string big(size_t{1024} * 1024, 'v');

  {
    OpenWith(cfg);
    auto r = queue_->Append(0, MakeWrite({"SET", "big", big}));
    ASSERT_TRUE(r.has_value()) << "1 MiB value must be accepted";
    ASSERT_TRUE(r->durable.get().has_value());
    queue_.reset();
  }

  OpenWith(cfg);
  auto read = queue_->Read(0, 0, 10, 100ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  const auto* w = std::get_if<core::entry::Write>(&read->front().payload);
  ASSERT_NE(w, nullptr);
  ASSERT_EQ(w->cmd.args.size(), 3U);
  EXPECT_EQ(w->cmd.args[2].size(), big.size());
}

TEST_F(WalQueueTest, IsRecoveringFalseAfterOpen) {
  OpenWith(DefaultConfig());
  EXPECT_FALSE(queue_->IsRecovering());
}

// --- Committed offsets ----------------------------------------------------------

TEST_F(WalQueueTest, CommittedOffsetIsVisibleImmediately) {
  OpenWith(DefaultConfig());
  AppendDurable(3);

  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 0).value(), std::nullopt);
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 0).has_value());
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{0});
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value());
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{2});
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 1).value(), std::nullopt);
  EXPECT_EQ(queue_->PersistedOffset(core::kColdConsumer, 0).value(), std::nullopt)
      << "a commit is not persisted before the interval or FlushOffsets";
}

TEST_F(WalQueueTest, CommitOffsetRejectsRegressionAndNonRetentionConsumers) {
  OpenWith(DefaultConfig());
  AppendDurable(3);
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value());
  EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value()) << "idempotent";

  auto regress = queue_->CommitOffset(core::kColdConsumer, 0, 1);
  ASSERT_FALSE(regress.has_value());
  EXPECT_EQ(regress.error().code(), core::ErrorCode::kInvalidArgument);

  auto stranger = queue_->CommitOffset(core::kResolverConsumer, 0, 1);
  ASSERT_FALSE(stranger.has_value());
  EXPECT_EQ(stranger.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_FALSE(queue_->CommittedOffset(core::kResolverConsumer, 0).has_value());
}

// QUEUE-2: a committed offset cannot pass the power-durable log, even
// under process_crash where the append was already acknowledged.
TEST_F(WalQueueTest, CommitRejectedUntilPowerDurable) {
  auto cfg = DefaultConfig();
  cfg.durability = core::Durability::kProcessCrash;
  OpenWith(cfg);
  FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());

  auto r = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->durable.get().has_value());
  ASSERT_TRUE(stall.AwaitEntered());
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kProcessCrash).value(), 1U);
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 0U);

  auto rejected = queue_->CommitOffset(core::kColdConsumer, 0, r->seq);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 0).value(), std::nullopt);

  stall.Release();
  ASSERT_TRUE(queue_->AwaitDurable(0, r->seq, core::Durability::kPowerLoss, 5s).value());
  EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, r->seq).has_value());
}

TEST_F(WalQueueTest, CheckpointFileUnchangedUntilFlushOffsets) {
  OpenWith(DefaultConfig());
  AppendDurable(3);
  const std::string before = ReadFileBytes(CheckpointPath());
  ASSERT_FALSE(before.empty());

  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value());
  EXPECT_EQ(ReadFileBytes(CheckpointPath()), before) << "a commit wrote the checkpoint eagerly";

  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  EXPECT_NE(ReadFileBytes(CheckpointPath()), before);
  EXPECT_EQ(queue_->PersistedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{2});
}

TEST_F(WalQueueTest, PersisterCheckpointsOnItsInterval) {
  auto cfg = DefaultConfig();
  cfg.offset_fsync_interval = 20ms;
  OpenWith(cfg);
  AppendDurable(3);
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 1).has_value());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (queue_->PersistedOffset(core::kColdConsumer, 0).value() != std::optional<uint64_t>{1} &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(queue_->PersistedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{1});
}

TEST_F(WalQueueTest, CleanCloseCheckpointsCommittedOffsets) {
  {
    OpenWith(DefaultConfig());
    AppendDurable(2);
    ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 1).has_value());
    queue_.reset();
  }

  OpenWith(DefaultConfig());
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{1});
  EXPECT_EQ(queue_->CommittedOffset(core::kHotConsumer, 0).value(), std::nullopt);
}

TEST_F(WalQueueTest, CrashResumesFromLastPersistedOffset) {
  {
    OpenWith(DefaultConfig());
    AppendDurable(8);
    ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 3).has_value());
    ASSERT_TRUE(queue_->FlushOffsets().has_value());
    ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 7).has_value());
    queue_->SkipFinalOffsetPersistForTesting();
    queue_.reset();
  }

  OpenWith(DefaultConfig());
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{3})
      << "only the persisted offset survives a crash; seqs 4..7 are redelivered";
}

// The reaper reclaims only below PERSISTED offsets. An in-memory commit
// past a segment must not let a rotation-triggered sweep delete it: a
// crash would then resume below a deleted segment.
TEST_F(WalQueueTest, ReaperHonoursPersistedNotCommittedOffsets) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;
  OpenWith(cfg);
  AppendDurable(10);

  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 2).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value());
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 9).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 9).has_value());

  // Rotations run the reaper against the persisted offset (2), not 9.
  AppendDurable(10);
  const auto sealed = queue_->ListSealedSegments();
  for (const auto& s : sealed) {
    EXPECT_GT(s.last_seq, 2U) << "a segment the persisted offset released was kept";
  }
  EXPECT_TRUE(std::ranges::any_of(sealed, [](const auto& s) { return s.last_seq <= 9; }))
      << "a segment only the in-memory commit released was reclaimed";

  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  for (const auto& s : queue_->ListSealedSegments()) {
    EXPECT_GT(s.last_seq, 9U) << "persisted 9 now releases older segments";
  }
}

TEST_F(WalQueueTest, OldestRetainedTracksPersistedOffsets) {
  OpenWith(DefaultConfig());
  AppendDurable(3);

  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 1).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 0).has_value());
  EXPECT_EQ(queue_->OldestRetained(0).value(), 0U) << "nothing persisted: FirstSeq";
  ASSERT_TRUE(queue_->FlushOffsets().has_value());

  auto oldest = queue_->OldestRetained(0);
  ASSERT_TRUE(oldest.has_value());
  EXPECT_EQ(*oldest, 0U);  // cold is behind, so min is 0
}

TEST_F(WalQueueTest, PersistFailureCountsRetriesAndHoldsRetention) {
  metrics::testing::Reset();
  OpenWith(DefaultConfig());
  AppendDurable(3);
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 2).has_value());

  queue_->SetOffsetPersistFaultForTesting([] {
    return core::Result<void>(std::unexpected(core::Error{core::ErrorCode::kInternal, "EIO"}));
  });
  EXPECT_FALSE(queue_->FlushOffsets().has_value());
  EXPECT_FALSE(queue_->FlushOffsets().has_value());
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kQueueOffsetPersistFailuresTotal)
                .value_or(0.0),
            2.0);
  EXPECT_EQ(queue_->PersistedOffset(core::kColdConsumer, 0).value(), std::nullopt);

  queue_->SetOffsetPersistFaultForTesting(nullptr);
  ASSERT_TRUE(queue_->FlushOffsets().has_value()) << "the next round must retry";
  EXPECT_EQ(queue_->PersistedOffset(core::kColdConsumer, 0).value(), std::optional<uint64_t>{2});
  EXPECT_GE(metrics::testing::GetHistogramCount(metrics::names::kQueueOffsetPersistDurationSeconds)
                .value_or(0),
            3U);
}

TEST_F(WalQueueTest, LegacyPerFileOffsetLayoutRefusesToOpen) {
  std::filesystem::create_directories(dir_->Path() / "offsets" / "1");
  std::ofstream(dir_->Path() / "offsets" / "1" / "00000000000000000000.offset") << "legacy";

  auto result = WalQueue::Open(DefaultConfig());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_NE(result.error().message().find("recreate the WAL directory"), std::string::npos)
      << result.error().message();
}

TEST_F(WalQueueTest, ChangedRetentionConsumerSetRefusesToOpen) {
  OpenWith(DefaultConfig());
  queue_.reset();

  auto cfg = DefaultConfig();
  cfg.retention_consumers = {core::kColdConsumer};
  auto result = WalQueue::Open(cfg);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kFailedPrecondition);
}

// --- Concurrency ------------------------------------------------------------------

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
    // The consumer owns its read position; commits trail it, clamped to the
    // durable tail as every retention consumer's must be.
    core::SequenceId next = 0;
    auto consume = [&](core::Duration timeout) -> bool {
      auto read = queue_->Read(0, next, 128, timeout, core::Durability::kProcessCrash);
      if (!read.has_value() || read->empty()) return false;
      for (const auto& e : *read) {
        EXPECT_EQ(e.seq, next) << "a read was not contiguous from the cursor";
        observed_seqs.push_back(e.seq);
        next = e.seq + 1;
      }
      if (auto end = queue_->DurableEnd(0, core::Durability::kPowerLoss);
          end.has_value() && *end > 1) {
        EXPECT_TRUE(
            queue_->CommitOffset(core::kHotConsumer, 0, std::min(next - 1, *end - 1)).has_value());
      }
      return true;
    };
    while (!stop.load(std::memory_order_acquire)) (void)consume(50ms);
    while (consume(10ms)) {
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

// Readers race an appender that rotates constantly and a committer that
// persists and reaps behind them. Covers index publication on the active
// segment and segment removal under an in-flight read. A reader that falls
// below the reclaimed floor gets kOutOfRange and rejoins at FirstSeq.
TEST_F(WalQueueTest, ConcurrentReadersSurviveRotationAndReaping) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 2048;
  cfg.shard_count = 1;
  cfg.min_retention = 0s;
  cfg.retention_consumers = {core::kColdConsumer};
  OpenWith(cfg);

  // Appends at least kAppends entries, and keeps going until a reap has run
  // under the readers.
  constexpr int kAppends = 1500;
  std::atomic<bool> done{false};
  std::thread appender([this, &done] {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    for (int i = 0; i < kAppends || (queue_->FirstSeq(0).value() == 0 &&
                                     std::chrono::steady_clock::now() < deadline);
         ++i) {
      if (i % 5 == 0) {
        std::vector<core::QueueEntry> batch;
        batch.reserve(3);
        for (int b = 0; b < 3; ++b) batch.push_back(MakeWrite({"SET", "b", std::to_string(i)}));
        ASSERT_TRUE(queue_->AppendBatch(0, batch).has_value());
      } else {
        ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "s", std::to_string(i)})).has_value());
      }
    }
    done.store(true, std::memory_order_release);
  });
  std::thread committer([this, &done] {
    while (!done.load(std::memory_order_acquire)) {
      if (auto end = queue_->DurableEnd(0, core::Durability::kPowerLoss);
          end.has_value() && *end > 1) {
        EXPECT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, *end - 1).has_value());
      }
      EXPECT_TRUE(queue_->FlushOffsets().has_value());
      std::this_thread::sleep_for(1ms);
    }
  });

  std::atomic<int> rejoins{0};
  auto reader = [this, &done, &rejoins](uint64_t seed) {
    std::mt19937_64 rng(seed);
    core::SequenceId next = 0;
    while (!done.load(std::memory_order_acquire)) {
      auto read = queue_->Read(0, next, 1 + (rng() % 32), 1ms, core::Durability::kProcessCrash);
      if (!read.has_value()) {
        ASSERT_EQ(read.error().code(), core::ErrorCode::kOutOfRange) << read.error().message();
        next = queue_->FirstSeq(0).value();
        rejoins.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      for (const auto& e : *read) {
        ASSERT_EQ(e.seq, next);
        ++next;
      }
    }
  };
  std::thread r1(reader, 1);
  std::thread r2(reader, 2);
  appender.join();
  committer.join();
  r1.join();
  r2.join();

  EXPECT_GT(queue_->FirstSeq(0).value(), 0U) << "the reaper never ran under the readers";
}

TEST_F(WalQueueTest, LaggingConsumerKeepsOldSegmentsAlive) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;  // retention window doesn't interfere.
  OpenWith(cfg);
  AppendDurable(20);

  // Hot caught up; cold is far behind. Segments must stay because cold hasn't
  // committed past them.
  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 1).has_value());
  ASSERT_TRUE(queue_->FlushOffsets().has_value());

  auto sealed_before = queue_->ListSealedSegments();
  ASSERT_FALSE(sealed_before.empty());

  // Another round reclaims nothing new because cold lags.
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  auto sealed_after = queue_->ListSealedSegments();
  EXPECT_EQ(sealed_after.size(), sealed_before.size());
}

TEST_F(WalQueueTest, PersistedCommitAcrossRotationReclaimsOldSegments) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = 200;
  cfg.min_retention = 0s;
  OpenWith(cfg);
  AppendDurable(20);

  auto sealed_before = queue_->ListSealedSegments();
  ASSERT_GT(sealed_before.size(), 0U);

  // Both consumers catch up past the sealed segments.
  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 19).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 19).has_value());
  EXPECT_EQ(queue_->ListSealedSegments().size(), sealed_before.size())
      << "reclaimed before the commit was persisted";
  ASSERT_TRUE(queue_->FlushOffsets().has_value());

  auto sealed_after = queue_->ListSealedSegments();
  EXPECT_EQ(sealed_after.size(), 0U);
}

TEST_F(WalQueueTest, ActiveSegmentNeverDeleted) {
  auto cfg = DefaultConfig();
  cfg.min_retention = 0s;
  OpenWith(cfg);
  AppendDurable(1);
  ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, 0, 0).has_value());
  ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, 0, 0).has_value());
  ASSERT_TRUE(queue_->FlushOffsets().has_value());

  // The active segment (which contains seq 0) must not be in the sealed list
  // and must not be removed.
  auto sealed = queue_->ListSealedSegments();
  EXPECT_TRUE(sealed.empty());
  auto read = queue_->Read(0, 0, 10, 10ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
}

TEST_F(WalQueueTest, BeginAppendPublishMakesEntryVisible) {
  OpenWith(DefaultConfig());

  auto pending = queue_->BeginAppend(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(pending.has_value());
  EXPECT_EQ(pending->seq(), 0U);

  pending->Publish();
  ASSERT_TRUE(pending->durable().get().has_value());

  auto read = queue_->Read(0, 0, 10, 100ms, core::Durability::kProcessCrash);
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
  auto read = queue_->Read(0, 0, 10, 100ms, core::Durability::kProcessCrash);
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

  auto read = queue_->Read(0, 0, 10, 100ms, core::Durability::kProcessCrash);
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
      auto read = queue_->Read(0, next, 32, 10ms, core::Durability::kProcessCrash);
      if (!read.has_value() || read->empty()) return false;
      for (auto& e : *read) {
        EXPECT_EQ(e.seq, next++);
        rpc.Fulfill(e.seq, core::RespValue::SimpleString("OK"));
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
  OpenWith(DefaultConfig());

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
  const auto shard_dir = dir_->Path() / "shard-0000";
  std::string segment_path;
  for (const auto& entry : std::filesystem::directory_iterator(shard_dir)) {
    if (entry.path().extension() == ".log") {
      segment_path = entry.path().string();
      break;
    }
  }
  ASSERT_FALSE(segment_path.empty());

#ifdef _WIN32
  auto f = abyss::platform::fs::Open(segment_path, {.mode = abyss::platform::fs::OpenMode::kWrite});
  ASSERT_TRUE(f.has_value());
  auto size = abyss::platform::fs::FileSize(*f);
  ASSERT_TRUE(size.has_value());
  ASSERT_TRUE(abyss::platform::fs::Ftruncate(*f, *size / 3).has_value());
#else
  struct stat st{};
  ASSERT_EQ(::stat(segment_path.c_str(), &st), 0);
  // Cut the file somewhere inside the mid-batch entries so the closing
  // entry at seq 4 cannot be recovered.
  ASSERT_EQ(::truncate(segment_path.c_str(), st.st_size / 3), 0);
#endif

  OpenWith(DefaultConfig());
  auto read = queue_->Read(0, 0, 100, 50ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->empty());
}

// A stalled device: what each class shows while the flush cannot finish.
TEST_F(WalQueueTest, ProcessCrashAcksAndShowsWritesTheFlushHasNotCovered) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = size_t{1} << 20;
  cfg.durability = core::Durability::kProcessCrash;
  OpenWith(cfg);
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());

  for (int i = 0; i < 3; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", std::to_string(i)}));
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->durable.wait_for(0ms), std::future_status::ready);
    EXPECT_TRUE(r->durable.get().has_value());
  }
  ASSERT_TRUE(stall.AwaitEntered());

  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kProcessCrash).value(), 3U);
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 0U);
  EXPECT_TRUE(queue_->AwaitDurable(0, 2, core::Durability::kProcessCrash, 0ms).value());
  EXPECT_FALSE(queue_->AwaitDurable(0, 0, core::Durability::kPowerLoss, 20ms).value());
  EXPECT_GT(queue_->UnflushedBytes(), 0U);
  EXPECT_GT(queue_->DurabilityLag(), core::Duration::zero());

  auto withheld = queue_->Read(0, 0, 100, 20ms, core::Durability::kPowerLoss);
  ASSERT_TRUE(withheld.has_value());
  EXPECT_TRUE(withheld->empty());
  auto shown = queue_->Read(0, 0, 100, 20ms, core::Durability::kProcessCrash);
  ASSERT_TRUE(shown.has_value());
  EXPECT_EQ(shown->size(), 3U);

  stall.Release();
  ASSERT_TRUE(queue_->AwaitDurable(0, 2, core::Durability::kPowerLoss, 5s).value());
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 3U);
  EXPECT_EQ(queue_->UnflushedBytes(), 0U);
  EXPECT_EQ(queue_->DurabilityLag(), core::Duration::zero());
}

TEST_F(WalQueueTest, PowerLossFuturePendsUntilTheFlushLands) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = size_t{1} << 20;
  OpenWith(cfg);
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());

  auto r = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(stall.AwaitEntered());
  EXPECT_EQ(r->durable.wait_for(50ms), std::future_status::timeout);
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kProcessCrash).value(), 1U);

  stall.Release();
  ASSERT_EQ(r->durable.wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(r->durable.get().has_value());
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 1U);
}

// The flush wakes power_loss readers; they never sit out their timeout.
TEST_F(WalQueueTest, PowerLossReaderWakesOnTheFlush) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = size_t{1} << 20;
  OpenWith(cfg);
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());

  auto reader = std::async(std::launch::async, [this] {
    return queue_->Read(0, 0, 100, 10s, core::Durability::kPowerLoss);
  });
  auto r = queue_->Append(0, MakeWrite({"SET", "k", "v"}));
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(stall.AwaitEntered());
  EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);

  const auto released = std::chrono::steady_clock::now();
  stall.Release();
  ASSERT_EQ(reader.wait_for(5s), std::future_status::ready);
  EXPECT_LT(std::chrono::steady_clock::now() - released, 1s);
  auto read = reader.get();
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->size(), 1U);
  EXPECT_EQ(read->front().seq, 0U);
}

TEST_F(WalQueueTest, StalledFlushBackpressuresThenRejectsAppends) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = size_t{1} << 20;
  cfg.durability = core::Durability::kProcessCrash;
  cfg.durability_window_bytes = 4096;
  cfg.durability_window = std::chrono::minutes{1};
  cfg.admission_timeout = 100ms;
  metrics::testing::Reset();
  OpenWith(cfg);
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());

  const std::string value(1000, 'x');
  int admitted = 0;
  core::Result<PendingAppend> blocked =
      std::unexpected(core::Error{core::ErrorCode::kInternal, ""});
  std::chrono::steady_clock::duration blocked_for{};
  for (; admitted < 100; ++admitted) {
    const auto start = std::chrono::steady_clock::now();
    auto pending = queue_->BeginAppend(0, MakeWrite({"SET", "k", value}));
    if (!pending.has_value()) {
      blocked_for = std::chrono::steady_clock::now() - start;
      blocked = std::move(pending);
      break;
    }
    pending->Publish();
  }
  ASSERT_FALSE(blocked.has_value()) << "the window never filled";
  EXPECT_GE(admitted, 4);
  EXPECT_EQ(blocked.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_NE(blocked.error().message().find("WAL durability window full"), std::string::npos)
      << blocked.error().message();
  EXPECT_GE(blocked_for, 100ms);
  EXPECT_GE(queue_->UnflushedBytes(), 4096U);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalBackpressureRejectionsTotal),
            1.0);

  stall.Release();
  // The released flush syncs before it frees the window, which can take
  // longer than the admission timeout on a slow device.
  const auto drain_by = std::chrono::steady_clock::now() + 10s;
  while (queue_->UnflushedBytes() >= cfg.durability_window_bytes) {
    ASSERT_LT(std::chrono::steady_clock::now(), drain_by) << "the released flush never drained";
    std::this_thread::sleep_for(1ms);
  }
  auto after = queue_->Append(0, MakeWrite({"SET", "k", value}));
  ASSERT_TRUE(after.has_value()) << after.error().message();
  EXPECT_EQ(after->seq, static_cast<core::SequenceId>(admitted));
}

// One value larger than the whole window is admitted when nothing is
// unflushed, so it can never deadlock.
TEST_F(WalQueueTest, ValueLargerThanTheWindowIsAdmittedWhenEmpty) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = size_t{1} << 20;
  cfg.durability_window_bytes = 1024;
  cfg.admission_timeout = 50ms;
  OpenWith(cfg);
  for (int i = 0; i < 3; ++i) {
    auto r = queue_->Append(0, MakeWrite({"SET", "k", std::string(8192, 'x')}));
    ASSERT_TRUE(r.has_value()) << r.error().message();
    ASSERT_TRUE(r->durable.get().has_value());
  }
}

TEST_F(WalQueueTest, FlushedExtentTracksTheLastFlush) {
  auto cfg = DefaultConfig();
  cfg.segment_size_bytes = size_t{1} << 20;
  cfg.durability = core::Durability::kProcessCrash;
  OpenWith(cfg);
  AppendDurable(1);
  ASSERT_TRUE(queue_->AwaitDurable(0, 0, core::Durability::kPowerLoss, 5s).value());
  const auto flushed = queue_->FlushedExtentForTesting(0);
  EXPECT_EQ(std::filesystem::file_size(flushed.path), flushed.offset);

  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  AppendDurable(2);
  ASSERT_TRUE(stall.AwaitEntered());
  const auto stalled = queue_->FlushedExtentForTesting(0);
  EXPECT_EQ(stalled.path, flushed.path);
  EXPECT_EQ(stalled.offset, flushed.offset);
  EXPECT_GT(std::filesystem::file_size(stalled.path), stalled.offset);
}

// The commit thread cannot unwind a throwing fatal capture, so these run
// in a child process.
TEST_F(WalQueueTest, FlushFailureIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto cfg = DefaultConfig();
  cfg.durability = core::Durability::kProcessCrash;
  EXPECT_DEATH(
      {
        OpenWith(cfg);
        queue_->SetFlushHookForTesting([](core::ShardId) -> core::Result<void> {
          return std::unexpected(core::Error{core::ErrorCode::kInternal, "injected EIO"});
        });
        ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
        std::this_thread::sleep_for(10s);
      },
      "WAL flush failed: injected EIO");
}

TEST_F(WalQueueTest, SealFailureDuringRotationIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto cfg = DefaultConfig();
  cfg.durability = core::Durability::kProcessCrash;
  EXPECT_DEATH(
      {
        OpenWith(cfg);
        // The commit thread's flush parks in the first call; every later
        // call is a rotation's seal, and fails.
        auto calls = std::make_shared<std::atomic<int>>(0);
        queue_->SetFlushHookForTesting([calls](core::ShardId) -> core::Result<void> {
          if (calls->fetch_add(1) == 0) {
            for (;;) std::this_thread::sleep_for(1s);
          }
          return std::unexpected(core::Error{core::ErrorCode::kInternal, "injected EIO"});
        });
        ASSERT_TRUE(queue_->Append(0, MakeWrite({"SET", "k", "v"})).has_value());
        while (calls->load() == 0) std::this_thread::sleep_for(1ms);
        for (int i = 0; i < 1000; ++i) {
          ASSERT_TRUE(
              queue_->Append(0, MakeWrite({"SET", "k", std::string(100, 'x')})).has_value());
        }
        std::this_thread::sleep_for(10s);
      },
      "WAL segment seal failed on shard 0: injected EIO");
}

}  // namespace
}  // namespace abyss::queue
