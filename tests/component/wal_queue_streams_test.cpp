#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/offset_checkpoint.h"
#include "abyss/queue/wal_queue.h"
#include "on_exit.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

// A few small frames per segment, past its 4 KiB header.
constexpr size_t kTinySegment = 4096 + 512;
constexpr auto kAck = core::Durability::kProcessCrash;

core::QueueEntry MakeWrite(const std::string& key, const std::string& value = "v") {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", key, value}}},
  };
}

std::size_t FrameBytes(const core::QueueEntry& entry) {
  std::vector<std::byte> frame;
  return frame::EncodeEntry(entry, 0, frame);
}

// A latch tests open once. A wait that times out fails the test and
// goes on, so no thread stays parked behind a failed assertion.
class Latch {
 public:
  void Open() {
    {
      const std::scoped_lock lock(mu_);
      open_ = true;
    }
    cv_.notify_all();
  }
  bool Wait(std::chrono::milliseconds timeout = 10s) {
    std::unique_lock lock(mu_);
    if (cv_.wait_for(lock, timeout, [this] { return open_; })) return true;
    ADD_FAILURE() << "latch wait timed out";
    return false;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool open_ = false;
};

class WalQueueStreamsTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override {
    queue_.reset();
    metrics::testing::Reset();
  }

  WalConfig Config(size_t shards) const {
    return WalConfig{
        .wal_path = dir_.String(),
        .segment_size_bytes = kTinySegment,
        .shard_count = shards,
        .durability = kAck,
        .min_retention = 0s,
        .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
        // Long enough that only FlushOffsets or teardown persists.
        .offset_fsync_interval = std::chrono::hours{1},
    };
  }

  void OpenWith(const WalConfig& config) {
    queue_.reset();
    auto opened = WalQueue::Open(config);
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  // The seq assigned. A failed append fails the test fatally, and every
  // later one returns at once.
  core::SequenceId Append(core::ShardId shard, const std::string& value = "v") {
    if (HasFatalFailure()) return 0;
    auto appended = queue_->Append(shard, MakeWrite("k" + std::to_string(shard), value));
    [&] { ASSERT_TRUE(appended.has_value()) << appended.error().message(); }();
    return appended.has_value() ? appended->seq : 0;
  }

  void AwaitPowerDurable(core::ShardId shard, core::SequenceId seq) {
    auto durable = queue_->AwaitDurable(shard, seq, core::Durability::kPowerLoss, 5s);
    ASSERT_TRUE(durable.has_value() && *durable) << "shard " << shard << " seq " << seq;
  }

  // Both retention consumers commit `seq` on `shard`.
  void CommitBoth(core::ShardId shard, core::SequenceId seq) {
    ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(shard, seq));
    ASSERT_TRUE(queue_->CommitOffset(core::kHotConsumer, shard, seq).has_value());
    ASSERT_TRUE(queue_->CommitOffset(core::kColdConsumer, shard, seq).has_value());
  }

  // Retention honours an offset once both checkpoint slots hold it: one
  // round persists it, the next writes it to the other slot.
  void PersistAndReclaim() {
    ASSERT_TRUE(queue_->FlushOffsets().has_value());
    ASSERT_TRUE(queue_->FlushOffsets().has_value());
  }

  std::vector<core::QueueEntry> ReadAll(core::ShardId shard, core::SequenceId from) {
    auto read = queue_->Read(shard, from, 100000, 0ms, kAck);
    EXPECT_TRUE(read.has_value()) << read.error().message();
    return read.has_value() ? std::move(*read) : std::vector<core::QueueEntry>{};
  }

  // Corrupts the checkpoint slot with the highest epoch, as a torn
  // write of it would.
  void TearNewestCheckpointSlot(size_t shards) const {
    const auto path = dir_.Path() / "offsets" / std::string(OffsetCheckpoint::kFileName);
    const size_t slot = OffsetCheckpoint::SlotBytes(static_cast<uint32_t>(shards), 2);
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    std::array<uint64_t, 2> epochs{};
    for (size_t i = 0; i < epochs.size(); ++i) {
      file.seekg(static_cast<std::streamoff>((i * slot) + 16));
      file.read(reinterpret_cast<char*>(&epochs.at(i)), sizeof(uint64_t));
    }
    const size_t newest = epochs[1] > epochs[0] ? 1 : 0;
    file.seekp(static_cast<std::streamoff>((newest * slot) + 40));
    file.put('\x5a');
    ASSERT_TRUE(file.good());
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"wal_streams"};
  std::unique_ptr<WalQueue> queue_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(WalQueueStreamsTest, AFormat1LayoutRefusesToStart) {
  std::filesystem::create_directories(dir_.Path() / "shard-0000");
  auto opened = WalQueue::Open(Config(1));
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_NE(opened.error().message().find("format 1"), std::string::npos)
      << opened.error().message();
  EXPECT_NE(opened.error().message().find("no migration"), std::string::npos)
      << opened.error().message();
}

TEST_F(WalQueueStreamsTest, ALogPastTheLogCountRefusesToStart) {
  auto config = Config(4);
  config.log_count = 2;
  OpenWith(config);
  Append(3);
  queue_.reset();

  config.log_count = 1;
  auto opened = WalQueue::Open(config);
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_NE(opened.error().message().find("log-0001"), std::string::npos)
      << opened.error().message();
}

// 16 shards over 4 logs: each log holds only its own shards, survives a
// reopen, and is reclaimed on its own.
TEST_F(WalQueueStreamsTest, FourLogsRouteReopenAndReclaimPerLog) {
  constexpr size_t kShards = 16;
  auto config = Config(kShards);
  config.log_count = 4;
  OpenWith(config);
  for (int round = 0; round < 12; ++round) {
    for (core::ShardId shard = 0; shard < kShards; ++shard) Append(shard, std::to_string(round));
  }
  for (core::ShardId shard = 0; shard < kShards; ++shard) AwaitPowerDurable(shard, 11);
  for (uint32_t log = 0; log < 4; ++log) {
    EXPECT_TRUE(std::filesystem::exists(dir_.Path() / ("log-000" + std::to_string(log))));
    const auto sealed = queue_->ListSealedSegments(log, 1000);
    ASSERT_FALSE(sealed.empty()) << "log " << log;
    for (const auto& segment : sealed) {
      for (const auto& range : segment.shards) EXPECT_EQ(range.shard % 4, log);
    }
  }

  OpenWith(config);
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    const auto entries = ReadAll(shard, 0);
    ASSERT_EQ(entries.size(), 12U) << "shard " << shard;
    for (size_t i = 0; i < entries.size(); ++i) EXPECT_EQ(entries[i].seq, i);
  }

  // Log 1's shards stay pinned; the other logs reclaim everything
  // sealed.
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    if (shard % 4 != 1) CommitBoth(shard, 11);
  }
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  for (uint32_t log = 0; log < 4; ++log) {
    EXPECT_EQ(queue_->ListSealedSegments(log, 1000).empty(), log != 1) << "log " << log;
  }
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    const core::SequenceId first = queue_->FirstSeq(shard).value();
    if (shard % 4 == 1) {
      EXPECT_EQ(first, 0U);
    } else {
      EXPECT_GT(first, 0U) << "shard " << shard;
    }
    const auto entries = ReadAll(shard, first);
    EXPECT_EQ(entries.size(), 12 - first) << "shard " << shard;
  }
}

// Must-fix 2: a shard with no frames in the oldest segment still reads
// from its own first frame, before and after that segment is reclaimed.
TEST_F(WalQueueStreamsTest, AShardAbsentFromTheOldestSegmentKeepsItsFirstSeq) {
  OpenWith(Config(2));
  for (int i = 0; i < 12; ++i) Append(0);
  for (int i = 0; i < 3; ++i) Append(1);
  AwaitPowerDurable(1, 2);
  const auto sealed = queue_->ListSealedSegments(0, 1);
  ASSERT_FALSE(sealed.empty());
  ASSERT_TRUE(
      std::ranges::none_of(sealed.front().shards, [](const auto& r) { return r.shard == 1; }));
  EXPECT_EQ(queue_->FirstSeq(1).value(), 0U);
  EXPECT_EQ(ReadAll(1, 0).size(), 3U);

  CommitBoth(0, 11);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  EXPECT_GT(queue_->FirstSeq(0).value(), 0U);
  EXPECT_EQ(queue_->FirstSeq(1).value(), 0U);
  EXPECT_EQ(ReadAll(1, 0).size(), 3U);

  OpenWith(Config(2));
  EXPECT_EQ(queue_->FirstSeq(1).value(), 0U);
  EXPECT_EQ(ReadAll(1, 0).size(), 3U);
}

// A2: shard 1 pins segment 0, so the later segments only shard 0 wrote
// stay too, and FirstSeq still names readable entries.
TEST_F(WalQueueStreamsTest, AStuckShardPinsTheLaterSegmentsAndFirstSeqStaysExact) {
  auto config = Config(2);
  // Off the ring, reads past a reclaim start from the moved index
  // floor.
  config.ring_entries = 4;
  OpenWith(config);
  Append(1);
  for (int i = 0; i < 30; ++i) Append(0);
  CommitBoth(0, 29);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());

  const size_t sealed = queue_->ListSealedSegments().size();
  ASSERT_GE(sealed, 3U);
  EXPECT_EQ(queue_->FirstSeq(0).value(), 0U);
  EXPECT_EQ(ReadAll(0, 0).size(), 30U);
  EXPECT_TRUE(queue_->OldestEligibleUnreapedAge().has_value())
      << "the held-back segments are reported";

  CommitBoth(1, 0);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  EXPECT_TRUE(queue_->ListSealedSegments().empty());
  EXPECT_FALSE(queue_->OldestEligibleUnreapedAge().has_value());
  const core::SequenceId first = queue_->FirstSeq(0).value();
  EXPECT_GT(first, 0U);
  EXPECT_EQ(ReadAll(0, first).size(), 30 - first);
  EXPECT_EQ(queue_->FirstSeq(1).value(), 1U);
  EXPECT_TRUE(ReadAll(1, 1).empty());
}

// Past a reclaim, the first retained frames have no index point of
// their own: with no ring entry or hint for them, a read starts from
// the floor the reclaim left at the oldest retained segment.
TEST_F(WalQueueStreamsTest, AReadJustPastAReclaimStartsFromTheMovedFloor) {
  auto config = Config(1);
  config.ring_entries = 4;
  OpenWith(config);
  for (int i = 0; i < 40; ++i) Append(0);
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, 39));
  const auto oldest = queue_->ListSealedSegments(0, 2);
  ASSERT_EQ(oldest.size(), 2U);
  const core::SequenceId released = oldest[1].shards.front().max_seq;
  CommitBoth(0, released);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  ASSERT_EQ(queue_->FirstSeq(0).value(), released + 1);

  auto read = queue_->Read(0, released + 1, 3, 0ms, kAck);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  ASSERT_EQ(read->size(), 3U);
  EXPECT_EQ(read->front().seq, released + 1);
}

// Must-fix 3: a shard writes far past its ring between two flushes; the
// flush walk still moves its power end to exactly the last frame.
TEST_F(WalQueueStreamsTest, ARingOverflowBetweenFlushesStillMovesThePowerEndExactly) {
  auto config = Config(1);
  config.segment_size_bytes = size_t{1} << 20;
  config.ring_entries = 4;
  config.durability = core::Durability::kPowerLoss;
  OpenWith(config);
  auto held = std::make_shared<Latch>();
  auto entered = std::make_shared<Latch>();
  const abyss::testing::OnExit release([held] { held->Open(); });
  queue_->SetFlushHookForTesting([held, entered](uint32_t) -> core::Result<void> {
    entered->Open();
    held->Wait();
    return {};
  });

  std::vector<DurabilityFuture> futures;
  futures.reserve(100);
  for (int i = 0; i < 100; ++i) {
    auto appended = queue_->Append(0, MakeWrite("k", std::to_string(i)));
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
    futures.push_back(std::move(appended->durable));
  }
  ASSERT_TRUE(entered->Wait());
  EXPECT_LT(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 100U);

  held->Open();
  for (auto& future : futures) {
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(future.get().has_value());
  }
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 100U);
  auto read = queue_->Read(0, 0, 1000, 0ms, core::Durability::kPowerLoss);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  EXPECT_EQ(read->size(), 100U);
}

// A3: a flush that covers only the first frame of a batch leaves the
// shard's power end where it was; the batch shows at power_loss only
// once its last frame is durable.
TEST_F(WalQueueStreamsTest, ABatchStraddlingAFlushIsHiddenUntilItsLastFrameIsDurable) {
  auto config = Config(3);
  config.segment_size_bytes = size_t{1} << 20;
  OpenWith(config);
  auto first_flush = std::make_shared<Latch>();
  auto first_entered = std::make_shared<Latch>();
  auto in_batch = std::make_shared<Latch>();
  auto finish_batch = std::make_shared<Latch>();
  auto flushes = std::make_shared<std::atomic<int>>(0);
  // Released before the batch's future is waited on.
  std::future<core::Result<AppendBatchResult>> batch;
  const abyss::testing::OnExit release([=] {
    first_flush->Open();
    finish_batch->Open();
  });
  queue_->SetFlushHookForTesting([=](uint32_t) -> core::Result<void> {
    if (flushes->fetch_add(1) == 0) {
      first_entered->Open();
      first_flush->Wait();
    }
    return {};
  });
  queue_->SetBatchCommitHookForTesting([=](std::size_t committed) {
    if (committed != 1) return;
    in_batch->Open();
    finish_batch->Wait();
  });

  // The first flush holds a snapshot of shard 1's frame alone.
  Append(1);
  ASSERT_TRUE(first_entered->Wait());
  Append(2);
  batch = std::async(std::launch::async, [this] {
    const std::vector<core::QueueEntry> entries{MakeWrite("a"), MakeWrite("b")};
    return queue_->AppendBatch(0, entries);
  });
  ASSERT_TRUE(in_batch->Wait());

  // The next flush snapshots shard 2's frame and the batch's first.
  first_flush->Open();
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(2, 0));
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 0U);
  EXPECT_FALSE(queue_->AwaitDurable(0, 0, core::Durability::kPowerLoss, 20ms).value());

  finish_batch->Open();
  auto appended = batch.get();
  ASSERT_TRUE(appended.has_value()) << appended.error().message();
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, 1));
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kPowerLoss).value(), 2U);
}

// Must-fix 5: with the preparer stopped, appends that need a new
// segment wait for one, are rejected cleanly at their deadline, and go
// through once a spare is ready.
TEST_F(WalQueueStreamsTest, NoSpareSegmentWaitsThenRejectsCleanly) {
  OpenWith(Config(1));
  std::future<core::Result<AppendResult>> waiting;
  queue_->PauseSegmentPreparerForTesting(0, true);
  const abyss::testing::OnExit resume([this] { queue_->PauseSegmentPreparerForTesting(0, false); });

  core::Result<PendingAppend> refused =
      std::unexpected(core::Error{core::ErrorCode::kInternal, ""});
  core::SequenceId accepted = 0;
  std::chrono::steady_clock::duration waited{};
  for (int i = 0; i < 200; ++i) {
    const auto start = std::chrono::steady_clock::now();
    auto pending = queue_->BeginAppend(0, MakeWrite("k"), start + 100ms);
    if (!pending.has_value()) {
      waited = std::chrono::steady_clock::now() - start;
      refused = std::move(pending);
      break;
    }
    pending->Publish();
    ++accepted;
  }
  ASSERT_FALSE(refused.has_value()) << "the spares never ran out";
  EXPECT_GT(accepted, 5U);
  EXPECT_EQ(refused.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_NE(refused.error().message().find(
                "no spare WAL segment ready: the disk is full or the segment preparer is behind"),
            std::string::npos)
      << refused.error().message();
  EXPECT_GE(waited, 100ms);
  EXPECT_GE(metrics::testing::GetCounterValue(metrics::names::kWalSpareWaitsTotal).value_or(0),
            1.0);
  // Nothing of the refused append was assigned or written.
  EXPECT_EQ(queue_->TailSeq(0).value(), accepted - 1);
  EXPECT_EQ(queue_->DurableEnd(0, kAck).value(), accepted);

  waiting = std::async(std::launch::async, [this] {
    return queue_->Append(0, MakeWrite("k", "after"), std::chrono::steady_clock::now() + 10s);
  });
  EXPECT_EQ(waiting.wait_for(50ms), std::future_status::timeout);
  queue_->PauseSegmentPreparerForTesting(0, false);
  ASSERT_EQ(waiting.wait_for(10s), std::future_status::ready);
  auto after = waiting.get();
  ASSERT_TRUE(after.has_value()) << after.error().message();
  EXPECT_EQ(after->seq, accepted);
  const auto entries = ReadAll(0, 0);
  ASSERT_EQ(entries.size(), accepted + 1);
}

// A1: an idle shard whose every frame was reclaimed reopens at the same
// next seq; so it does when the checkpoint write after the reclaim
// tore.
TEST_F(WalQueueStreamsTest, AFullyReclaimedShardKeepsItsNextSeqAcrossReopen) {
  OpenWith(Config(2));
  for (int i = 0; i < 3; ++i) Append(1);
  for (int i = 0; i < 30; ++i) Append(0);
  CommitBoth(0, 29);
  CommitBoth(1, 2);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  ASSERT_EQ(queue_->FirstSeq(1).value(), 3U) << "shard 1's frames were not reclaimed";

  OpenWith(Config(2));
  EXPECT_EQ(queue_->FirstSeq(1).value(), 3U);
  EXPECT_EQ(queue_->TailSeq(1).value(), 2U);
  EXPECT_EQ(Append(1), 3U);

  // A later commit, persisted only at close, and that write tears.
  CommitBoth(0, 29);
  CommitBoth(1, 3);
  queue_.reset();
  ASSERT_NO_FATAL_FAILURE(TearNewestCheckpointSlot(2));
  OpenWith(Config(2));
  EXPECT_EQ(queue_->CommittedOffset(core::kColdConsumer, 1).value(),
            std::optional<core::SequenceId>{2});
  EXPECT_EQ(Append(1), 4U) << "the retained seq 3 still counts";
}

// A reclaim waits until both checkpoint slots hold the offsets it
// relies on, so damage to the newest slot falls back to offsets that
// still cover it: no seq is reused.
TEST_F(WalQueueStreamsTest, ADamagedNewestSlotAfterAReclaimReusesNoSeq) {
  OpenWith(Config(2));
  for (int i = 0; i < 3; ++i) Append(1);
  for (int i = 0; i < 30; ++i) Append(0);
  CommitBoth(0, 29);
  CommitBoth(1, 2);
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  EXPECT_EQ(queue_->FirstSeq(1).value(), 0U) << "reclaimed on one slot's word";
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  ASSERT_EQ(queue_->FirstSeq(1).value(), 3U) << "shard 1's frames were not reclaimed";
  queue_.reset();
  ASSERT_NO_FATAL_FAILURE(TearNewestCheckpointSlot(2));

  OpenWith(Config(2));
  EXPECT_EQ(queue_->FirstSeq(1).value(), 3U);
  EXPECT_GT(Append(1), 2U) << "a reclaimed seq was reused";
}

// A1 gap: the oldest retained frame lies above what a consumer
// persisted, so the entries in between are lost; Open refuses.
TEST_F(WalQueueStreamsTest, AGapBelowTheFirstRetainedFrameIsCorruption) {
  OpenWith(Config(1));
  for (int i = 0; i < 30; ++i) Append(0);
  CommitBoth(0, 29);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  ASSERT_GT(queue_->FirstSeq(0).value(), 1U);
  queue_.reset();

  // Both slots lose the offsets the reclaim relied on.
  {
    auto checkpoint = OffsetCheckpoint::Open(OffsetCheckpointConfig{
        .dir = dir_.Path() / "offsets",
        .shard_count = 1,
        .consumers = {core::kHotConsumer, core::kColdConsumer},
    });
    ASSERT_TRUE(checkpoint.has_value()) << checkpoint.error().message();
    const std::vector<uint64_t> low{OffsetCheckpoint::Encode(0), OffsetCheckpoint::Encode(0)};
    ASSERT_TRUE((*checkpoint)->Write(low).has_value());
    ASSERT_TRUE((*checkpoint)->Write(low).has_value());
  }

  auto opened = WalQueue::Open(Config(1));
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), core::ErrorCode::kCorruption);
  EXPECT_NE(opened.error().message().find("lost"), std::string::npos) << opened.error().message();
}

std::string ReadBytes(const std::filesystem::path& path, uint64_t from) {
  std::ifstream in(path, std::ios::binary);
  in.seekg(static_cast<std::streamoff>(from));
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

uint64_t OrdinalOf(const std::string& path) {
  return std::stoull(std::filesystem::path(path).stem().string());
}

std::filesystem::path SegmentPath(const std::filesystem::path& wal_path, uint64_t ordinal) {
  std::string name = std::to_string(ordinal);
  name.insert(0, 20 - name.size(), '0');
  return wal_path / "log-0000" / (name + ".seg");
}

// Must-fix 1: in a recycled segment a new frame lands exactly where an
// old frame of the same length sat. A power cut that keeps the new
// commit word but the old CRC and body must read as a clean torn tail;
// the old frame's seq would otherwise make Open fail with corruption.
TEST_F(WalQueueStreamsTest, ANewCommitWordOverAStaleFrameIsACleanTornTail) {
  constexpr uint64_t kFramesAt = 4096;
  OpenWith(Config(1));
  // One frame size throughout, so every life puts frames at one offset.
  const std::string value = "uniform";
  for (int i = 0; i < 40; ++i) Append(0, value);
  CommitBoth(0, 39);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  ASSERT_GT(queue_->FirstSeq(0).value(), 0U);
  const uint64_t active = OrdinalOf(queue_->DurableExtentForTesting(0).path);

  // Once the tail is two segments on, the next spare is a recycled
  // file.
  core::SequenceId tail = 39;
  for (int i = 0; i < 200 && OrdinalOf(queue_->DurableExtentForTesting(0).path) < active + 2; ++i) {
    tail = Append(0, value);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, tail));
  }
  ASSERT_GE(OrdinalOf(queue_->DurableExtentForTesting(0).path), active + 2);
  const auto recycled = SegmentPath(dir_.Path(), active + 3);
  std::string stale;
  for (int i = 0; i < 500; ++i) {
    if (std::filesystem::exists(recycled)) {
      stale = ReadBytes(recycled, kFramesAt);
      if (stale.find_first_not_of('\0') != std::string::npos) break;
    }
    std::this_thread::sleep_for(10ms);
  }
  ASSERT_NE(stale.find_first_not_of('\0'), std::string::npos) << "no recycled spare appeared";

  // Fill into the recycled segment.
  for (int i = 0; i < 20; ++i) tail = Append(0, value);
  queue_.reset();
  const std::string fresh = ReadBytes(recycled, kFramesAt);
  ASSERT_EQ(fresh.size(), stale.size());
  uint64_t fresh_word = 0;
  uint64_t stale_word = 0;
  std::memcpy(&fresh_word, fresh.data(), sizeof(fresh_word));
  std::memcpy(&stale_word, stale.data(), sizeof(stale_word));
  ASSERT_EQ(frame::CommitLen(fresh_word), frame::CommitLen(stale_word));
  ASSERT_NE(frame::CommitGen(fresh_word), frame::CommitGen(stale_word));

  // The new commit word reached the disk; the rest of the frame did
  // not.
  {
    std::fstream file(recycled, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(static_cast<std::streamoff>(kFramesAt + frame::kCommitBytes));
    file.write(stale.data() + frame::kCommitBytes,
               static_cast<std::streamsize>(stale.size() - frame::kCommitBytes));
    ASSERT_TRUE(file.good());
  }

  OpenWith(Config(1));
  const core::SequenceId head = queue_->TailSeq(0).value() + 1;
  EXPECT_LT(head, tail + 1) << "the torn frame was read back";
  const core::SequenceId first = queue_->FirstSeq(0).value();
  const auto entries = ReadAll(0, first);
  ASSERT_EQ(entries.size(), head - first);
  for (size_t i = 0; i < entries.size(); ++i) EXPECT_EQ(entries[i].seq, first + i);
  EXPECT_EQ(Append(0, value), head);
}

class WalQueueScanTest : public WalQueueStreamsTest {
 protected:
  static constexpr size_t kShards = 64;

  WalConfig ScanConfig() const {
    auto config = Config(kShards);
    config.segment_size_bytes = size_t{64} << 10;
    return config;
  }

  // Appends interleaved across every shard; returns the entry frame
  // bytes.
  uint64_t Fill(size_t entries) {
    uint64_t bytes = 0;
    for (size_t i = 0; i < entries; ++i) {
      const auto shard = static_cast<core::ShardId>(((i * 37) + (i / 64)) % kShards);
      const auto entry = MakeWrite("k" + std::to_string(i), std::string(i % 200, 'v'));
      bytes += FrameBytes(entry);
      auto appended = queue_->Append(shard, entry);
      EXPECT_TRUE(appended.has_value()) << appended.error().message();
    }
    return bytes;
  }

  void Bounds(std::vector<core::SequenceId>& from, std::vector<core::SequenceId>& end) {
    from.resize(kShards);
    end.resize(kShards);
    for (core::ShardId shard = 0; shard < kShards; ++shard) {
      from[shard] = queue_->FirstSeq(shard).value();
      end[shard] = queue_->DurableEnd(shard, queue_->AckDurability()).value();
    }
  }
};

// 64 shards on one log: every entry once, in order per shard, never two
// batches of one shard at a time, and the log read about once.
TEST_F(WalQueueScanTest, DeliversEveryEntryOnceInOrderPerShard) {
  OpenWith(ScanConfig());
  const uint64_t entry_bytes = Fill(20000);
  OpenWith(ScanConfig());

  std::vector<core::SequenceId> from;
  std::vector<core::SequenceId> end;
  Bounds(from, end);
  std::vector<core::SequenceId> next = from;
  std::vector<std::atomic<bool>> in_flight(kShards);
  std::atomic<uint64_t> delivered{0};
  std::atomic<bool> overlapped{false};
  std::atomic<bool> out_of_order{false};
  const core::Queue::ScanSink sink = [&](core::ShardId shard,
                                         std::vector<core::QueueEntry>& entries) {
    if (in_flight[shard].exchange(true)) overlapped = true;
    for (const auto& entry : entries) {
      if (entry.seq != next[shard]) out_of_order = true;
      next[shard] = entry.seq + 1;
    }
    delivered.fetch_add(entries.size());
    in_flight[shard].store(false);
    return core::Result<void>{};
  };
  const std::atomic<bool> cancel{false};
  auto scanned = queue_->Scan(from, end, 4, sink, cancel);
  ASSERT_TRUE(scanned.has_value()) << scanned.error().message();
  EXPECT_FALSE(overlapped.load()) << "a shard was delivered concurrently";
  EXPECT_FALSE(out_of_order.load());
  EXPECT_EQ(delivered.load(), 20000U);
  EXPECT_EQ(next, end);

  const double walked =
      metrics::testing::GetCounterValue(metrics::names::kWalScanBytesTotal).value_or(0);
  EXPECT_GE(walked, static_cast<double>(entry_bytes));
  // Padding at each segment end is walked too, but nothing twice.
  EXPECT_LE(walked, static_cast<double>(entry_bytes) * 1.05);
}

TEST_F(WalQueueScanTest, AMidLogRangeDeliversOnlyThatRange) {
  OpenWith(ScanConfig());
  Fill(3000);
  std::vector<core::SequenceId> from;
  std::vector<core::SequenceId> end;
  Bounds(from, end);
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    from[shard] = end[shard] / 3;
    end[shard] = (2 * end[shard]) / 3;
  }
  std::vector<uint64_t> got(kShards, 0);
  std::mutex mu;
  const core::Queue::ScanSink sink = [&](core::ShardId shard,
                                         std::vector<core::QueueEntry>& entries) {
    const std::scoped_lock lock(mu);
    if (!entries.empty() && entries.front().seq != from[shard] + got[shard]) {
      return core::Result<void>(
          std::unexpected(core::Error{core::ErrorCode::kInternal, "out of range"}));
    }
    got[shard] += entries.size();
    return core::Result<void>{};
  };
  const std::atomic<bool> cancel{false};
  auto scanned = queue_->Scan(from, end, 3, sink, cancel);
  ASSERT_TRUE(scanned.has_value()) << scanned.error().message();
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    EXPECT_EQ(got[shard], end[shard] - from[shard]) << "shard " << shard;
  }
}

TEST_F(WalQueueScanTest, ACancelStopsTheScan) {
  OpenWith(ScanConfig());
  Fill(5000);
  std::vector<core::SequenceId> from;
  std::vector<core::SequenceId> end;
  Bounds(from, end);
  std::atomic<bool> cancel{false};
  const core::Queue::ScanSink sink = [&](core::ShardId, std::vector<core::QueueEntry>&) {
    cancel.store(true);
    return core::Result<void>{};
  };
  auto scanned = queue_->Scan(from, end, 4, sink, cancel);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().code(), core::ErrorCode::kUnavailable);
}

TEST_F(WalQueueScanTest, ASinkErrorStopsTheScanAndIsReturned) {
  OpenWith(ScanConfig());
  Fill(5000);
  std::vector<core::SequenceId> from;
  std::vector<core::SequenceId> end;
  Bounds(from, end);
  const core::Queue::ScanSink sink = [](core::ShardId shard, std::vector<core::QueueEntry>&) {
    if (shard == 7) {
      return core::Result<void>(
          std::unexpected(core::Error{core::ErrorCode::kInternal, "sink refused shard 7"}));
    }
    return core::Result<void>{};
  };
  const std::atomic<bool> cancel{false};
  auto scanned = queue_->Scan(from, end, 4, sink, cancel);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().message(), "sink refused shard 7");

  // Retention ran on through the scan's end.
  CommitBoth(0, 0);
  EXPECT_TRUE(queue_->FlushOffsets().has_value());
}

TEST_F(WalQueueScanTest, ARangeBelowTheFirstRetainedSeqIsOutOfRange) {
  OpenWith(Config(1));
  for (int i = 0; i < 30; ++i) Append(0);
  CommitBoth(0, 29);
  ASSERT_NO_FATAL_FAILURE(PersistAndReclaim());
  const core::SequenceId first = queue_->FirstSeq(0).value();
  ASSERT_GT(first, 0U);
  const std::vector<core::SequenceId> from{first - 1};
  const std::vector<core::SequenceId> end{30};
  const std::atomic<bool> cancel{false};
  auto scanned = queue_->Scan(
      from, end, 1,
      [](core::ShardId, std::vector<core::QueueEntry>&) { return core::Result<void>{}; }, cancel);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().code(), core::ErrorCode::kOutOfRange);
}

}  // namespace
}  // namespace abyss::queue
