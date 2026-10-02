#include "shard_stream.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"
#include "on_exit.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

constexpr auto kNow = core::Durability::kProcessCrash;

core::QueueEntry Set(const std::string& key, std::size_t value_bytes) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Write{.cmd = core::RespCommand{{"SET", key, std::string(value_bytes, 'v')}}},
  };
}

std::string KeyOf(const core::QueueEntry& entry) {
  return std::get<core::entry::Write>(entry.payload).cmd.args.at(1);
}

class ShardStreamTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override {
    queue_.reset();
    metrics::testing::Reset();
  }

  void Open(std::size_t shards, std::size_t ring,
            std::size_t segment_bytes = std::size_t{64} << 10) {
    queue_.reset();
    auto opened = WalQueue::Open(WalConfig{
        .wal_path = dir_.String(),
        .segment_size_bytes = segment_bytes,
        .shard_count = shards,
        .ring_entries = ring,
        .min_retention = 0s,
    });
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  void Append(core::ShardId shard, std::size_t value_bytes) {
    const std::string key = "s" + std::to_string(shard) + "-" + std::to_string(keys_[shard].size());
    auto appended = queue_->Append(shard, Set(key, value_bytes));
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
    ASSERT_EQ(appended->seq, keys_[shard].size());
    keys_[shard].push_back(key);
  }

  std::vector<core::QueueEntry> Read(core::ShardId shard, core::SequenceId from,
                                     std::size_t max = 1) {
    auto read = queue_->Read(shard, from, max, 0ms, kNow);
    EXPECT_TRUE(read.has_value()) << read.error().message();
    return read.has_value() ? std::move(*read) : std::vector<core::QueueEntry>{};
  }

  // Every seq read alone and in chunks matches what was appended.
  void ExpectEveryReadMatches(std::size_t shards) {
    for (core::ShardId shard = 0; shard < shards; ++shard) {
      const auto& keys = keys_[shard];
      for (core::SequenceId seq = 0; seq < keys.size(); ++seq) {
        const auto one = Read(shard, seq);
        ASSERT_EQ(one.size(), 1U) << shard << "/" << seq;
        EXPECT_EQ(one[0].seq, seq);
        EXPECT_EQ(KeyOf(one[0]), keys[seq]);
        const auto chunk = Read(shard, seq, 7);
        ASSERT_EQ(chunk.size(), std::min<std::size_t>(7, keys.size() - seq));
        for (std::size_t i = 0; i < chunk.size(); ++i) EXPECT_EQ(KeyOf(chunk[i]), keys[seq + i]);
      }
    }
  }

  const ShardStream& Stream(core::ShardId shard) const { return queue_->StreamForTesting(shard); }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"shard_stream"};
  std::unique_ptr<WalQueue> queue_;
  std::vector<std::vector<std::string>> keys_ = std::vector<std::vector<std::string>>(16);
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// The rings are allocated whole at open: 16 bytes a slot, every shard.
TEST_F(ShardStreamTest, TheRingsAreCountedInTheirGauge) {
  Open(2, 8);
  EXPECT_EQ(ShardStream::RingBytes(8), 128U);
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kWalRingBytes), 256.0);
  queue_.reset();
  EXPECT_EQ(metrics::testing::GetGaugeValue(metrics::names::kWalRingBytes), 0.0);
}

// Interleaved shards of mixed sizes roll many small segments, so reads
// cross other shards' frames and roll padding, live and after reopen.
TEST_F(ShardStreamTest, ReadsEverySeqAcrossInterleavedShardsAndRolls) {
  constexpr std::size_t kShards = 4;
  Open(kShards, 4, std::size_t{16} << 10);
  for (int i = 0; i < 600; ++i) {
    const auto shard = static_cast<core::ShardId>(((i * 7) + (i / 5)) % kShards);
    Append(shard, 32 + static_cast<std::size_t>((i * 131) % 600));
  }
  ExpectEveryReadMatches(kShards);

  Open(kShards, 4, std::size_t{16} << 10);
  ExpectEveryReadMatches(kShards);
}

TEST_F(ShardStreamTest, RingHitsRecentSeqsAndAWrappedSlotFallsBackToTheIndex) {
  Open(2, 8);
  for (int i = 0; i < 100; ++i) {
    Append(0, 64);
    Append(1, 64);
  }
  const ShardStream& stream = Stream(0);
  EXPECT_TRUE(stream.RingPositionForTesting(99).has_value());
  EXPECT_TRUE(stream.RingPositionForTesting(92).has_value());
  // Slot 50 % 8 now holds seq 98.
  EXPECT_FALSE(stream.RingPositionForTesting(50).has_value());
  EXPECT_FALSE(stream.RingPositionForTesting(100).has_value());

  const uint64_t before = stream.SkippedForTesting();
  auto recent = Read(0, 95, 5);
  ASSERT_EQ(recent.size(), 5U);
  EXPECT_EQ(KeyOf(recent[0]), keys_[0][95]);
  EXPECT_EQ(stream.SkippedForTesting(), before) << "a ring hit needs no skip";

  auto wrapped = Read(0, 50);
  ASSERT_EQ(wrapped.size(), 1U);
  EXPECT_EQ(wrapped[0].seq, 50U);
  EXPECT_EQ(KeyOf(wrapped[0]), keys_[0][50]);
  EXPECT_GT(stream.SkippedForTesting(), before) << "the miss went through the index";
}

TEST_F(ShardStreamTest, TheIndexKeepsAPointEvery64KiBOfAShardsFrames) {
  Open(2, 4, std::size_t{1} << 20);
  for (int i = 0; i < 320; ++i) {
    Append(0, 1000);
    if (i % 32 == 0) Append(1, 16);
  }
  // About 320 KiB of shard 0's frames.
  EXPECT_GE(Stream(0).index_points(), 5U);
  EXPECT_LE(Stream(0).index_points(), 6U);
  EXPECT_EQ(Stream(1).index_points(), 1U);

  const uint64_t before = Stream(0).SkippedForTesting();
  auto read = Read(0, 200);
  ASSERT_EQ(read.size(), 1U);
  EXPECT_EQ(KeyOf(read[0]), keys_[0][200]);
  // From the floor point, at most a stride of shard 0 plus shard 1's
  // few.
  EXPECT_LE(Stream(0).SkippedForTesting() - before, 80U);
}

// Frames larger than the index stride each get a point, and reads
// across them and the rolls between them come back whole, live and
// reopened.
TEST_F(ShardStreamTest, EntriesLargerThanTheIndexStride) {
  Open(2, 4, std::size_t{1} << 20);
  for (int i = 0; i < 24; ++i) {
    Append(0, std::size_t{100} << 10);
    Append(1, 16);
  }
  EXPECT_EQ(Stream(0).index_points(), 24U);
  ExpectEveryReadMatches(2);
  Open(2, 4, std::size_t{1} << 20);
  EXPECT_EQ(Stream(0).index_points(), 24U);
  ExpectEveryReadMatches(2);
}

// Off the ring, a Read starting where the previous one stopped skips
// only the other shards' frames in between, not the way from the index
// floor.
TEST_F(ShardStreamTest, AHintSavesConsecutiveReadsTheSkipFromTheFloor) {
  constexpr std::size_t kShards = 8;
  Open(kShards, 4);
  for (int round = 0; round < 64; ++round) {
    for (core::ShardId shard = 0; shard < kShards; ++shard) Append(shard, 16);
  }
  const ShardStream& stream = Stream(0);
  ASSERT_EQ(stream.index_points(), 1U);

  const uint64_t start = stream.SkippedForTesting();
  ASSERT_EQ(Read(0, 30).size(), 1U);
  const uint64_t from_floor = stream.SkippedForTesting() - start;
  EXPECT_GE(from_floor, 30U * kShards);

  const uint64_t mid = stream.SkippedForTesting();
  auto next = Read(0, 31);
  ASSERT_EQ(next.size(), 1U);
  EXPECT_EQ(KeyOf(next[0]), keys_[0][31]);
  EXPECT_LE(stream.SkippedForTesting() - mid, kShards);
}

// Readers look up recent seqs while the writer wraps a small ring under
// them; a slot read mid-rewrite must fail validation, never return
// another seq's position (which Read reports as an internal error). It
// runs to a count, or a deadline on a slow machine.
TEST_F(ShardStreamTest, SeqlockReadersRacingAWrappingWriter) {
  Open(1, 16);
  constexpr uint64_t kAppends = 20000;
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> reads{0};
  std::vector<std::jthread> readers;
  // Declared after the readers, so they are stopped before the join.
  const abyss::testing::OnExit stopper([&stop] { stop.store(true, std::memory_order_release); });
  readers.reserve(3);
  for (int r = 0; r < 3; ++r) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        const core::SequenceId end = queue_->DurableEnd(0, kNow).value_or(0);
        for (core::SequenceId back = 1; back <= 24 && back <= end; ++back) {
          auto read = queue_->Read(0, end - back, 1, 0ms, kNow);
          ASSERT_TRUE(read.has_value()) << read.error().message();
          ASSERT_EQ(read->size(), 1U);
          ASSERT_EQ(read->front().seq, end - back);
          reads.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  uint64_t appended = 0;
  while (appended < kAppends && std::chrono::steady_clock::now() < deadline) {
    auto result = queue_->Append(0, Set("k", 24));
    ASSERT_TRUE(result.has_value()) << result.error().message();
    ++appended;
  }
  stop.store(true, std::memory_order_release);
  readers.clear();
  EXPECT_GT(appended, 0U);
  EXPECT_GT(reads.load(), 0U);
}

}  // namespace
}  // namespace abyss::queue
