#include "shard_stream.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/reservation.h"
#include "abyss/queue/wal_queue.h"
#include "latch.h"
#include "on_exit.h"
#include "recording_sink.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

constexpr auto kNow = core::Durability::kProcessCrash;
constexpr auto kPower = core::Durability::kPowerLoss;

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
    ASSERT_EQ(appended->seq, core::kFirstSeq + keys_[shard].size());
    keys_[shard].push_back(key);
  }

  const std::string& KeyAt(core::ShardId shard, core::SequenceId seq) const {
    return keys_.at(shard).at(seq - core::kFirstSeq);
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
      const core::SequenceId end = core::kFirstSeq + keys_[shard].size();
      for (core::SequenceId seq = core::kFirstSeq; seq < end; ++seq) {
        const auto one = Read(shard, seq);
        ASSERT_EQ(one.size(), 1U) << shard << "/" << seq;
        EXPECT_EQ(one[0].seq, seq);
        EXPECT_EQ(KeyOf(one[0]), KeyAt(shard, seq));
        const auto chunk = Read(shard, seq, 7);
        ASSERT_EQ(chunk.size(), std::min<std::size_t>(7, end - seq));
        for (std::size_t i = 0; i < chunk.size(); ++i) {
          EXPECT_EQ(KeyOf(chunk[i]), KeyAt(shard, seq + i));
        }
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
  EXPECT_TRUE(stream.RingPositionForTesting(100).has_value());
  EXPECT_TRUE(stream.RingPositionForTesting(93).has_value());
  // Slot 50 % 8 now holds seq 98.
  EXPECT_FALSE(stream.RingPositionForTesting(50).has_value());
  EXPECT_FALSE(stream.RingPositionForTesting(101).has_value());

  const uint64_t before = stream.SkippedForTesting();
  auto recent = Read(0, 95, 5);
  ASSERT_EQ(recent.size(), 5U);
  EXPECT_EQ(KeyOf(recent[0]), KeyAt(0, 95));
  EXPECT_EQ(stream.SkippedForTesting(), before) << "a ring hit needs no skip";

  auto wrapped = Read(0, 50);
  ASSERT_EQ(wrapped.size(), 1U);
  EXPECT_EQ(wrapped[0].seq, 50U);
  EXPECT_EQ(KeyOf(wrapped[0]), KeyAt(0, 50));
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
  EXPECT_EQ(KeyOf(read[0]), KeyAt(0, 200));
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
  // The 31st frame of shard 0, past 30 rounds of every shard's.
  constexpr core::SequenceId kSeq = core::kFirstSeq + 30;
  ASSERT_EQ(Read(0, kSeq).size(), 1U);
  const uint64_t from_floor = stream.SkippedForTesting() - start;
  EXPECT_GE(from_floor, 30U * kShards);

  const uint64_t mid = stream.SkippedForTesting();
  auto next = Read(0, kSeq + 1);
  ASSERT_EQ(next.size(), 1U);
  EXPECT_EQ(KeyOf(next[0]), KeyAt(0, kSeq + 1));
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
        const core::SequenceId end = queue_->DurableEnd(0, kNow).value_or(core::kFirstSeq);
        for (core::SequenceId back = 1; back <= 24 && back + core::kFirstSeq <= end; ++back) {
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

// --- Durable waits and parked publishes ---

class StreamWaitTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override {
    release_.Open();
    queue_.reset();
    metrics::testing::Reset();
  }

  void Open(core::Durability durability) {
    auto opened = WalQueue::Open(WalConfig{
        .wal_path = dir_.String(),
        .segment_size_bytes = std::size_t{64} << 10,
        .shard_count = 1,
        .durability = durability,
        .min_retention = 0s,
    });
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  // Until release_ opens, no flush syncs, so no power end moves.
  void HoldFlushes() {
    queue_->SetFlushHookForTesting([this](uint32_t) {
      release_.Wait();
      return core::Result<void>{};
    });
  }

  // Bounded: until `shard`'s end at `durability` is past `seq`.
  bool DurableBy(core::SequenceId seq, core::Durability durability) {
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (queue_->DurableEnd(0, durability).value() <= seq) {
      if (std::chrono::steady_clock::now() >= until) return false;
      std::this_thread::sleep_for(1ms);
    }
    return true;
  }

  std::size_t Waiters(core::Durability durability) const {
    return queue_->StreamForTesting(0).DurableWaitersForTesting(durability);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"stream_wait"};
  abyss::testing::Latch release_;
  std::unique_ptr<WalQueue> queue_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// A publish between AwaitDurable's check and its registration is seen
// by the registration's re-check.
TEST_F(StreamWaitTest, APublishBetweenTheCheckAndTheWaitIsNotLost) {
  Open(kNow);
  auto pending = queue_->BeginAppend(0, Set("k", 8));
  ASSERT_TRUE(pending.has_value()) << pending.error().message();
  const core::SequenceId seq = pending->seq();
  queue_->SetDurableWaitHookForTesting([&pending] { pending->Publish(); });
  const auto start = std::chrono::steady_clock::now();
  const auto durable = queue_->AwaitDurable(0, seq, kNow, 10s);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 5s) << "the publish was lost";
  ASSERT_TRUE(durable.has_value()) << durable.error().message();
  EXPECT_TRUE(*durable);
  EXPECT_EQ(Waiters(kNow), 0U);
}

TEST_F(StreamWaitTest, AFlushBetweenTheCheckAndTheWaitIsNotLost) {
  Open(kPower);
  HoldFlushes();
  auto appended = queue_->Append(0, Set("k", 8));
  ASSERT_TRUE(appended.has_value()) << appended.error().message();
  const core::SequenceId seq = appended->seq;
  ASSERT_EQ(queue_->DurableEnd(0, kPower).value(), seq);
  queue_->SetDurableWaitHookForTesting([&] {
    release_.Open();
    EXPECT_TRUE(DurableBy(seq, kPower));
  });
  const auto start = std::chrono::steady_clock::now();
  const auto durable = queue_->AwaitDurable(0, seq, kPower, 10s);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 5s) << "the flush was lost";
  ASSERT_TRUE(durable.has_value()) << durable.error().message();
  EXPECT_TRUE(*durable);
  EXPECT_TRUE(appended->durable.get().has_value());
  EXPECT_EQ(Waiters(kPower), 0U);
}

// A caller that retries on every timeout leaves no waiter behind, at
// either class.
TEST_F(StreamWaitTest, RetriesOnTimeoutLeaveNoWaiters) {
  Open(kPower);
  HoldFlushes();
  auto pending = queue_->BeginAppend(0, Set("k", 8));
  ASSERT_TRUE(pending.has_value()) << pending.error().message();
  const core::SequenceId seq = pending->seq();
  // The append's own future waits on the power list throughout.
  ASSERT_EQ(Waiters(kPower), 1U);
  for (int i = 0; i < 200; ++i) {
    ASSERT_FALSE(queue_->AwaitDurable(0, seq, kNow, 5ms).value()) << "retry " << i;
    ASSERT_EQ(Waiters(kNow), 0U) << "retry " << i;
  }
  pending->Publish();
  for (int i = 0; i < 200; ++i) {
    ASSERT_FALSE(queue_->AwaitDurable(0, seq, kPower, 5ms).value()) << "retry " << i;
    ASSERT_EQ(Waiters(kPower), 1U) << "retry " << i;
  }
  release_.Open();
  EXPECT_TRUE(queue_->AwaitDurable(0, seq, kPower, 10s).value());
  EXPECT_TRUE(DurableBy(seq, kPower));
  EXPECT_EQ(Waiters(kNow), 0U);
}

TEST_F(StreamWaitTest, ARegisteredNodeIsPostedOnceItsClassPasses) {
  Open(kPower);
  HoldFlushes();
  abyss::testing::RecordingSink sink;
  auto pending = queue_->BeginAppend(0, Set("k", 8));
  ASSERT_TRUE(pending.has_value()) << pending.error().message();
  const core::SequenceId seq = pending->seq();
  core::SeqWaiterNode published(seq, sink.For(1));
  core::SeqWaiterNode power(seq, sink.For(2));
  core::SeqWaiterNode cancelled(seq, sink.For(3));
  ASSERT_FALSE(queue_->WhenDurable(0, kNow, published).has_value());
  ASSERT_FALSE(queue_->WhenDurable(0, kPower, power).has_value());
  ASSERT_FALSE(queue_->WhenDurable(0, kPower, cancelled).has_value());
  EXPECT_TRUE(queue_->CancelDurable(0, kPower, cancelled));
  EXPECT_FALSE(queue_->CancelDurable(0, kPower, cancelled));

  pending->Publish();
  ASSERT_TRUE(sink.WaitFor(1));
  EXPECT_EQ(sink.Events().front().slot_seq, 1U);
  release_.Open();
  ASSERT_TRUE(sink.WaitFor(2));
  const auto events = sink.Events();
  ASSERT_EQ(events.size(), 2U);
  EXPECT_EQ(events[1].slot_seq, 2U);
  EXPECT_EQ(events[1].kind, core::CompletionKind::kDurable);
  EXPECT_TRUE(events[1].result.has_value());

  core::SeqWaiterNode late(seq, sink.For(4));
  const auto now = queue_->WhenDurable(0, kPower, late);
  EXPECT_TRUE(now.has_value() && now->has_value()) << "a durable seq is ready at once";
}

// Shutdown fails process_crash's waiters; the committer's stop, after
// the final flush, fails power_loss's.
TEST_F(StreamWaitTest, StoppingFailsEachClassesWaitersUnavailable) {
  Open(kPower);
  abyss::testing::RecordingSink sink;
  core::SeqWaiterNode published(core::kFirstSeq + 5, sink.For(1));
  core::SeqWaiterNode power(core::kFirstSeq + 5, sink.For(2));
  ASSERT_FALSE(queue_->WhenDurable(0, kNow, published).has_value());
  ASSERT_FALSE(queue_->WhenDurable(0, kPower, power).has_value());
  queue_.reset();
  ASSERT_TRUE(sink.WaitFor(2));
  const auto events = sink.Events();
  ASSERT_EQ(events.size(), 2U);
  for (const core::CompletionEvent& event : events) {
    SCOPED_TRACE(event.slot_seq);
    ASSERT_FALSE(event.result.has_value());
    EXPECT_EQ(event.result.error().code(), core::ErrorCode::kUnavailable);
  }
  EXPECT_EQ(events[0].result.error().message(), "queue shutting down");
  EXPECT_EQ(events[1].result.error().message(), "WAL group committer stopped");
}

// A publish waiting on an earlier reservation parks past its yield, and
// wakes once that reservation publishes.
TEST_F(StreamWaitTest, APublishBehindAHeldReservationParksAndWakes) {
  Open(kNow);
  std::vector<core::QueueEntry> first{Set("a", 8)};
  auto held = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = first}});
  ASSERT_TRUE(held.has_value()) << held.error().message();
  auto behind = std::async(std::launch::async, [this] {
    std::vector<core::QueueEntry> second{Set("b", 8)};
    auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = second}});
    if (!reserved.has_value()) return false;
    queue_->Complete(std::move(*reserved));
    return true;
  });
  const auto until = std::chrono::steady_clock::now() + 10s;
  while (queue_->StreamForTesting(0).PublishParkedForTesting() == 0 &&
         std::chrono::steady_clock::now() < until) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(queue_->StreamForTesting(0).PublishParkedForTesting(), 1U);
  EXPECT_EQ(behind.wait_for(20ms), std::future_status::timeout) << "published past a hole";

  queue_->Complete(std::move(*held));
  ASSERT_EQ(behind.wait_for(5s), std::future_status::ready) << "the parked publish never woke";
  EXPECT_TRUE(behind.get());
  EXPECT_EQ(queue_->DurableEnd(0, kNow).value(), core::kFirstSeq + 2);
  EXPECT_EQ(queue_->StreamForTesting(0).PublishParkedForTesting(), 0U);
}

// A publish landing just as a publisher parks, when it counts no one
// parked and so notifies no one, is seen by the parker's re-read.
TEST_F(StreamWaitTest, APublishRacingAParkIsNotLost) {
  Open(kNow);
  std::vector<core::QueueEntry> first{Set("a", 8)};
  auto held = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = first}});
  ASSERT_TRUE(held.has_value()) << held.error().message();
  abyss::testing::Latch parking;
  abyss::testing::Latch published;
  queue_->SetPublishParkHookForTesting(0, [&] {
    parking.Open();
    published.Wait();
  });
  auto behind = std::async(std::launch::async, [this] {
    std::vector<core::QueueEntry> second{Set("b", 8)};
    auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = second}});
    if (!reserved.has_value()) return false;
    queue_->Complete(std::move(*reserved));
    return true;
  });
  ASSERT_TRUE(parking.Wait());
  queue_->Complete(std::move(*held));
  published.Open();
  ASSERT_EQ(behind.wait_for(5s), std::future_status::ready) << "the racing publish was lost";
  EXPECT_TRUE(behind.get());
  EXPECT_EQ(queue_->DurableEnd(0, kNow).value(), core::kFirstSeq + 2);
}

}  // namespace
}  // namespace abyss::queue
