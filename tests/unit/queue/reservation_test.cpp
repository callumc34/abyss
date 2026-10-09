#include "abyss/queue/reservation.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/wal_queue.h"
#include "binary_io.h"
#include "latch.h"
#include "on_exit.h"
#include "shard_stream.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;
using abyss::testing::Latch;

constexpr auto kProcess = core::Durability::kProcessCrash;
constexpr auto kPower = core::Durability::kPowerLoss;
constexpr uint64_t kHeaderBlock = 4096;
constexpr LogPosition kNoPosition = ~LogPosition{0};
// Past kLockHoldFrameBytes once framed.
constexpr std::size_t kLarge = std::size_t{20} << 10;
// Whole microseconds, as a frame stores them.
constexpr core::WallTime kStamp{std::chrono::microseconds{1'700'000'000'123'456}};

core::QueueEntry Write(const std::string& key, std::size_t value_bytes = 8) {
  return core::QueueEntry{
      .appended_at = kStamp,
      .payload =
          core::entry::Write{.cmd = core::RespCommand{{"SET", key, std::string(value_bytes, 'v')}}},
  };
}

std::string KeyOf(const core::QueueEntry& entry) {
  return std::get<core::entry::Write>(entry.payload).cmd.args.at(1);
}

std::size_t ValueBytes(const core::QueueEntry& entry) {
  return std::get<core::entry::Write>(entry.payload).cmd.args.at(2).size();
}

class ReservationTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override {
    queue_.reset();
    metrics::testing::Reset();
  }

  WalConfig Config(std::size_t shards) const {
    return WalConfig{
        .wal_path = dir_.String(),
        .segment_size_bytes = std::size_t{1} << 20,
        .shard_count = shards,
        .min_retention = 0s,
        .retention_consumers = {core::kHotConsumer},
        .offset_fsync_interval = std::chrono::hours{1},
    };
  }

  void OpenWith(const WalConfig& config) {
    queue_.reset();
    config_ = config;
    auto opened = WalQueue::Open(config);
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  core::SequenceId PublishedEnd(core::ShardId shard) {
    return queue_->DurableEnd(shard, kProcess).value();
  }

  std::vector<core::QueueEntry> ReadAll(core::ShardId shard) {
    auto read = queue_->Read(shard, 0, 100000, 0ms, kProcess);
    EXPECT_TRUE(read.has_value()) << read.error().message();
    return read.has_value() ? std::move(*read) : std::vector<core::QueueEntry>{};
  }

  // `count` bytes from `pos` on, read from its segment file.
  std::vector<std::byte> BytesAt(LogPosition pos, std::size_t count) const {
    const uint64_t space = config_.segment_size_bytes - kHeaderBlock;
    const uint64_t ordinal = pos / space;
    std::string name = std::to_string(ordinal);
    name.insert(0, 20 - name.size(), '0');
    std::ifstream in(dir_.Path() / "log-0000" / (name + ".seg"), std::ios::binary);
    in.seekg(static_cast<std::streamoff>(kHeaderBlock + (pos - (ordinal * space))));
    std::vector<std::byte> bytes(count);
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(count));
    bytes.resize(static_cast<std::size_t>(in.gcount()));
    return bytes;
  }

  // The batch at `pos` as the buffered encoder gives `expected`, but for
  // each frame's sealed CRC and its commit word's gen.
  void ExpectOnDisk(LogPosition pos, const std::vector<std::byte>& expected) const {
    const std::vector<std::byte> actual = BytesAt(pos, expected.size());
    ASSERT_EQ(actual.size(), expected.size());
    for (std::size_t off = 0; off < expected.size();) {
      const auto word = binary::LoadLE<uint64_t>(actual.data() + off);
      const uint32_t len = frame::CommitLen(binary::LoadLE<uint64_t>(expected.data() + off));
      EXPECT_EQ(frame::CommitLen(word), len) << "frame at " << off;
      const std::size_t body = off + frame::kCommitBytes + frame::kCrcBytes;
      const std::size_t end = off + frame::FrameSize(len);
      EXPECT_TRUE(std::equal(expected.begin() + static_cast<std::ptrdiff_t>(body),
                             expected.begin() + static_cast<std::ptrdiff_t>(end),
                             actual.begin() + static_cast<std::ptrdiff_t>(body)))
          << "frame at " << off;
      off = end;
    }
  }

  // The frame header at `pos`, read from its segment file.
  std::optional<frame::Header> HeaderAt(LogPosition pos) const {
    const uint64_t space = config_.segment_size_bytes - kHeaderBlock;
    const uint64_t ordinal = pos / space;
    std::string name = std::to_string(ordinal);
    name.insert(0, 20 - name.size(), '0');
    std::ifstream in(dir_.Path() / "log-0000" / (name + ".seg"), std::ios::binary);
    in.seekg(static_cast<std::streamoff>(kHeaderBlock + (pos - (ordinal * space))));
    std::vector<std::byte> bytes(kLarge * 2);
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    bytes.resize(static_cast<std::size_t>(in.gcount()));
    if (bytes.size() < frame::kMinFrameBytes) return std::nullopt;
    const frame::View view = frame::Inspect(binary::LoadLE<uint64_t>(bytes.data()), bytes,
                                            static_cast<uint32_t>(ordinal), 0, false);
    if (view.state != frame::State::kFilled) return std::nullopt;
    return view.header;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"reservation"};
  WalConfig config_;
  std::unique_ptr<WalQueue> queue_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(ReservationTest, OneShardReservesThenPublishesOnComplete) {
  OpenWith(Config(2));
  std::vector<core::QueueEntry> entries{Write("a"), Write("b")};
  const std::array parts{ShardEntries{.shard = 1, .entries = entries}};
  auto reserved = queue_->Reserve(parts);
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  ASSERT_EQ(reserved->ranges().size(), 1U);
  EXPECT_EQ(reserved->ranges()[0].shard, 1U);
  EXPECT_EQ(reserved->ranges()[0].first, 0U);
  EXPECT_EQ(reserved->ranges()[0].last, 1U);
  EXPECT_EQ(entries[1].seq, 1U);
  EXPECT_EQ(queue_->ReadyToComplete(0), 1U);
  EXPECT_EQ(ReservationsHeld(), 1U);
  EXPECT_EQ(PublishedEnd(1), 0U) << "nothing is visible before Complete";

  DurableFutures durable = queue_->Complete(std::move(*reserved));
  EXPECT_EQ(ReservationsHeld(), 0U);
  EXPECT_EQ(queue_->ReadyToComplete(0), 0U);
  ASSERT_EQ(durable.size(), 1U);
  EXPECT_EQ(durable[0].shard, 1U);
  ASSERT_EQ(durable[0].durable.wait_for(0s), std::future_status::ready);
  EXPECT_TRUE(durable[0].durable.get().has_value());
  EXPECT_EQ(PublishedEnd(1), 2U);
  const auto read = ReadAll(1);
  ASSERT_EQ(read.size(), 2U);
  EXPECT_EQ(KeyOf(read[0]), "a");
  EXPECT_EQ(KeyOf(read[1]), "b");

  std::vector<core::QueueEntry> more{Write("c")};
  auto next = queue_->Reserve(std::array{ShardEntries{.shard = 1, .entries = more}});
  ASSERT_TRUE(next.has_value()) << next.error().message();
  EXPECT_EQ(next->ranges()[0].first, 2U);
  queue_->Complete(std::move(*next));
  EXPECT_EQ(PublishedEnd(1), 3U);
}

// One log reservation: the shards' frames are back to back in part
// order, and every batch_rest runs to the batch's end.
TEST_F(ReservationTest, ThreeShardsShareOneContiguousBatch) {
  OpenWith(Config(4));
  ASSERT_TRUE(queue_->Append(1, Write("before")).has_value());
  std::vector<core::QueueEntry> zero{Write("z0"), Write("z1")};
  std::vector<core::QueueEntry> one{Write("o0", 300)};
  std::vector<core::QueueEntry> three{Write("t0"), Write("t1", kLarge), Write("t2")};
  const std::array parts{ShardEntries{.shard = 0, .entries = zero},
                         ShardEntries{.shard = 1, .entries = one},
                         ShardEntries{.shard = 3, .entries = three}};
  std::vector<std::size_t> sizes;
  for (const auto& part : parts) {
    for (const auto& entry : part.entries) sizes.push_back(frame::EntryFrameSize(entry));
  }
  auto reserved = queue_->Reserve(parts);
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  const std::vector<ReservedRange> ranges = reserved->ranges();
  ASSERT_EQ(ranges.size(), 3U);
  EXPECT_EQ(ranges[0].first, 0U);
  EXPECT_EQ(ranges[0].last, 1U);
  EXPECT_EQ(ranges[1].first, 1U) << "shard 1 continues its own seqs";
  EXPECT_EQ(ranges[1].last, 1U);
  EXPECT_EQ(ranges[2].first, 0U);
  EXPECT_EQ(ranges[2].last, 2U);
  DurableFutures durable = queue_->Complete(std::move(*reserved));
  ASSERT_EQ(durable.size(), 3U);

  std::vector<LogPosition> positions;
  for (const ReservedRange& range : ranges) {
    for (core::SequenceId seq = range.first; seq <= range.last; ++seq) {
      const LogPosition pos =
          queue_->StreamForTesting(range.shard).RingPositionForTesting(seq).value_or(kNoPosition);
      ASSERT_NE(pos, kNoPosition);
      positions.push_back(pos);
    }
  }
  ASSERT_EQ(positions.size(), sizes.size());
  uint64_t total = 0;
  for (const std::size_t size : sizes) total += size;
  for (std::size_t i = 0; i < positions.size(); ++i) {
    if (i > 0) EXPECT_EQ(positions[i], positions[i - 1] + sizes[i - 1]) << "frame " << i;
    const frame::Header header =
        HeaderAt(positions[i]).value_or(frame::Header{.kind = frame::Kind::kPadding});
    ASSERT_EQ(header.kind, frame::Kind::kEntry) << "frame " << i;
    EXPECT_EQ(header.batch_rest, total - (positions[i] - positions[0])) << "frame " << i;
    EXPECT_EQ(
        header.appended_at_us,
        std::chrono::duration_cast<std::chrono::microseconds>(kStamp.time_since_epoch()).count());
  }
  EXPECT_EQ(PublishedEnd(0), 2U);
  EXPECT_EQ(PublishedEnd(1), 2U);
  EXPECT_EQ(PublishedEnd(3), 3U);
  EXPECT_EQ(PublishedEnd(2), 0U);
  EXPECT_EQ(ValueBytes(ReadAll(3).at(1)), kLarge);
}

// Frames are encoded straight into the segment; on disk each matches
// the buffered encoder, on both append paths.
TEST_F(ReservationTest, FramesEncodedInPlaceMatchTheBufferedEncoderOnDisk) {
  OpenWith(Config(2));
  const auto seq_of = [](core::QueueEntry entry, core::SequenceId seq) {
    entry.seq = seq;
    return entry;
  };
  std::vector<core::QueueEntry> zero{Write("small"), Write("large", kLarge)};
  std::vector<core::QueueEntry> one{Write("other", 300)};
  std::vector<std::byte> reserved_batch;
  frame::EncodeEntry(seq_of(zero[0], 0), 0, reserved_batch);
  frame::EncodeEntry(seq_of(zero[1], 1), 0, reserved_batch);
  frame::EncodeEntry(seq_of(one[0], 0), 1, reserved_batch);
  frame::CloseBatch(reserved_batch);
  auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = zero},
                                             ShardEntries{.shard = 1, .entries = one}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  queue_->Complete(std::move(*reserved));
  const LogPosition reserved_at =
      queue_->StreamForTesting(0).RingPositionForTesting(0).value_or(kNoPosition);
  ASSERT_NE(reserved_at, kNoPosition);
  ExpectOnDisk(reserved_at, reserved_batch);

  const std::vector<core::QueueEntry> appended{Write("a0"), Write("a1", kLarge)};
  std::vector<std::byte> appended_batch;
  frame::EncodeEntry(seq_of(appended[0], 1), 1, appended_batch);
  frame::EncodeEntry(seq_of(appended[1], 2), 1, appended_batch);
  frame::CloseBatch(appended_batch);
  ASSERT_TRUE(queue_->AppendBatch(1, appended).has_value());
  const LogPosition appended_at =
      queue_->StreamForTesting(1).RingPositionForTesting(1).value_or(kNoPosition);
  ASSERT_NE(appended_at, kNoPosition);
  ExpectOnDisk(appended_at, appended_batch);
  EXPECT_EQ(ReadAll(0).size(), 2U) << "every frame's sealed CRC verifies on read";
  EXPECT_EQ(ReadAll(1).size(), 3U);
}

// Admission is re-checked without waiting; a refusal takes no seq and
// moves from no entry.
TEST_F(ReservationTest, AWindowRefusalConsumesNothing) {
  auto config = Config(1);
  config.durability_window_bytes = 1;
  config.durability_window = 60s;
  OpenWith(config);
  auto stalled = std::make_shared<Latch>();
  const abyss::testing::OnExit release([stalled] { stalled->Open(); });
  queue_->SetFlushHookForTesting([stalled](uint32_t) -> core::Result<void> {
    stalled->Wait(30s);
    return {};
  });
  ASSERT_TRUE(queue_->Append(0, Write("first")).has_value());

  std::vector<core::QueueEntry> entries{Write("small"), Write("large", kLarge)};
  const std::array parts{ShardEntries{.shard = 0, .entries = entries}};
  auto refused = queue_->Reserve(parts);
  ASSERT_FALSE(refused.has_value());
  EXPECT_EQ(refused.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(queue_->TailSeq(0).value(), 0U);
  EXPECT_EQ(queue_->ReadyToComplete(0), 0U);
  EXPECT_EQ(ReservationsHeld(), 0U);
  EXPECT_EQ(ValueBytes(entries[1]), kLarge) << "a refused entry is not moved from";

  const auto start = std::chrono::steady_clock::now();
  auto waited = queue_->Admit(0, start + 30ms);
  ASSERT_FALSE(waited.has_value());
  EXPECT_EQ(waited.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 30ms);

  stalled->Open();
  ASSERT_TRUE(queue_->Admit(0, std::chrono::steady_clock::now() + 10s).has_value());
  auto reserved = queue_->Reserve(parts);
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  EXPECT_EQ(reserved->ranges()[0].first, 1U);
  queue_->Complete(std::move(*reserved));
  const auto read = ReadAll(0);
  ASSERT_EQ(read.size(), 3U);
  EXPECT_EQ(ValueBytes(read[2]), kLarge);
}

TEST_F(ReservationTest, NoSpareRefusesWithoutConsumingASeq) {
  auto config = Config(2);
  config.segment_size_bytes = kHeaderBlock + (std::size_t{64} << 10);
  OpenWith(config);
  queue_->PauseSegmentPreparerForTesting(0, true);
  const abyss::testing::OnExit resume([this] { queue_->PauseSegmentPreparerForTesting(0, false); });

  std::vector<core::QueueEntry> large{Write("large", kLarge)};
  std::vector<core::QueueEntry> small{Write("small")};
  const std::array parts{ShardEntries{.shard = 0, .entries = large},
                         ShardEntries{.shard = 1, .entries = small}};
  core::Result<void> refusal;
  core::SequenceId accepted = 0;
  for (int i = 0; i < 100; ++i) {
    large[0] = Write("large", kLarge);
    auto reserved = queue_->Reserve(parts);
    if (!reserved.has_value()) {
      refusal = std::unexpected(reserved.error());
      break;
    }
    queue_->Complete(std::move(*reserved));
    ++accepted;
  }
  ASSERT_FALSE(refusal.has_value()) << "the spares never ran out";
  EXPECT_EQ(refusal.error().code(), core::ErrorCode::kUnavailable) << refusal.error().message();
  for (core::ShardId shard = 0; shard < 2; ++shard) {
    EXPECT_EQ(queue_->TailSeq(shard).value(), accepted - 1) << shard;
    EXPECT_EQ(PublishedEnd(shard), accepted) << shard;
  }
  EXPECT_EQ(queue_->ReadyToComplete(0), 0U);
  EXPECT_EQ(ValueBytes(large[0]), kLarge) << "a refused entry is not moved from";

  EXPECT_FALSE(queue_->WaitForSpare(0, std::chrono::steady_clock::now() + 30ms));
  queue_->PauseSegmentPreparerForTesting(0, false);
  ASSERT_TRUE(queue_->WaitForSpare(0, std::chrono::steady_clock::now() + 10s));
  auto reserved = queue_->Reserve(parts);
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  EXPECT_EQ(reserved->ranges()[0].first, accepted);
  EXPECT_EQ(reserved->ranges()[1].first, accepted);
  queue_->Complete(std::move(*reserved));
  EXPECT_EQ(ReadAll(0).size(), accepted + 1);
  EXPECT_EQ(ReadAll(1).size(), accepted + 1);
}

TEST_F(ReservationTest, ShardsOnDifferentLogsAreCrossSlot) {
  auto config = Config(4);
  config.log_count = 2;
  OpenWith(config);
  std::vector<core::QueueEntry> a{Write("a")};
  std::vector<core::QueueEntry> b{Write("b")};
  auto refused = queue_->Reserve(
      std::array{ShardEntries{.shard = 0, .entries = a}, ShardEntries{.shard = 1, .entries = b}});
  ASSERT_FALSE(refused.has_value());
  EXPECT_EQ(refused.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_TRUE(refused.error().message().starts_with("CROSSSLOT")) << refused.error().message();
  EXPECT_EQ(queue_->ReadyToComplete(0), 0U);
  EXPECT_EQ(queue_->ReadyToComplete(1), 0U);

  auto reserved = queue_->Reserve(
      std::array{ShardEntries{.shard = 0, .entries = a}, ShardEntries{.shard = 2, .entries = b}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  EXPECT_EQ(reserved->ranges()[0].first, 0U) << "the refusal took no seq";
  queue_->Complete(std::move(*reserved));
}

TEST_F(ReservationTest, PartsMustBeSortedAndDistinct) {
  OpenWith(Config(4));
  std::vector<core::QueueEntry> a{Write("a")};
  std::vector<core::QueueEntry> b{Write("b")};
  for (const auto& parts :
       {std::array{ShardEntries{.shard = 2, .entries = a}, ShardEntries{.shard = 1, .entries = b}},
        std::array{ShardEntries{.shard = 1, .entries = a},
                   ShardEntries{.shard = 1, .entries = b}}}) {
    auto refused = queue_->Reserve(parts);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error().code(), core::ErrorCode::kInvalidArgument);
  }
}

// A frame past kLockHoldFrameBytes is encoded and filled in Complete;
// until then it holds back its shard's publish. Every frame carries the
// caller's appended_at.
TEST_F(ReservationTest, ALargeFrameIsFilledInComplete) {
  OpenWith(Config(1));
  std::vector<core::QueueEntry> entries{Write("small"), Write("large", kLarge)};
  ASSERT_LE(frame::EntryFrameSize(entries[0]), core::kLockHoldFrameBytes);
  ASSERT_GT(frame::EntryFrameSize(entries[1]), core::kLockHoldFrameBytes);
  auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = entries}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  const LogPosition large_at =
      queue_->StreamForTesting(0).RingPositionForTesting(1).value_or(kNoPosition);
  ASSERT_NE(large_at, kNoPosition);
  EXPECT_FALSE(HeaderAt(large_at).has_value()) << "filled before Complete";
  EXPECT_EQ(PublishedEnd(0), 0U);

  queue_->Complete(std::move(*reserved));
  EXPECT_TRUE(HeaderAt(large_at).has_value());
  const auto read = ReadAll(0);
  ASSERT_EQ(read.size(), 2U);
  EXPECT_EQ(ValueBytes(read[1]), kLarge);
  for (const auto& entry : read) EXPECT_EQ(entry.appended_at, kStamp) << KeyOf(entry);
}

// Two reservations on one shard, completed in reverse order, publish in
// seq order: the later one's Complete waits for the earlier one.
TEST_F(ReservationTest, ReservationsPublishInSeqOrderWhenCompletedInReverse) {
  OpenWith(Config(1));
  for (const std::size_t first_bytes : {kLarge, std::size_t{8}}) {
    SCOPED_TRACE(first_bytes);
    const core::SequenceId base = PublishedEnd(0);
    std::vector<core::QueueEntry> earlier{Write("earlier", first_bytes)};
    auto first = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = earlier}});
    ASSERT_TRUE(first.has_value()) << first.error().message();

    Latch reserved;
    auto later = std::async(std::launch::async, [&] {
      std::vector<core::QueueEntry> entries{Write("later")};
      auto second = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = entries}});
      reserved.Open();
      if (!second.has_value()) return core::SequenceId{0};
      const core::SequenceId seq = second->ranges()[0].first;
      queue_->Complete(std::move(*second));
      return seq;
    });
    ASSERT_TRUE(reserved.Wait());
    EXPECT_EQ(later.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(PublishedEnd(0), base);

    queue_->Complete(std::move(*first));
    ASSERT_EQ(later.wait_for(10s), std::future_status::ready);
    EXPECT_EQ(later.get(), base + 1);
    EXPECT_EQ(PublishedEnd(0), base + 2);
    const auto read = ReadAll(0);
    ASSERT_EQ(read.size(), base + 2);
    EXPECT_EQ(KeyOf(read[base]), "earlier");
    EXPECT_EQ(KeyOf(read[base + 1]), "later");
  }
}

// An append on the old path after a reservation still in flight waits
// for it before publishing, so the published end never passes a frame
// not yet filled or published.
TEST_F(ReservationTest, AnAppendAfterAnOpenReservationPublishesAfterIt) {
  OpenWith(Config(1));
  for (const std::size_t reserved_bytes : {std::size_t{8}, kLarge}) {
    SCOPED_TRACE(reserved_bytes);
    const core::SequenceId base = PublishedEnd(0);
    std::vector<core::QueueEntry> entries{Write("reserved", reserved_bytes)};
    auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = entries}});
    ASSERT_TRUE(reserved.has_value()) << reserved.error().message();

    auto appended = std::async(std::launch::async, [this] {
      auto result = queue_->Append(0, Write("appended"));
      return result.has_value() ? result->seq : core::SequenceId{0};
    });
    EXPECT_EQ(appended.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(PublishedEnd(0), base) << "the append published past the reservation";

    queue_->Complete(std::move(*reserved));
    ASSERT_EQ(appended.wait_for(10s), std::future_status::ready);
    EXPECT_EQ(appended.get(), base + 1);
    EXPECT_EQ(PublishedEnd(0), base + 2);
    const auto read = ReadAll(0);
    ASSERT_EQ(read.size(), base + 2);
    EXPECT_EQ(KeyOf(read[base]), "reserved");
    EXPECT_EQ(KeyOf(read[base + 1]), "appended");
  }
}

TEST_F(ReservationTest, PowerLossFuturesResolveOncePowerDurable) {
  auto config = Config(3);
  config.durability = kPower;
  OpenWith(config);
  std::vector<core::QueueEntry> a{Write("a")};
  std::vector<core::QueueEntry> c{Write("c"), Write("d")};
  auto reserved = queue_->Reserve(
      std::array{ShardEntries{.shard = 0, .entries = a}, ShardEntries{.shard = 2, .entries = c}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  DurableFutures durable = queue_->Complete(std::move(*reserved));
  ASSERT_EQ(durable.size(), 2U);
  for (ShardDurable& part : durable) {
    ASSERT_EQ(part.durable.wait_for(10s), std::future_status::ready) << part.shard;
    EXPECT_TRUE(part.durable.get().has_value());
  }
  EXPECT_EQ(queue_->DurableEnd(0, kPower).value(), 1U);
  EXPECT_EQ(queue_->DurableEnd(2, kPower).value(), 2U);
}

// The threadsafe style re-runs the test in a child, which opens its own
// queue in its own directory.
TEST_F(ReservationTest, DroppingAReservationIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        OpenWith(Config(1));
        std::vector<core::QueueEntry> entries{Write("dropped")};
        {
          auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = entries}});
        }
        std::this_thread::sleep_for(10s);
      },
      "reservation dropped without Complete");
}

#ifdef NDEBUG
TEST_F(ReservationTest, ReservingOrAppendingWhileHoldingAReservationIsADebugCheck) {
  GTEST_SKIP() << "ABYSS_DCHECK is compiled out";
}
#else
TEST_F(ReservationTest, ReservingOrAppendingWhileHoldingAReservationIsADebugCheck) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  const auto hold_then = [this](const auto& blocked) {
    OpenWith(Config(2));
    std::vector<core::QueueEntry> entries{Write("held")};
    auto held = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = entries}});
    if (held.has_value()) blocked();
    std::this_thread::sleep_for(10s);
  };
  EXPECT_DEATH(hold_then([this] {
                 std::vector<core::QueueEntry> entries{Write("again")};
                 const auto again =
                     queue_->Reserve(std::array{ShardEntries{.shard = 1, .entries = entries}});
               }),
               "WAL reservation by a thread holding one");
  EXPECT_DEATH(hold_then([this] { const auto appended = queue_->Append(1, Write("append")); }),
               "WAL append by a thread holding a reservation");
  EXPECT_DEATH(hold_then([this] {
                 const std::vector<core::QueueEntry> entries{Write("batch")};
                 const auto appended = queue_->AppendBatch(1, entries);
               }),
               "WAL append by a thread holding a reservation");
}

// The thread-local count of held reservations is the reserving
// thread's; completing elsewhere would corrupt both threads' counts.
TEST_F(ReservationTest, CompletingOnAnotherThreadIsADebugCheck) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        OpenWith(Config(1));
        std::vector<core::QueueEntry> entries{Write("moved")};
        auto held = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = entries}});
        if (held.has_value()) {
          std::thread other(
              [this, &held] { const auto durable = queue_->Complete(*std::move(held)); });
          other.join();
        }
        std::this_thread::sleep_for(10s);
      },
      "completed off the thread that reserved it");
}
#endif

// Old and new paths on shared shards, with Completes delayed: every
// shard's seqs stay contiguous, publish in order, and never show a
// frame before it is filled.
TEST_F(ReservationTest, MixedAppendersKeepEveryShardContiguousAndInOrder) {
  constexpr std::size_t kShards = 6;
  constexpr int kWorkers = 6;
  constexpr int kOps = 200;
  auto config = Config(kShards);
  config.durability = kPower;
  config.ring_entries = std::size_t{1} << 12;
  OpenWith(config);

  struct Placed {
    core::ShardId shard = 0;
    core::SequenceId seq = 0;
    std::string key;
  };
  std::mutex placed_mu;
  std::vector<Placed> placed;
  std::vector<std::future<core::Result<void>>> futures;
  std::atomic<bool> done{false};
  std::atomic<int> failures{0};
  const auto fail = [&failures](const std::string& what) {
    failures.fetch_add(1);
    ADD_FAILURE() << what;
  };

  auto monitor = std::async(std::launch::async, [&] {
    std::array<core::SequenceId, kShards> checked{};
    while (!done.load()) {
      for (core::ShardId shard = 0; shard < kShards; ++shard) {
        const core::SequenceId power = queue_->DurableEnd(shard, kPower).value();
        const core::SequenceId published = queue_->DurableEnd(shard, kProcess).value();
        if (power > published) fail("power end passed the published end");
        if (published < checked.at(shard)) fail("published end regressed");
        if (published == checked.at(shard)) continue;
        auto read =
            queue_->Read(shard, checked.at(shard), published - checked.at(shard), 0ms, kProcess);
        if (!read.has_value()) {
          fail("published read failed: " + read.error().message());
        } else if (read->size() != published - checked.at(shard)) {
          fail("published read came back short");
        } else {
          for (std::size_t i = 0; i < read->size(); ++i) {
            if ((*read)[i].seq != checked.at(shard) + i) fail("published read skipped a seq");
          }
        }
        checked.at(shard) = published;
      }
      std::this_thread::sleep_for(100us);
    }
  });

  const auto worker = [&](int id) {
    std::mt19937 rng(static_cast<uint32_t>(id) * 7919U);
    const auto value_bytes = [&rng] {
      return std::uniform_int_distribution<int>(0, 9)(rng) < 3
                 ? std::uniform_int_distribution<std::size_t>(17 << 10, 24 << 10)(rng)
                 : std::uniform_int_distribution<std::size_t>(1, 200)(rng);
    };
    std::vector<Placed> mine;
    std::vector<std::future<core::Result<void>>> pending;
    for (int op = 0; op < kOps && failures.load() == 0; ++op) {
      const std::string tag = "w" + std::to_string(id) + "-" + std::to_string(op) + "-";
      if (std::uniform_int_distribution<int>(0, 3)(rng) == 0) {
        const auto shard = std::uniform_int_distribution<core::ShardId>(0, kShards - 1)(rng);
        std::vector<core::QueueEntry> batch;
        const int count = std::uniform_int_distribution<int>(1, 3)(rng);
        batch.reserve(count);
        for (int i = 0; i < count; ++i) batch.push_back(Write(tag + std::to_string(i), 40));
        auto appended = queue_->AppendBatch(shard, batch);
        if (!appended.has_value()) {
          fail("append: " + appended.error().message());
          break;
        }
        for (int i = 0; i < count; ++i) {
          mine.push_back({shard, appended->first_seq + i, tag + std::to_string(i)});
        }
        pending.push_back(std::move(appended->durable));
        continue;
      }
      std::vector<core::ShardId> shards(kShards);
      for (core::ShardId s = 0; s < kShards; ++s) shards[s] = s;
      std::ranges::shuffle(shards, rng);
      shards.resize(std::uniform_int_distribution<std::size_t>(1, 3)(rng));
      std::ranges::sort(shards);
      std::vector<std::vector<core::QueueEntry>> entries(shards.size());
      std::vector<ShardEntries> parts;
      for (std::size_t p = 0; p < shards.size(); ++p) {
        const int count = std::uniform_int_distribution<int>(1, 3)(rng);
        for (int i = 0; i < count; ++i) {
          entries[p].push_back(
              Write(tag + std::to_string(p) + "." + std::to_string(i), value_bytes()));
        }
        parts.push_back({.shard = shards[p], .entries = entries[p]});
      }
      std::optional<Reservation> reserved;
      for (int attempt = 0; attempt < 1000 && !reserved.has_value(); ++attempt) {
        auto result = queue_->Reserve(parts);
        if (result.has_value()) {
          reserved.emplace(std::move(*result));
        } else if (result.error().code() == core::ErrorCode::kResourceExhausted) {
          const auto admitted = queue_->Admit(shards[0], std::chrono::steady_clock::now() + 5s);
          if (!admitted.has_value()) fail("admit: " + admitted.error().message());
        } else if (result.error().code() == core::ErrorCode::kUnavailable) {
          if (!queue_->WaitForSpare(shards[0], std::chrono::steady_clock::now() + 5s)) {
            fail("no spare segment");
          }
        } else {
          fail("reserve: " + result.error().message());
          break;
        }
      }
      if (!reserved.has_value()) {
        fail("reserve never succeeded");
        break;
      }
      for (std::size_t p = 0; p < parts.size(); ++p) {
        const ReservedRange& range = reserved->ranges()[p];
        for (core::SequenceId seq = range.first; seq <= range.last; ++seq) {
          mine.push_back({range.shard, seq,
                          tag + std::to_string(p) + "." + std::to_string(seq - range.first)});
        }
      }
      const auto delay = std::uniform_int_distribution<int>(0, 200)(rng);
      if (delay > 150) std::this_thread::sleep_for(std::chrono::microseconds(delay));
      for (ShardDurable& part : queue_->Complete(std::move(*reserved))) {
        pending.push_back(std::move(part.durable));
      }
    }
    const std::scoped_lock lock(placed_mu);
    std::ranges::move(mine, std::back_inserter(placed));
    std::ranges::move(pending, std::back_inserter(futures));
  };

  const auto start = std::chrono::steady_clock::now();
  std::vector<std::future<void>> workers;
  workers.reserve(kWorkers);
  for (int id = 0; id < kWorkers; ++id) {
    workers.push_back(std::async(std::launch::async, worker, id));
  }
  for (auto& running : workers) {
    ASSERT_EQ(running.wait_for(60s), std::future_status::ready) << "a worker hung";
  }
  done.store(true);
  ASSERT_EQ(monitor.wait_for(10s), std::future_status::ready);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 30s);
  ASSERT_EQ(failures.load(), 0);

  EXPECT_EQ(queue_->ReadyToComplete(0), 0U);
  for (auto& future : futures) {
    ASSERT_EQ(future.wait_for(10s), std::future_status::ready);
    EXPECT_TRUE(future.get().has_value());
  }
  std::array<std::vector<std::string>, kShards> keys;
  for (Placed& at : placed) {
    auto& shard_keys = keys.at(at.shard);
    if (shard_keys.size() <= at.seq) shard_keys.resize(at.seq + 1);
    EXPECT_TRUE(shard_keys[at.seq].empty())
        << "shard " << at.shard << " seq " << at.seq << " twice";
    shard_keys[at.seq] = std::move(at.key);
  }
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    SCOPED_TRACE(shard);
    const auto& expected = keys.at(shard);
    EXPECT_EQ(PublishedEnd(shard), expected.size());
    EXPECT_EQ(queue_->DurableEnd(shard, kPower).value(), expected.size());
    const auto read = ReadAll(shard);
    ASSERT_EQ(read.size(), expected.size());
    for (std::size_t seq = 0; seq < read.size(); ++seq) {
      ASSERT_FALSE(expected[seq].empty()) << "seq " << seq << " was never assigned";
      EXPECT_EQ(KeyOf(read[seq]), expected[seq]) << "seq " << seq;
    }
  }
}

}  // namespace
}  // namespace abyss::queue
