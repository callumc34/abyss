#include "abyss/queue/log.h"

#ifndef _WIN32
#include <sys/stat.h>
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/platform/fs.h"
#include "binary_io.h"
#include "crc32c.h"
#include "on_exit.h"
#include "segment_header_v2.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

namespace pfs = abyss::platform::fs;
using namespace std::chrono_literals;

constexpr std::size_t kSegmentBytes = std::size_t{64} << 10;
constexpr uint64_t kSpace = kSegmentBytes - kLogSegmentHeaderBytes;
constexpr uint32_t kShards = 4;
constexpr std::size_t kBodyAt = frame::kCommitBytes + frame::kCrcBytes;
// A Write frame for SET k <value> is 64 bytes plus the value.
constexpr std::size_t kWriteFrameOverhead = 64;

// One closed frame of exactly `size` bytes (a multiple of 8, >= 64).
std::vector<std::byte> Frame(core::ShardId shard, core::SequenceId seq, std::size_t size,
                             core::WallTime appended_at = core::WallClock::now()) {
  std::vector<std::byte> out;
  frame::EncodeEntry(
      core::QueueEntry{
          .seq = seq,
          .appended_at = appended_at,
          .payload =
              core::entry::Write{
                  .cmd = core::RespCommand{{"SET", "k",
                                            std::string(size - kWriteFrameOverhead, 'v')}}}},
      shard, out);
  frame::CloseBatch(out);
  EXPECT_EQ(out.size(), size);
  return out;
}

struct Batch {
  std::vector<std::byte> bytes;
  std::vector<std::size_t> offsets;
  std::vector<std::size_t> sizes;
};

Batch MakeBatch(core::ShardId shard, core::SequenceId first,
                const std::vector<std::size_t>& sizes) {
  Batch batch;
  for (std::size_t i = 0; i < sizes.size(); ++i) {
    batch.offsets.push_back(batch.bytes.size());
    batch.sizes.push_back(frame::EncodeEntry(
        core::QueueEntry{
            .seq = first + i,
            .payload =
                core::entry::Write{
                    .cmd = core::RespCommand{{"SET", "k",
                                              std::string(sizes[i] - kWriteFrameOverhead, 'b')}}}},
        shard, batch.bytes));
  }
  frame::CloseBatch(batch.bytes);
  return batch;
}

void CommitSlice(Log& log, const Log::Reservation& reservation, const Batch& batch, std::size_t i) {
  const std::size_t off = batch.offsets[i];
  const Log::Reservation slice{.pos = reservation.pos + off,
                               .size = static_cast<uint32_t>(batch.sizes[i]),
                               .gen = reservation.gen,
                               .salt = reservation.salt,
                               .dst = reservation.dst + off};
  log.Commit(slice, std::span(batch.bytes).subspan(off, batch.sizes[i]));
}

std::string Padded(uint64_t ordinal) {
  const std::string digits = std::to_string(ordinal);
  return std::string(20 - digits.size(), '0') + digits;
}

void PatchFile(const std::filesystem::path& path, uint64_t offset,
               std::span<const std::byte> bytes) {
  auto file = pfs::Open(path, {.mode = pfs::OpenMode::kReadWrite});
  ASSERT_TRUE(file.has_value()) << file.error().message();
  ASSERT_TRUE(pfs::Pwrite(*file, bytes.data(), bytes.size(), offset).has_value());
}

void FlipByte(const std::filesystem::path& path, uint64_t offset) {
  auto file = pfs::Open(path, {.mode = pfs::OpenMode::kReadWrite});
  ASSERT_TRUE(file.has_value()) << file.error().message();
  std::byte b{};
  ASSERT_TRUE(pfs::Pread(*file, &b, 1, offset).has_value());
  b ^= std::byte{0x01};
  ASSERT_TRUE(pfs::Pwrite(*file, &b, 1, offset).has_value());
}

uint64_t ReadWord(const std::filesystem::path& path, uint64_t offset) {
  auto file = pfs::Open(path, {.mode = pfs::OpenMode::kRead});
  EXPECT_TRUE(file.has_value());
  std::array<std::byte, 8> bytes{};
  if (file.has_value()) {
    EXPECT_TRUE(pfs::Pread(*file, bytes.data(), bytes.size(), offset));
  }
  return binary::LoadLE<uint64_t>(bytes.data());
}

constexpr std::size_t kAll = 1000;

std::optional<LogPosition> NextOf(const Log& log, LogPosition pos) {
  Log::Cursor cursor(log);
  auto next = cursor.Next(pos);
  EXPECT_TRUE(next.has_value()) << next.error().message();
  return next.value_or(std::nullopt);
}

template <typename Pred>
bool Eventually(Pred pred, std::chrono::milliseconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

class LogTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }

  LogConfig Config(uint32_t shards = kShards) const {
    return LogConfig{.dir = dir_.Path() / "log-0000",
                     .log_id = 3,
                     .shard_count = shards,
                     .segment_size_bytes = kSegmentBytes,
                     .durability_window_bytes = uint64_t{1} << 20};
  }

  std::unique_ptr<Log> OpenLog(std::vector<RecoveredFrame>* recovered = nullptr) {
    auto log = Log::Open(Config(), [recovered](const RecoveredFrame& f) {
      if (recovered != nullptr) recovered->push_back(f);
    });
    EXPECT_TRUE(log.has_value()) << log.error().message();
    return log.has_value() ? std::move(*log) : nullptr;
  }

  // Reserves and commits one frame, waiting for a spare if need be.
  static Log::Reservation Append(Log& log, core::ShardId shard, core::SequenceId seq,
                                 std::size_t size,
                                 core::WallTime appended_at = core::WallClock::now()) {
    const auto bytes = Frame(shard, seq, size, appended_at);
    auto reservation = log.Reserve(static_cast<uint32_t>(size));
    if (!reservation.has_value() && reservation.error().code() == core::ErrorCode::kUnavailable) {
      EXPECT_TRUE(log.WaitForSpare(std::chrono::steady_clock::now() + 5s));
      reservation = log.Reserve(static_cast<uint32_t>(size));
    }
    EXPECT_TRUE(reservation.has_value()) << reservation.error().message();
    if (!reservation.has_value()) return {};
    log.Commit(*reservation, bytes);
    return *reservation;
  }

  std::filesystem::path SegPath(uint64_t ordinal) const {
    return Config().dir / (Padded(ordinal) + ".seg");
  }

  std::vector<std::string> FilesWith(std::string_view part) const {
    std::vector<std::string> names;
    for (const auto& entry : std::filesystem::directory_iterator(Config().dir)) {
      const std::string name = entry.path().filename().string();
      if (name.contains(part)) names.push_back(name);
    }
    std::ranges::sort(names);
    return names;
  }

 private:
  testing::TempDir dir_{"log"};
};

TEST_F(LogTest, ReserveFitsAndCommitAdvancesThePrefix) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(log->FilledPrefix(), 0U);
  EXPECT_EQ(log->ReservedTail(), 0U);
  EXPECT_EQ(log->spare_count(), 2U);

  const auto a = Append(*log, 0, 0, 64);
  EXPECT_EQ(a.pos, 0U);
  EXPECT_EQ(a.gen, 0U);
  EXPECT_EQ(log->FilledPrefix(), 64U);
  const auto b = Append(*log, 1, 0, 128);
  EXPECT_EQ(b.pos, 64U);
  EXPECT_EQ(log->FilledPrefix(), 192U);
  EXPECT_EQ(log->ReservedTail(), 192U);

  auto ref = log->ReadFrame(64);
  ASSERT_TRUE(ref.has_value()) << ref.error().message();
  EXPECT_EQ(ref->view.header.shard, 1U);
  EXPECT_EQ(ref->view.size, 128U);
  auto entry = frame::DecodeEntry(ref->view);
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(std::get<core::entry::Write>(entry->payload).cmd.args[2].size(), 64U);
}

TEST_F(LogTest, RollPadsTheOldSegmentAndNextFrameSkipsThePadding) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  // Three 20000-byte frames leave 1440 bytes; the fourth rolls.
  for (core::SequenceId seq = 0; seq < 3; ++seq)
    EXPECT_EQ(Append(*log, 0, seq, 20000).pos, seq * 20000);
  const auto rolled = Append(*log, 0, 3, 20000);
  EXPECT_EQ(rolled.pos, kSpace);
  EXPECT_EQ(rolled.gen, 1U);
  EXPECT_EQ(log->FilledPrefix(), kSpace + 20000);

  auto pad = log->ReadFrame(60000);
  ASSERT_TRUE(pad.has_value()) << pad.error().message();
  EXPECT_EQ(pad->view.header.kind, frame::Kind::kPadding);
  EXPECT_EQ(pad->view.size, kSpace - 60000);
  EXPECT_EQ(NextOf(*log, 40000), std::optional<LogPosition>(kSpace));
  EXPECT_EQ(NextOf(*log, kSpace), std::nullopt);
  EXPECT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
}

TEST_F(LogTest, AFrameThatWouldLeaveAnUnpaddableRemainderRolls) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  Append(*log, 0, 0, kSpace - 1000);
  // 960 bytes would leave 40, too few for a padding frame.
  const auto rolled = Append(*log, 0, 1, 960);
  EXPECT_EQ(rolled.pos, kSpace);
  auto pad = log->ReadFrame(kSpace - 1000);
  ASSERT_TRUE(pad.has_value());
  EXPECT_EQ(pad->view.header.kind, frame::Kind::kPadding);
  EXPECT_EQ(pad->view.size, 1000U);
}

TEST_F(LogTest, ABatchCommitsThroughSlicesOfOneReservation) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  const Batch batch = MakeBatch(2, 10, {64, 128, 64});
  auto reservation = log->Reserve(static_cast<uint32_t>(batch.bytes.size()));
  ASSERT_TRUE(reservation.has_value());

  CommitSlice(*log, *reservation, batch, 0);
  EXPECT_EQ(log->FilledPrefix(), reservation->pos + 64);
  CommitSlice(*log, *reservation, batch, 2);
  EXPECT_EQ(log->FilledPrefix(), reservation->pos + 64);
  CommitSlice(*log, *reservation, batch, 1);
  EXPECT_EQ(log->FilledPrefix(), reservation->pos + batch.bytes.size());

  for (std::size_t i = 0; i < 3; ++i) {
    auto ref = log->ReadFrame(reservation->pos + batch.offsets[i]);
    ASSERT_TRUE(ref.has_value()) << ref.error().message();
    EXPECT_EQ(ref->view.header.seq, 10 + i);
    EXPECT_EQ(ref->view.header.batch_rest, batch.bytes.size() - batch.offsets[i]);
  }
}

TEST_F(LogTest, ABatchNeverStraddlesARoll) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  Append(*log, 0, 0, 30000);
  Append(*log, 0, 1, 30000);
  const Batch batch = MakeBatch(1, 0, {512, 512, 512});
  ASSERT_GT(batch.bytes.size(), kSpace - 60000);
  auto reservation = log->Reserve(static_cast<uint32_t>(batch.bytes.size()));
  ASSERT_TRUE(reservation.has_value());
  EXPECT_EQ(reservation->pos, kSpace);
  for (std::size_t i = 0; i < 3; ++i) CommitSlice(*log, *reservation, batch, i);

  auto pad = log->ReadFrame(60000);
  ASSERT_TRUE(pad.has_value());
  EXPECT_EQ(pad->view.header.kind, frame::Kind::kPadding);
  EXPECT_EQ(pad->view.size, kSpace - 60000);
  EXPECT_EQ(NextOf(*log, 30000), std::optional<LogPosition>(kSpace));
  const LogPosition batch_end = kSpace + batch.bytes.size();
  for (std::size_t i = 0; i < 3; ++i) {
    const LogPosition pos = kSpace + batch.offsets[i];
    auto ref = log->ReadFrame(pos);
    ASSERT_TRUE(ref.has_value());
    EXPECT_EQ(pos + ref->view.header.batch_rest, batch_end);
  }
}

TEST_F(LogTest, NoSpareIsUnavailableUntilThePreparerResumes) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  log->PausePreparerForTesting();
  // Segments 0-2 are published: nine frames use both spares.
  for (core::SequenceId seq = 0; seq < 9; ++seq) Append(*log, 0, seq, 20000);
  EXPECT_EQ(log->spare_count(), 0U);
  const LogPosition tail = log->ReservedTail();
  const LogPosition filled = log->FilledPrefix();

  auto reservation = log->Reserve(20000);
  ASSERT_FALSE(reservation.has_value());
  EXPECT_EQ(reservation.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_EQ(log->ReservedTail(), tail);
  EXPECT_EQ(log->FilledPrefix(), filled);
  EXPECT_FALSE(log->WaitForSpare(std::chrono::steady_clock::now() + 50ms));

  log->ResumePreparerForTesting();
  EXPECT_TRUE(log->WaitForSpare(std::chrono::steady_clock::now() + 5s));
  reservation = log->Reserve(20000);
  ASSERT_TRUE(reservation.has_value()) << reservation.error().message();
  EXPECT_EQ(reservation->pos, 3 * kSpace);
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalSpareWaitsTotal),
            std::optional<double>(2));
}

TEST_F(LogTest, AnOversizedFrameIsRejected) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  auto too_big = log->Reserve(static_cast<uint32_t>(kSpace + 8));
  ASSERT_FALSE(too_big.has_value());
  EXPECT_EQ(too_big.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_NE(too_big.error().message().find("segment"), std::string::npos);
  auto unpaddable = log->Reserve(static_cast<uint32_t>(kSpace - 40));
  ASSERT_FALSE(unpaddable.has_value());
  EXPECT_EQ(unpaddable.error().code(), core::ErrorCode::kResourceExhausted);
  auto misaligned = log->Reserve(100);
  ASSERT_FALSE(misaligned.has_value());
  EXPECT_EQ(misaligned.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_EQ(log->ReservedTail(), 0U);
  EXPECT_TRUE(log->Reserve(static_cast<uint32_t>(kSpace)).has_value());
}

TEST_F(LogTest, TheFilledPrefixStopsAtAHole) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  const auto a_bytes = Frame(0, 0, 64);
  const auto b_bytes = Frame(1, 0, 64);
  auto a = log->Reserve(64);
  auto b = log->Reserve(64);
  ASSERT_TRUE(a.has_value() && b.has_value());
  log->Commit(*b, b_bytes);
  EXPECT_EQ(log->FilledPrefix(), a->pos);
  log->Commit(*a, a_bytes);
  EXPECT_EQ(log->FilledPrefix(), b->pos + 64);
}

TEST_F(LogTest, FlushReportsEachNewlyDurableEntryOnceInOrder) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  for (core::SequenceId seq = 0; seq < 5; ++seq)
    Append(*log, static_cast<core::ShardId>(seq % 2), seq / 2, 64);

  std::vector<std::pair<core::ShardId, core::SequenceId>> seen;
  auto flushed = log->Flush([&](const frame::Header& header, uint32_t size) {
    EXPECT_EQ(log->DurablePrefix(), 0U) << "published only after the walk";
    EXPECT_EQ(size, 64U);
    seen.emplace_back(header.shard, header.seq);
  });
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(flushed->from, 0U);
  EXPECT_EQ(flushed->to, 320U);
  EXPECT_EQ(flushed->entries, 5U);
  EXPECT_EQ(flushed->entry_bytes, 320U);
  EXPECT_EQ(log->DurablePrefix(), 320U);
  const std::vector<std::pair<core::ShardId, core::SequenceId>> expected{
      {0, 0}, {1, 0}, {0, 1}, {1, 1}, {0, 2}};
  EXPECT_EQ(seen, expected);

  auto again = log->Flush([&](const frame::Header&, uint32_t) { ADD_FAILURE(); });
  ASSERT_TRUE(again.has_value());
  EXPECT_EQ(again->from, again->to);
  EXPECT_EQ(again->entries, 0U);

  // A roll's padding is durable too, but is not an entry.
  Append(*log, 2, 0, kSpace - 320 - 1000);
  Append(*log, 2, 1, 2000);
  seen.clear();
  flushed = log->Flush(
      [&](const frame::Header& header, uint32_t) { seen.emplace_back(header.shard, header.seq); });
  ASSERT_TRUE(flushed.has_value());
  EXPECT_EQ(seen, (std::vector<std::pair<core::ShardId, core::SequenceId>>{{2, 0}, {2, 1}}));
  EXPECT_EQ(flushed->entries, 2U);
  EXPECT_EQ(flushed->entry_bytes, kSpace - 320 - 1000 + 2000);
  EXPECT_EQ(flushed->to - flushed->from, kSpace - 320 + 2000);
}

TEST_F(LogTest, FlushSyncsEverySegmentItCovers) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  const uint64_t before = log->SyncCountForTesting();
  Append(*log, 0, 0, 64);
  ASSERT_TRUE(log->Flush({}).has_value());
  EXPECT_EQ(log->SyncCountForTesting(), before + 1);
  Append(*log, 0, 1, 40000);
  Append(*log, 0, 2, 40000);
  ASSERT_TRUE(log->Flush({}).has_value());
  EXPECT_EQ(log->SyncCountForTesting(), before + 3);
}

TEST_F(LogTest, FlushReturnsAnInjectedSyncError) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  Append(*log, 0, 0, 64);
  log->InjectSyncErrorForTesting(core::Error{core::ErrorCode::kInternal, "injected sync failure"});
  auto failed = log->Flush({});
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().message(), "injected sync failure");
  EXPECT_EQ(log->DurablePrefix(), 0U);
  auto flushed = log->Flush({});
  ASSERT_TRUE(flushed.has_value());
  EXPECT_EQ(log->DurablePrefix(), 64U);
}

TEST_F(LogTest, ACursorWalksAcrossSegments) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  for (core::SequenceId seq = 0; seq < 10; ++seq) Append(*log, 1, seq, 20000);

  Log::Cursor cursor(*log);
  std::vector<core::SequenceId> seqs;
  std::optional<LogPosition> pos = 0;
  while (pos.has_value()) {
    auto view = cursor.Read(*pos);
    ASSERT_TRUE(view.has_value()) << view.error().message();
    ASSERT_EQ(view->header.kind, frame::Kind::kEntry);
    auto peeked = cursor.Peek(*pos);
    ASSERT_TRUE(peeked.has_value());
    EXPECT_EQ(peeked->header.seq, view->header.seq);
    auto entry = frame::DecodeEntry(*view);
    ASSERT_TRUE(entry.has_value());
    seqs.push_back(entry->seq);
    auto next = cursor.Next(*pos);
    ASSERT_TRUE(next.has_value()) << next.error().message();
    pos = *next;
  }
  EXPECT_EQ(seqs, (std::vector<core::SequenceId>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  auto past = cursor.Read(log->FilledPrefix());
  ASSERT_FALSE(past.has_value());
  EXPECT_EQ(past.error().code(), core::ErrorCode::kOutOfRange);
  EXPECT_FALSE(cursor.Next(log->FilledPrefix()).has_value());
}

TEST_F(LogTest, ACursorReportsReclaimAndDamageAsErrors) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  for (core::SequenceId seq = 0; seq < 4; ++seq) Append(*log, 0, seq, 20000);
  ASSERT_TRUE(log->Flush({}).has_value());
  ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));

  Log::Cursor held(*log);
  ASSERT_TRUE(held.Read(0).has_value());
  ASSERT_TRUE(log->Reclaim(0).has_value());
  EXPECT_TRUE(held.Read(20000).has_value()) << "the segment it is on stays readable";
  Log::Cursor fresh(*log);
  auto reclaimed = fresh.Next(0);
  ASSERT_FALSE(reclaimed.has_value());
  EXPECT_EQ(reclaimed.error().code(), core::ErrorCode::kOutOfRange);
  EXPECT_EQ(fresh.Read(0).error().code(), core::ErrorCode::kOutOfRange);

  // Media damage below the filled prefix fails the verified read only.
  auto bytes = Frame(0, 4, 64);
  const auto reservation = Append(*log, 0, 4, 64);
  FlipByte(SegPath(1), kLogSegmentHeaderBytes + (reservation.pos - kSpace) + kBodyAt + 40);
  Log::Cursor cursor(*log);
  EXPECT_TRUE(cursor.Peek(reservation.pos).has_value());
  auto torn = cursor.Read(reservation.pos);
  ASSERT_FALSE(torn.has_value());
  EXPECT_EQ(torn.error().code(), core::ErrorCode::kCorruption);

  bytes[kBodyAt] = std::byte{9};
  frame::CloseBatch(bytes);
  auto unknown = log->Reserve(64);
  ASSERT_TRUE(unknown.has_value());
  log->Commit(*unknown, bytes);
  auto stepped = cursor.Next(reservation.pos);
  ASSERT_FALSE(stepped.has_value());
  EXPECT_EQ(stepped.error().code(), core::ErrorCode::kCorruption);
  EXPECT_EQ(cursor.Peek(unknown->pos).error().code(), core::ErrorCode::kCorruption);
}

TEST_F(LogTest, SealedSegmentsCarryTheirShardRanges) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  Append(*log, 0, 5, 20000);
  Append(*log, 2, 9, 20000);
  Append(*log, 0, 6, 20000);
  Append(*log, 0, 7, 20000);
  EXPECT_TRUE(log->SealedSegments(kAll).empty());
  ASSERT_TRUE(log->Flush({}).has_value());
  const auto sealed = log->SealedSegments(kAll);
  ASSERT_EQ(sealed.size(), 1U);
  EXPECT_TRUE(log->SealedSegments(0).empty());
  EXPECT_EQ(sealed[0].ordinal, 0U);
  ASSERT_EQ(sealed[0].shards.size(), 2U);
  EXPECT_EQ(sealed[0].shards[0].shard, 0U);
  EXPECT_EQ(sealed[0].shards[0].min_seq, 5U);
  EXPECT_EQ(sealed[0].shards[0].max_seq, 6U);
  EXPECT_EQ(sealed[0].shards[1].shard, 2U);
  EXPECT_EQ(sealed[0].shards[1].min_seq, 9U);
}

// Retention ages a segment from its seal: a spare prepared long before
// it took frames is stamped when the flush passes its end, and stamps
// never go back with the clock.
TEST_F(LogTest, ASegmentIsStampedWhenSealedNotWhenPrepared) {
  auto seconds = std::make_shared<std::atomic<int64_t>>(1000);
  auto config = Config();
  config.wall_clock = [seconds] { return core::WallTime(std::chrono::seconds(seconds->load())); };
  auto opened = Log::Open(config, nullptr);
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  Log& log = **opened;
  const auto at = [](int64_t s) { return core::WallTime(std::chrono::seconds(s)); };

  // Idle for ten days with segments 0, 1 and 2 already prepared.
  constexpr int64_t kDay = 86400;
  seconds->store(1000 + (10 * kDay));
  // Three frames fill a segment; the next one rolls and lets it seal.
  core::SequenceId seq = 0;
  const auto append_and_flush = [&](int frames) {
    for (int i = 0; i < frames; ++i) Append(log, 0, seq++, 20000);
    ASSERT_TRUE(log.Flush({}).has_value());
  };
  append_and_flush(4);
  seconds->store(1000 + (11 * kDay));
  append_and_flush(3);
  seconds->store(1000);
  append_and_flush(3);

  const auto sealed = log.SealedSegments(kAll);
  ASSERT_GE(sealed.size(), 3U);
  EXPECT_EQ(sealed[0].sealed_at, at(1000 + (10 * kDay)));
  EXPECT_EQ(sealed[1].sealed_at, at(1000 + (11 * kDay)));
  EXPECT_EQ(sealed[2].sealed_at, at(1000 + (11 * kDay))) << "a seal stamp went back";
}

// A reopened segment is stamped with its newest entry's append time,
// wherever in the segment that entry sits.
TEST_F(LogTest, ARecoveredSegmentIsStampedWithItsNewestEntry) {
  const auto at = [](int64_t s) { return core::WallTime(std::chrono::seconds(s)); };
  std::map<uint64_t, core::WallTime> newest;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    const std::vector<int64_t> times = {5000, 8000, 6000, 7000, 9000, 6500, 9500};
    for (std::size_t i = 0; i < times.size(); ++i) {
      const auto reserved = Append(*log, 0, i, 20000, at(times[i]));
      auto& stamp = newest[reserved.pos / kSpace];
      stamp = std::max(stamp, at(times[i]));
    }
    log->AwaitFilled(log->ReservedTail());
  }
  ASSERT_GE(newest.size(), 3U);
  EXPECT_EQ(newest[0], at(8000));

  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  const auto sealed = log->SealedSegments(kAll);
  ASSERT_EQ(sealed.size(), newest.size());
  for (const auto& segment : sealed) {
    EXPECT_EQ(segment.sealed_at, newest[segment.ordinal]) << "segment " << segment.ordinal;
  }
}

TEST_F(LogTest, ReclaimWaitsForReadersBeforeRecycling) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  for (core::SequenceId seq = 0; seq < 4; ++seq) Append(*log, 0, seq, 20000);
  ASSERT_TRUE(log->Flush({}).has_value());
  ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));

  auto ref = log->ReadFrame(0);
  ASSERT_TRUE(ref.has_value());
  ASSERT_TRUE(log->Reclaim(0).has_value());
  EXPECT_TRUE(log->SealedSegments(kAll).empty());
  auto gone = log->ReadFrame(0);
  ASSERT_FALSE(gone.has_value());
  EXPECT_EQ(gone.error().code(), core::ErrorCode::kOutOfRange);
  EXPECT_EQ(log->free_count(), 0U);
  EXPECT_TRUE(std::filesystem::exists(SegPath(0)));
  EXPECT_EQ(ref->view.header.seq, 0U) << "the held view stays readable";

  ref->hold.reset();
  EXPECT_TRUE(Eventually([&] { return log->free_count() == 1; }));
  EXPECT_FALSE(std::filesystem::exists(SegPath(0)));
  EXPECT_EQ(FilesWith("free-").size(), 1U);
}

TEST_F(LogTest, AFullPoolUnlinks) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  for (core::SequenceId seq = 0; seq < 10; ++seq) Append(*log, 0, seq, 20000);
  ASSERT_TRUE(log->Flush({}).has_value());
  ASSERT_EQ(log->SealedSegments(kAll).size(), 3U);
  ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
  log->PausePreparerForTesting();

  for (uint64_t ordinal = 0; ordinal < 3; ++ordinal) {
    ASSERT_TRUE(log->Reclaim(ordinal).has_value()) << ordinal;
  }
  EXPECT_EQ(log->free_count(), 2U);
  EXPECT_EQ(FilesWith("free-").size(), 2U);
  for (uint64_t ordinal = 0; ordinal < 3; ++ordinal)
    EXPECT_FALSE(std::filesystem::exists(SegPath(ordinal)));
}

TEST_F(LogTest, ReclaimedFilesLeaveTheDiskOldestFirst) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  for (core::SequenceId seq = 0; seq < 7; ++seq) Append(*log, 0, seq, 20000);
  ASSERT_TRUE(log->Flush({}).has_value());
  ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
  EXPECT_EQ(log->SealedSegments(1).size(), 1U);

  auto ref = log->ReadFrame(0);
  ASSERT_TRUE(ref.has_value());
  ASSERT_TRUE(log->Reclaim(0).has_value());
  ASSERT_TRUE(log->Reclaim(1).has_value());
  // Removing segment 1 first would leave a gap below it on disk.
  EXPECT_TRUE(std::filesystem::exists(SegPath(0)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  ref->hold.reset();
  EXPECT_TRUE(Eventually([&] {
    return !std::filesystem::exists(SegPath(0)) && !std::filesystem::exists(SegPath(1));
  }));
}

#ifndef _WIN32
using Listing = std::map<std::string, ino_t>;

// A log directory's files by name, with each one's inode.
Listing ListDir(const std::filesystem::path& dir) {
  Listing files;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    struct stat st{};
    if (::stat(entry.path().c_str(), &st) == 0) files[entry.path().filename().string()] = st.st_ino;
  }
  return files;
}

// Segment files named in `before` but not in `after`.
size_t SegmentsGone(const Listing& before, const Listing& after) {
  size_t gone = 0;
  for (const auto& [name, inode] : before) {
    if (name.ends_with(".seg") && !name.starts_with("free-") && !after.contains(name)) ++gone;
  }
  return gone;
}

// A power loss that keeps the last synced listing and loses the oldest
// segment removal or move made since, if any: that file returns under
// its old name.
void LoseOldestUnsyncedMove(const std::filesystem::path& dir, const Listing& last_synced,
                            const Listing& now) {
  for (const auto& [name, inode] : last_synced) {
    if (!name.ends_with(".seg") || name.starts_with("free-") || now.contains(name)) continue;
    for (const auto& [now_named, same] : now) {
      if (same != inode) continue;
      std::filesystem::rename(dir / now_named, dir / name);
      return;
    }
  }
}

// A power loss may lose any subset of the directory changes made since
// the last directory sync (ALICE). Reclaim renames or unlinks old
// segments; were two removals unsynced and only the older one lost, it
// would return below a gap and recovery would refuse the log. Between
// syncs the log removes at most one segment, oldest first, so whatever
// is lost leaves the files contiguous.
TEST_F(LogTest, LosingAnyUnsyncedReclaimLeavesTheLogOpenable) {
  std::mutex mu;
  std::vector<Listing> synced;
  Log::SetDirSyncHookForTesting([&](const std::filesystem::path& dir) -> core::Result<void> {
    const std::scoped_lock lock(mu);
    synced.push_back(ListDir(dir));
    return {};
  });
  const testing::OnExit unhook([] { Log::SetDirSyncHookForTesting(nullptr); });
  Listing at_crash;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 7; ++seq) Append(*log, 0, seq, 20000);
    ASSERT_TRUE(log->Flush({}).has_value());
    ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
    ASSERT_TRUE(log->Reclaim(0).has_value());
    ASSERT_TRUE(log->Reclaim(1).has_value());
    ASSERT_TRUE(Eventually([&] {
      return !std::filesystem::exists(SegPath(0)) && !std::filesystem::exists(SegPath(1));
    }));
    const std::scoped_lock lock(mu);
    at_crash = ListDir(Config().dir);
    for (size_t i = 1; i < synced.size(); ++i) {
      EXPECT_LE(SegmentsGone(synced[i - 1], synced[i]), 1U)
          << "two segments removed between directory syncs";
    }
  }
  Log::SetDirSyncHookForTesting(nullptr);
  LoseOldestUnsyncedMove(Config().dir, synced.back(), at_crash);
  std::vector<RecoveredFrame> recovered;
  auto reopened = OpenLog(&recovered);
  ASSERT_NE(reopened, nullptr) << "a power loss during reclaim left the log unopenable";
}

// Recovery moves each segment past the recovered end to the free pool,
// highest first, each move synced before the next. A power loss at any
// sync of it, losing the move that sync covered, still leaves a log
// that opens with every frame.
TEST_F(LogTest, APowerLossAtAnyRecoveryMoveLeavesTheLogOpenable) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 4; ++seq) Append(*log, 0, seq, 20000);
    ASSERT_TRUE(log->Flush({}).has_value());
    // Spares past the end, for recovery to move.
    ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
  }
  for (int fail_at = 0;; ++fail_at) {
    SCOPED_TRACE("the power fails at directory sync " + std::to_string(fail_at));
    std::mutex mu;
    std::vector<Listing> synced{ListDir(Config().dir)};
    int syncs = 0;
    Log::SetDirSyncHookForTesting([&](const std::filesystem::path& dir) -> core::Result<void> {
      const std::scoped_lock lock(mu);
      if (syncs++ == fail_at) {
        return std::unexpected(core::Error{core::ErrorCode::kInternal, "injected: power fails"});
      }
      synced.push_back(ListDir(dir));
      return {};
    });
    const testing::OnExit unhook([] { Log::SetDirSyncHookForTesting(nullptr); });
    auto crashed = Log::Open(Config(), [](const RecoveredFrame&) {});
    Log::SetDirSyncHookForTesting(nullptr);
    if (crashed.has_value()) {
      // Past the last sync: nothing left to fail.
      ASSERT_GT(fail_at, 0);
      break;
    }
    for (size_t i = 1; i < synced.size(); ++i) {
      EXPECT_LE(SegmentsGone(synced[i - 1], synced[i]), 1U) << "two moves between syncs";
    }
    LoseOldestUnsyncedMove(Config().dir, synced.back(), ListDir(Config().dir));
    std::vector<RecoveredFrame> recovered;
    auto reopened = OpenLog(&recovered);
    ASSERT_NE(reopened, nullptr) << "a power loss during recovery left the log unopenable";
    EXPECT_EQ(recovered.size(), 4U);
    // The next open has the same spares to move.
    ASSERT_TRUE(Eventually([&] { return reopened->spare_count() == 2; }));
    ASSERT_LT(fail_at, 20) << "recovery never stopped syncing";
  }
}

// A directory sync that fails after a reclaim is fatal, as a failed
// flush is: a retry of the sync could report success for a removal the
// device dropped. The preparer thread cannot unwind a throwing fatal
// capture, so this runs in a child process.
TEST_F(LogTest, AReclaimDirectorySyncFailureIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        auto log = OpenLog();
        for (core::SequenceId seq = 0; seq < 7; ++seq) Append(*log, 0, seq, 20000);
        ASSERT_TRUE(log->Flush({}).has_value());
        ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
        Log::SetDirSyncHookForTesting([](const std::filesystem::path&) -> core::Result<void> {
          return std::unexpected(core::Error{core::ErrorCode::kInternal, "injected EIO"});
        });
        ASSERT_TRUE(log->Reclaim(0).has_value());
        std::this_thread::sleep_for(10s);
      },
      "WAL directory sync failed after reclaiming .*: injected EIO");
}
#endif

#ifdef __APPLE__
// Recovery syncs what it adopts before it serves anything: a sync that
// fails there fails the open, whatever it would have recovered, and a
// later open with a healthy device recovers it all.
TEST_F(LogTest, ASyncFailureDuringRecoveryFailsTheOpen) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 4; ++seq) Append(*log, 0, seq, 4096);
    ASSERT_TRUE(log->Flush({}).has_value());
  }
  platform::fs::testing::SetFullFsyncForTesting([](int) {
    errno = EIO;
    return -1;
  });
  auto failed = Log::Open(Config(), [](const RecoveredFrame&) {});
  platform::fs::testing::SetFullFsyncForTesting(nullptr);
  ASSERT_FALSE(failed.has_value()) << "an open whose sync failed served the log";

  std::vector<RecoveredFrame> recovered;
  auto reopened = OpenLog(&recovered);
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(recovered.size(), 4U);
}
#endif

TEST_F(LogTest, AFileThatCannotBeRemovedHoldsBackLaterReclaims) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 10; ++seq) Append(*log, 0, seq, 20000);
    ASSERT_TRUE(log->Flush({}).has_value());
    ASSERT_TRUE(Eventually([&] { return log->spare_count() == 2; }));
    log->PausePreparerForTesting();
    log->InjectRemoveErrorForTesting(
        core::Error{core::ErrorCode::kInternal, "injected remove failure"});

    auto failed = log->Reclaim(0);
    ASSERT_FALSE(failed.has_value());
    EXPECT_NE(failed.error().message().find("injected remove failure"), std::string::npos);
    EXPECT_TRUE(std::filesystem::exists(SegPath(0)));
    auto refused = log->Reclaim(1);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error().code(), core::ErrorCode::kUnavailable);
    EXPECT_NE(refused.error().message().find(Padded(0) + ".seg"), std::string::npos)
        << refused.error().message();

    log->ResumePreparerForTesting();
    EXPECT_TRUE(Eventually([&] { return log->Reclaim(1).has_value(); }));
    EXPECT_FALSE(std::filesystem::exists(SegPath(0)));
  }
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 4U);
  EXPECT_EQ(recovered.front().header.seq, 6U);
}

TEST_F(LogTest, ReclaimTakesOnlyTheOldestSealedSegment) {
  auto log = OpenLog();
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(log->Reclaim(9).error().code(), core::ErrorCode::kNotFound);
  for (core::SequenceId seq = 0; seq < 7; ++seq) Append(*log, 0, seq, 20000);
  ASSERT_TRUE(log->Flush({}).has_value());
  ASSERT_EQ(log->SealedSegments(kAll).size(), 2U);
  EXPECT_EQ(log->Reclaim(1).error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_EQ(log->Reclaim(2).error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_TRUE(log->Reclaim(0).has_value());
  EXPECT_TRUE(log->Reclaim(1).has_value());
}

TEST_F(LogTest, ACleanReopenReportsEveryEntryInOrder) {
  std::vector<Log::Reservation> written;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 7; ++seq) {
      written.push_back(Append(*log, static_cast<core::ShardId>(seq % 3), seq / 3, 20000));
    }
    // The tail stays unflushed: a process crash keeps it in the page cache.
    ASSERT_TRUE(log->Flush({}).has_value());
    Append(*log, 1, 2, 64);
  }
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 8U);
  for (std::size_t i = 0; i < 7; ++i) {
    EXPECT_EQ(recovered[i].pos, written[i].pos);
    EXPECT_EQ(recovered[i].size, 20000U);
    EXPECT_EQ(recovered[i].ordinal, written[i].pos / kSpace);
    EXPECT_EQ(recovered[i].header.shard, i % 3);
    EXPECT_EQ(recovered[i].header.seq, i / 3);
  }
  EXPECT_EQ(recovered[7].header.shard, 1U);
  EXPECT_EQ(recovered[7].header.seq, 2U);
  // Segment 2 held the tail; appends continue in segment 3.
  EXPECT_EQ(log->FilledPrefix(), 3 * kSpace);
  EXPECT_EQ(log->DurablePrefix(), 3 * kSpace);
  EXPECT_EQ(log->ReservedTail(), 3 * kSpace);
  EXPECT_EQ(log->spare_count(), 2U);
  const auto sealed = log->SealedSegments(kAll);
  ASSERT_EQ(sealed.size(), 3U);
  EXPECT_EQ(sealed[2].shards.size(), 2U);
  EXPECT_EQ(Append(*log, 0, 3, 64).pos, 3 * kSpace);

  log.reset();
  recovered.clear();
  log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(recovered.size(), 9U);
  EXPECT_EQ(log->FilledPrefix(), 4 * kSpace);
}

TEST_F(LogTest, ATornTailIsCut) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 3; ++seq) Append(*log, 0, seq, 1000);
  }
  FlipByte(SegPath(0), kLogSegmentHeaderBytes + 2000 + 100);
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 2U);
  EXPECT_EQ(recovered[1].header.seq, 1U);
  EXPECT_EQ(log->FilledPrefix(), kSpace);
  auto pad = log->ReadFrame(2000);
  ASSERT_TRUE(pad.has_value()) << pad.error().message();
  EXPECT_EQ(pad->view.header.kind, frame::Kind::kPadding);
}

TEST_F(LogTest, ATearAcrossARollEndsTheLogAtTheTear) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 5; ++seq) Append(*log, 0, seq, 20000);
    ASSERT_TRUE(log->Flush({}).has_value());
  }
  FlipByte(SegPath(0), kLogSegmentHeaderBytes + 40000 + 100);
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 2U);
  EXPECT_EQ(log->FilledPrefix(), kSpace);
  auto pad = log->ReadFrame(40000);
  ASSERT_TRUE(pad.has_value());
  EXPECT_EQ(pad->view.header.kind, frame::Kind::kPadding);
  EXPECT_EQ(NextOf(*log, 20000), std::nullopt);
}

// Segment 0 is filled with three equal frames, reclaimed and recycled
// as segment 4, so stale gen-0 frames sit where new frames will go.
class RecycledLogTest : public LogTest {
 protected:
  static constexpr std::size_t kThird = kSpace / 3;

  void BuildRecycled(Log& log) {
    for (core::SequenceId seq = 0; seq < 3; ++seq) Append(log, 0, seq, kThird);
    Append(log, 0, 3, kThird);
    ASSERT_TRUE(log.Flush({}).has_value());
    ASSERT_TRUE(Eventually([&] { return log.spare_count() == 2; }));
    ASSERT_TRUE(log.Reclaim(0).has_value());
    ASSERT_EQ(log.free_count(), 1U);
    // Rolling into segment 2 makes the preparer recycle it as segment 4.
    for (core::SequenceId seq = 4; seq < 12; ++seq) Append(log, 0, seq, kThird);
    ASSERT_TRUE(Eventually([&] { return log.spare_count() == 2 && log.free_count() == 0; }));
    ASSERT_EQ(log.ReservedTail(), 4 * kSpace);
    const uint64_t stale = ReadWord(SegPath(4), kLogSegmentHeaderBytes + kThird);
    ASSERT_EQ(frame::CommitGen(stale), 0U) << "segment 4 is the recycled segment 0";
    ASSERT_NE(frame::CommitLen(stale), 0U);
  }
};

TEST_F(RecycledLogTest, StaleFramesOfARecycledSegmentReadAsUnfilled) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    BuildRecycled(*log);
    if (HasFatalFailure()) return;
    Append(*log, 0, 12, 64);
    EXPECT_EQ(NextOf(*log, 4 * kSpace), std::nullopt);
    ASSERT_TRUE(log->Flush({}).has_value());
  }
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 10U);
  EXPECT_EQ(recovered.front().header.seq, 3U);
  EXPECT_EQ(recovered.back().header.seq, 12U);
  EXPECT_EQ(recovered.back().pos, 4 * kSpace);
  EXPECT_EQ(log->FilledPrefix(), 5 * kSpace);
}

TEST_F(RecycledLogTest, ANewCommitWordOverAStaleFrameIsACleanTornTail) {
  uint32_t stale_len = 0;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    BuildRecycled(*log);
    if (HasFatalFailure()) return;
    stale_len = frame::CommitLen(ReadWord(SegPath(4), kLogSegmentHeaderBytes));
    ASSERT_EQ(stale_len, frame::CommitLen(binary::LoadLE<uint64_t>(Frame(0, 12, kThird).data())));
  }
  // Only the sector holding the new frame's commit word reached disk.
  std::array<std::byte, 8> word{};
  binary::StoreLE(word.data(), frame::CommitWord(stale_len, 4));
  PatchFile(SegPath(4), kLogSegmentHeaderBytes, word);

  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 9U);
  EXPECT_EQ(recovered.back().header.seq, 11U);
  EXPECT_EQ(log->FilledPrefix(), 4 * kSpace);
  EXPECT_EQ(Append(*log, 0, 12, 64).pos, 4 * kSpace);
}

TEST_F(LogTest, FramesPastAHoleAreNeverResurrected) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    Append(*log, 0, 0, 64);
    // Never committed: the owner died mid-fill.
    ASSERT_TRUE(log->Reserve(20000).has_value());
    Append(*log, 1, 0, 20000);
    Append(*log, 1, 1, 20000);
    // Filled frames of segment 1's gen, past the hole.
    EXPECT_EQ(Append(*log, 1, 2, 20480).pos, kSpace);
    Append(*log, 1, 3, 20480);
    EXPECT_EQ(log->FilledPrefix(), 64U);
  }
  std::vector<RecoveredFrame> recovered;
  {
    auto log = OpenLog(&recovered);
    ASSERT_NE(log, nullptr);
    ASSERT_EQ(recovered.size(), 1U);
    EXPECT_EQ(log->FilledPrefix(), kSpace);
    // Same offset and size as the stale frame for seq 2.
    EXPECT_EQ(Append(*log, 1, 0, 20480).pos, kSpace);
  }
  recovered.clear();
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 2U);
  EXPECT_EQ(recovered[1].header.shard, 1U);
  EXPECT_EQ(recovered[1].header.seq, 0U);
  EXPECT_EQ(log->FilledPrefix(), 2 * kSpace);
}

// A file past the recovered end may move below the gen of the frames
// in it at one recovery; it must never get that gen back at the next.
TEST_F(LogTest, AFileNeverRegainsAGenItHeldAcrossTwoRecoveries) {
  constexpr std::size_t kThird = kSpace / 3;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    // Only segments 0-2 exist, so recovery has just their files to reuse.
    log->PausePreparerForTesting();
    Append(*log, 0, 0, 64);
    ASSERT_TRUE(log->Reserve(20000).has_value());
    Append(*log, 1, 0, 20000);
    Append(*log, 1, 1, 20000);
    for (core::SequenceId seq = 2; seq < 5; ++seq) Append(*log, 1, seq, kThird);
    // Gen-2 frames at offsets 0 and kThird, past the hole.
    EXPECT_EQ(Append(*log, 1, 5, kThird).pos, 2 * kSpace);
    Append(*log, 1, 6, kThird);
    EXPECT_EQ(log->FilledPrefix(), 64U);
  }
  // No segment may hold, where frames would start, a word of its own gen
  // that this life did not write.
  const auto expect_no_stale_gen = [&] {
    for (uint64_t ordinal = 1; ordinal < 4; ++ordinal) {
      for (uint64_t off = 0; off < kSpace; off += kThird) {
        const uint64_t word = ReadWord(SegPath(ordinal), kLogSegmentHeaderBytes + off);
        EXPECT_NE(frame::CommitGen(word), ordinal) << "segment " << ordinal << " offset " << off;
      }
    }
  };
  std::vector<RecoveredFrame> recovered;
  {
    auto log = OpenLog(&recovered);
    ASSERT_NE(log, nullptr);
    ASSERT_EQ(recovered.size(), 1U);
    EXPECT_EQ(log->FilledPrefix(), kSpace);
    expect_no_stale_gen();
  }
  recovered.clear();
  {
    auto log = OpenLog(&recovered);
    ASSERT_NE(log, nullptr);
    ASSERT_EQ(recovered.size(), 1U);
    EXPECT_EQ(log->FilledPrefix(), kSpace);
    expect_no_stale_gen();

    for (core::SequenceId seq = 0; seq < 3; ++seq) Append(*log, 1, seq, kThird);
    auto hole = log->Reserve(static_cast<uint32_t>(kThird));
    ASSERT_TRUE(hole.has_value());
    EXPECT_EQ(hole->pos, 2 * kSpace);
    auto after = log->Reserve(static_cast<uint32_t>(kThird));
    ASSERT_TRUE(after.has_value());
    log->Commit(*after, Frame(1, 4, kThird));
    EXPECT_EQ(log->FilledPrefix(), hole->pos) << "P must not pass a hole over a stale word";
    log->Commit(*hole, Frame(1, 3, kThird));
    EXPECT_EQ(log->FilledPrefix(), after->pos + kThird);
  }
  recovered.clear();
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 6U);
  for (std::size_t i = 1; i < recovered.size(); ++i) {
    EXPECT_EQ(recovered[i].header.shard, 1U);
    EXPECT_EQ(recovered[i].header.seq, i - 1);
  }
}

TEST_F(LogTest, AFrameForgedWithoutTheSegmentSaltIsATornTail) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    Append(*log, 0, 0, 64);
    // Never committed: the crash leaves this the first unfilled frame.
    ASSERT_TRUE(log->Reserve(64).has_value());
  }
  // A well-formed seq 1 of the right gen, as a client-chosen value could
  // carry it, but its CRC cannot cover the salt it does not know.
  std::vector<std::byte> forged = Frame(0, 1, 64);
  const uint32_t len = frame::CommitLen(binary::LoadLE<uint64_t>(forged.data()));
  const uint64_t word = frame::CommitWord(len, 0);
  const auto body_crc = binary::LoadLE<uint32_t>(forged.data() + frame::kCommitBytes);
  std::array<std::byte, 8> word_bytes{};
  binary::StoreLE(word_bytes.data(), word);
  binary::StoreLE(forged.data() + frame::kCommitBytes, Crc32cExtend(body_crc, word_bytes));
  binary::StoreLE(forged.data(), word);

  std::vector<std::byte> header(kLogSegmentHeaderBytes);
  {
    auto file = pfs::Open(SegPath(0), {.mode = pfs::OpenMode::kRead});
    ASSERT_TRUE(file.has_value());
    ASSERT_TRUE(pfs::Pread(*file, header.data(), header.size(), 0).has_value());
  }
  auto decoded = DecodeLogSegmentHeader(header);
  ASSERT_TRUE(decoded.has_value());
  std::vector<std::byte> salted = forged;
  binary::StoreLE(salted.data() + frame::kCommitBytes,
                  frame::SealCrc(body_crc, decoded->salt, word));
  ASSERT_EQ(frame::Inspect(word, salted, 0, decoded->salt, true).state, frame::State::kFilled)
      << "the same frame under the real salt would pass";
  PatchFile(SegPath(0), kLogSegmentHeaderBytes + 64, forged);

  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 1U);
  EXPECT_EQ(log->FilledPrefix(), kSpace);
}

// Recovery must take the segments past the end away before it pads the
// tail: a crash between the two would otherwise walk into them.
class SealCrashTest : public LogTest {
 protected:
  void CrashWhileSealing(Log::OpenStep step) {
    constexpr std::size_t kThird = kSpace / 3;
    {
      auto log = OpenLog();
      ASSERT_NE(log, nullptr);
      Append(*log, 0, 0, 64);
      ASSERT_TRUE(log->Reserve(20000).has_value());
      Append(*log, 1, 0, 20000);
      Append(*log, 1, 1, 20000);
      // Real, CRC-valid frames of segment 1, past the hole.
      EXPECT_EQ(Append(*log, 1, 2, kThird).pos, kSpace);
      Append(*log, 1, 3, kThird);
    }
    Log::CrashOpenAfterForTesting(step);
    auto crashed = Log::Open(Config(), {});
    Log::CrashOpenAfterForTesting(Log::OpenStep::kNone);
    ASSERT_FALSE(crashed.has_value());

    std::vector<RecoveredFrame> recovered;
    {
      auto log = OpenLog(&recovered);
      ASSERT_NE(log, nullptr);
      ASSERT_EQ(recovered.size(), 1U);
      EXPECT_EQ(log->FilledPrefix(), kSpace);
      Append(*log, 1, 0, 64);
    }
    recovered.clear();
    auto log = OpenLog(&recovered);
    ASSERT_NE(log, nullptr);
    ASSERT_EQ(recovered.size(), 2U);
    EXPECT_EQ(recovered[1].header.shard, 1U);
    EXPECT_EQ(recovered[1].header.seq, 0U);
  }

  void TearDown() override {
    Log::CrashOpenAfterForTesting(Log::OpenStep::kNone);
    LogTest::TearDown();
  }
};

TEST_F(SealCrashTest, ACrashAfterThePastEndRenamesLeavesTheHoleToEndTheLog) {
  CrashWhileSealing(Log::OpenStep::kPastEndRenamed);
}

TEST_F(SealCrashTest, ACrashAfterTheTailSyncFindsTheNextSegmentGone) {
  CrashWhileSealing(Log::OpenStep::kTailSynced);
}

TEST_F(LogTest, AnIncompleteTrailingBatchIsCutEvenAcrossAFlush) {
  LogPosition batch_pos = 0;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    Append(*log, 0, 0, 64);
    const Batch whole = MakeBatch(1, 0, {64, 64});
    auto first = log->Reserve(static_cast<uint32_t>(whole.bytes.size()));
    ASSERT_TRUE(first.has_value());
    CommitSlice(*log, *first, whole, 0);
    CommitSlice(*log, *first, whole, 1);

    const Batch cut = MakeBatch(2, 0, {64, 64, 64});
    auto reservation = log->Reserve(static_cast<uint32_t>(cut.bytes.size()));
    ASSERT_TRUE(reservation.has_value());
    batch_pos = reservation->pos;
    CommitSlice(*log, *reservation, cut, 0);
    CommitSlice(*log, *reservation, cut, 1);
    // The batch's first two frames become durable; its last never fills.
    ASSERT_TRUE(log->Flush({}).has_value());
    EXPECT_EQ(log->DurablePrefix(), batch_pos + 128);
  }
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  ASSERT_EQ(recovered.size(), 3U);
  EXPECT_EQ(recovered[1].header.shard, 1U);
  EXPECT_EQ(recovered[2].header.shard, 1U);
  EXPECT_EQ(log->FilledPrefix(), kSpace);
  auto pad = log->ReadFrame(batch_pos);
  ASSERT_TRUE(pad.has_value());
  EXPECT_EQ(pad->view.header.kind, frame::Kind::kPadding);
}

TEST_F(LogTest, AnUnknownKindUnderAValidCrcFailsClosed) {
  LogPosition pos = 0;
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    auto bytes = Frame(0, 0, 64);
    bytes[kBodyAt] = std::byte{9};
    frame::CloseBatch(bytes);
    auto reservation = log->Reserve(64);
    ASSERT_TRUE(reservation.has_value());
    log->Commit(*reservation, bytes);
    pos = reservation->pos;
    auto read = log->ReadFrame(pos);
    ASSERT_FALSE(read.has_value());
    EXPECT_EQ(read.error().code(), core::ErrorCode::kCorruption);
    EXPECT_NE(read.error().message().find("kind 9"), std::string::npos) << read.error().message();
    auto flushed = log->Flush({});
    ASSERT_FALSE(flushed.has_value());
    EXPECT_EQ(flushed.error().code(), core::ErrorCode::kCorruption);
  }
  auto reopened = Log::Open(Config(), {});
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kCorruption);
  EXPECT_NE(reopened.error().message().find("kind 9"), std::string::npos);
}

TEST_F(LogTest, ALeftoverTmpIsDeletedAndATornSpareIsUnlinked) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    Append(*log, 0, 0, 64);
  }
  const std::filesystem::path tmp = Config().dir / (Padded(9) + ".seg.tmp");
  std::ofstream(tmp) << "partial";
  ASSERT_TRUE(std::filesystem::exists(tmp));
  FlipByte(SegPath(2), 10);

  const double grown_before =
      metrics::testing::GetCounterValue(metrics::names::kWalSegmentsGrownTotal).value_or(0);
  std::vector<RecoveredFrame> recovered;
  auto log = OpenLog(&recovered);
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(recovered.size(), 1U);
  EXPECT_FALSE(std::filesystem::exists(tmp));
  // Without its last ordinal the torn spare cannot be renumbered safely.
  // Old spare 1 becomes segment 2; segments 1 and 3 are grown.
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kWalSegmentsGrownTotal).value_or(0),
            grown_before + 2);
  EXPECT_EQ(log->spare_count(), 2U);
  EXPECT_TRUE(FilesWith("free-").empty());
  for (const auto& name : FilesWith(".seg")) {
    std::vector<std::byte> header(kLogSegmentHeaderBytes);
    auto file = pfs::Open(Config().dir / name, {.mode = pfs::OpenMode::kRead});
    ASSERT_TRUE(file.has_value());
    ASSERT_TRUE(pfs::Pread(*file, header.data(), header.size(), 0).has_value());
    EXPECT_TRUE(DecodeLogSegmentHeader(header).has_value()) << name;
  }
}

TEST_F(LogTest, AnOrdinalGapIsCorruption) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    for (core::SequenceId seq = 0; seq < 8; ++seq) Append(*log, 0, seq, 20000);
  }
  std::filesystem::remove(SegPath(1));
  auto log = Log::Open(Config(), {});
  ASSERT_FALSE(log.has_value());
  EXPECT_EQ(log.error().code(), core::ErrorCode::kCorruption);
  EXPECT_NE(log.error().message().find("segment 1"), std::string::npos) << log.error().message();
}

TEST_F(LogTest, AShardCountMismatchIsRefused) {
  {
    auto log = OpenLog();
    ASSERT_NE(log, nullptr);
    Append(*log, 0, 0, 64);
  }
  auto log = Log::Open(Config(kShards * 2), {});
  ASSERT_FALSE(log.has_value());
  EXPECT_EQ(log.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_NE(log.error().message().find("4 shards"), std::string::npos) << log.error().message();
}

TEST(LogSegmentHeaderTest, RoundTripsAndRejectsDamage) {
  std::vector<std::byte> bytes(kLogSegmentHeaderBytes, std::byte{0x5A});
  const LogSegmentHeader header{.log_id = 2,
                                .ordinal = 77,
                                .created_at = core::WallTime(std::chrono::microseconds(123)),
                                .shard_count = 64,
                                .durability_window_bytes = 1 << 20,
                                .salt = 0x0123'4567'89ab'cdef};
  EncodeLogSegmentHeader(header, bytes);
  EXPECT_EQ(bytes.back(), std::byte{0}) << "the rest of the block is zeroed";
  auto decoded = DecodeLogSegmentHeader(bytes);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  EXPECT_EQ(decoded->log_id, 2U);
  EXPECT_EQ(decoded->ordinal, 77U);
  EXPECT_EQ(decoded->shard_count, 64U);
  EXPECT_EQ(decoded->durability_window_bytes, 1U << 20);
  EXPECT_EQ(decoded->created_at, header.created_at);
  EXPECT_EQ(decoded->salt, header.salt);

  bytes[20] ^= std::byte{0x01};
  EXPECT_FALSE(DecodeLogSegmentHeader(bytes).has_value());
  bytes[20] ^= std::byte{0x01};
  bytes[4] = std::byte{1};
  EXPECT_FALSE(DecodeLogSegmentHeader(bytes).has_value()) << "format 1 is not a log segment";
}

}  // namespace
}  // namespace abyss::queue
