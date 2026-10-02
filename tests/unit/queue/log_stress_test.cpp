#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/platform/fs.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/log.h"
#include "binary_io.h"
#include "segment_header_v2.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

constexpr std::size_t kSegmentBytes = std::size_t{64} << 10;
constexpr uint64_t kSpace = kSegmentBytes - kLogSegmentHeaderBytes;
constexpr int kThreads = 8;
constexpr int kPerThread = 2000;
// A Write frame for SET k <value> is 64 bytes plus the value.
constexpr std::size_t kWriteFrameOverhead = 64;

std::vector<std::byte> Frame(core::ShardId shard, core::SequenceId seq, std::size_t size) {
  std::vector<std::byte> out;
  frame::EncodeEntry(
      core::QueueEntry{
          .seq = seq,
          .payload =
              core::entry::Write{
                  .cmd = core::RespCommand{{"SET", "k",
                                            std::string(size - kWriteFrameOverhead, 'v')}}}},
      shard, out);
  frame::CloseBatch(out);
  return out;
}

LogConfig Config(const testing::TempDir& dir) {
  return LogConfig{.dir = dir.Path() / "log",
                   .shard_count = kThreads,
                   .segment_size_bytes = kSegmentBytes,
                   .durability_window_bytes = uint64_t{1} << 20};
}

// Reserves until a spare is ready, commits, and waits for the prefix.
Log::Reservation AppendFrame(Log& log, std::span<const std::byte> bytes) {
  for (;;) {
    auto reservation = log.Reserve(static_cast<uint32_t>(bytes.size()));
    if (reservation.has_value()) {
      log.Commit(*reservation, bytes);
      log.AwaitFilled(reservation->pos + reservation->size);
      return *reservation;
    }
    EXPECT_EQ(reservation.error().code(), core::ErrorCode::kUnavailable);
    if (!log.WaitForSpare(std::chrono::steady_clock::now() + 10s)) {
      ADD_FAILURE() << "no spare segment within 10s";
      return {};
    }
  }
}

// Reserves until a spare is ready and commits, without waiting for P.
Log::Reservation ReserveAndCommit(Log& log, std::span<const std::byte> bytes) {
  for (;;) {
    auto reservation = log.Reserve(static_cast<uint32_t>(bytes.size()));
    if (reservation.has_value()) {
      log.Commit(*reservation, bytes);
      return *reservation;
    }
    if (!log.WaitForSpare(std::chrono::steady_clock::now() + 10s)) {
      ADD_FAILURE() << "no spare segment within 10s";
      return {};
    }
  }
}

// Each thread owns one shard, so its seqs are its reservation order.
TEST(LogStressTest, ConcurrentAppendersAndAFlusherAgree) {
  const testing::TempDir dir("log_stress");
  auto opened = Log::Open(Config(dir), {});
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  Log& log = **opened;

  std::atomic<int> done{0};
  std::vector<std::vector<Log::Reservation>> reserved(kThreads);
  std::vector<std::pair<core::ShardId, core::SequenceId>> flushed;
  std::thread flusher([&] {
    for (;;) {
      const bool finished = done.load(std::memory_order_acquire) == kThreads;
      auto result = log.Flush([&](const frame::Header& header, uint32_t) {
        flushed.emplace_back(header.shard, header.seq);
      });
      if (!result.has_value()) {
        ADD_FAILURE() << result.error().message();
        return;
      }
      if (finished && log.DurablePrefix() == log.ReservedTail()) return;
    }
  });
  std::vector<std::thread> writers;
  writers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t] {
      std::mt19937 rng(static_cast<uint32_t>(t));
      std::uniform_int_distribution<std::size_t> words(0, 60);
      for (int seq = 0; seq < kPerThread; ++seq) {
        const auto bytes = Frame(static_cast<core::ShardId>(t), static_cast<core::SequenceId>(seq),
                                 kWriteFrameOverhead + (8 * words(rng)));
        reserved[t].push_back(AppendFrame(log, bytes));
      }
      done.fetch_add(1, std::memory_order_release);
    });
  }
  for (auto& writer : writers) writer.join();
  flusher.join();

  EXPECT_EQ(log.FilledPrefix(), log.ReservedTail());
  EXPECT_EQ(log.DurablePrefix(), log.FilledPrefix());

  // Reservations never overlap, and each was filled by its own frame.
  std::vector<Log::Reservation> all;
  for (const auto& per_thread : reserved)
    all.insert(all.end(), per_thread.begin(), per_thread.end());
  std::ranges::sort(all, {}, &Log::Reservation::pos);
  for (std::size_t i = 1; i < all.size(); ++i) {
    ASSERT_LE(all[i - 1].pos + all[i - 1].size, all[i].pos);
  }
  std::map<LogPosition, std::pair<core::ShardId, core::SequenceId>> walked;
  Log::Cursor cursor(log);
  for (std::optional<LogPosition> pos = 0; pos.has_value();) {
    auto view = cursor.Read(*pos);
    ASSERT_TRUE(view.has_value()) << view.error().message();
    ASSERT_EQ(view->header.kind, frame::Kind::kEntry);
    walked.emplace(*pos, std::pair(view->header.shard, view->header.seq));
    auto next = cursor.Next(*pos);
    ASSERT_TRUE(next.has_value()) << next.error().message();
    pos = *next;
  }
  ASSERT_EQ(walked.size(), all.size());
  for (int t = 0; t < kThreads; ++t) {
    for (int seq = 0; seq < kPerThread; ++seq) {
      const auto it = walked.find(reserved[t][seq].pos);
      ASSERT_NE(it, walked.end());
      EXPECT_EQ(it->second,
                std::pair(static_cast<core::ShardId>(t), static_cast<core::SequenceId>(seq)));
    }
  }

  // Every entry became durable in exactly one flush walk.
  ASSERT_EQ(flushed.size(), all.size());
  std::vector<core::SequenceId> next(kThreads, 0);
  for (const auto& [shard, seq] : flushed) EXPECT_EQ(seq, next[shard]++);

  opened->reset();
  std::vector<std::vector<core::SequenceId>> recovered(kThreads);
  auto reopened = Log::Open(Config(dir), [&](const RecoveredFrame& f) {
    recovered[f.header.shard].push_back(f.header.seq);
  });
  ASSERT_TRUE(reopened.has_value()) << reopened.error().message();
  for (int t = 0; t < kThreads; ++t) {
    ASSERT_EQ(recovered[t].size(), static_cast<std::size_t>(kPerThread));
    for (int seq = 0; seq < kPerThread; ++seq)
      EXPECT_EQ(recovered[t][seq], static_cast<core::SequenceId>(seq));
  }
}

// Retention reclaims sealed segments while appenders roll and readers
// hold frames: deferred recycling and the hazard check under load.
TEST(LogStressTest, ReclaimRacesAppendersAndReaders) {
  const testing::TempDir dir("log_reclaim_stress");
  auto opened = Log::Open(Config(dir), {});
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  Log& log = **opened;

  constexpr int kWriters = 4;
  constexpr int kFrames = 600;
  std::atomic<int> done{0};
  std::thread flusher([&] {
    while (done.load(std::memory_order_acquire) < kWriters) {
      if (auto result = log.Flush({}); !result.has_value()) {
        ADD_FAILURE() << result.error().message();
        return;
      }
      for (const auto& segment : log.SealedSegments(64)) {
        if (auto reclaimed = log.Reclaim(segment.ordinal); !reclaimed.has_value()) {
          ADD_FAILURE() << reclaimed.error().message();
          return;
        }
      }
    }
  });
  std::thread reader([&] {
    // A fixed seed keeps a failure reproducible.
    std::mt19937_64 rng(7);  // NOLINT(bugprone-random-generator-seed)
    std::vector<FrameRef> held;
    while (done.load(std::memory_order_acquire) < kWriters) {
      // Every segment starts with a frame.
      const auto sealed = log.SealedSegments(64);
      const LogPosition start = !sealed.empty() && (rng() % 2) == 0
                                    ? sealed.front().ordinal * kSpace
                                    : (log.DurablePrefix() / kSpace) * kSpace;
      std::optional<LogPosition> pos =
          start < log.FilledPrefix() ? std::optional(start) : std::nullopt;
      Log::Cursor cursor(log);
      for (int step = 0; pos.has_value() && step < 64; ++step) {
        auto ref = log.ReadFrame(*pos);
        if (ref.has_value()) {
          held.push_back(std::move(*ref));
        } else if (ref.error().code() != core::ErrorCode::kOutOfRange) {
          ADD_FAILURE() << ref.error().message();
          return;
        }
        auto next = cursor.Next(*pos);
        if (!next.has_value()) {
          if (next.error().code() != core::ErrorCode::kOutOfRange) {
            ADD_FAILURE() << next.error().message();
            return;
          }
          break;
        }
        pos = *next;
      }
      if (held.size() > 32 || (rng() % 4) == 0) held.clear();
    }
  });
  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int t = 0; t < kWriters; ++t) {
    writers.emplace_back([&, t] {
      for (int seq = 0; seq < kFrames; ++seq) {
        AppendFrame(log,
                    Frame(static_cast<core::ShardId>(t), static_cast<core::SequenceId>(seq), 4096));
      }
      done.fetch_add(1, std::memory_order_release);
    });
  }
  for (auto& writer : writers) writer.join();
  flusher.join();
  reader.join();
  EXPECT_EQ(log.FilledPrefix(), log.ReservedTail());
  EXPECT_LE(log.free_count(), 2U);
}

// Two recycled segments hold, at every 8-byte offset, a plausible
// commit word for the gen they are about to take, as old payload bytes
// can. P must still move only through committed frames.
TEST(LogStressTest, StaleBytesShapedLikeCommitWordsNeverMoveP) {
  const testing::TempDir dir("log_seeded_stress");
  auto opened = Log::Open(Config(dir), {});
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  Log& log = **opened;
  for (core::SequenceId seq = 0; seq < 7; ++seq) {
    AppendFrame(log, Frame(0, seq, 20000));
  }
  ASSERT_TRUE(log.Flush({}).has_value());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (log.spare_count() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(log.spare_count(), 2U);
  log.PausePreparerForTesting();
  ASSERT_TRUE(log.Reclaim(0).has_value());
  ASSERT_TRUE(log.Reclaim(1).has_value());
  ASSERT_EQ(log.free_count(), 2U);

  // Segments 0-4 exist, so the pool files become 5 and 6.
  std::vector<std::byte> seeded(kSpace);
  for (std::size_t off = 0; off < kSpace; off += 8) {
    binary::StoreLE(seeded.data() + off,
                    frame::CommitWord(48, static_cast<uint32_t>(5 + ((off / 8) % 2))));
  }
  for (const auto& entry : std::filesystem::directory_iterator(dir.Path() / "log")) {
    if (!entry.path().filename().string().starts_with("free-")) continue;
    auto file = platform::fs::Open(entry.path(), {.mode = platform::fs::OpenMode::kReadWrite});
    ASSERT_TRUE(file.has_value());
    ASSERT_TRUE(platform::fs::Pwrite(*file, seeded.data(), seeded.size(), kLogSegmentHeaderBytes)
                    .has_value());
  }
  log.ResumePreparerForTesting();

  // Shard 0 holds the setup frames; each writer owns one of the rest.
  constexpr int kWriters = kThreads - 1;
  constexpr int kFrames = 400;
  std::atomic<int> done{0};
  std::vector<std::pair<core::ShardId, core::SequenceId>> flushed;
  std::thread flusher([&] {
    for (;;) {
      const bool finished = done.load(std::memory_order_acquire) == kWriters;
      auto result = log.Flush([&](const frame::Header& header, uint32_t) {
        flushed.emplace_back(header.shard, header.seq);
      });
      if (!result.has_value()) {
        ADD_FAILURE() << result.error().message();
        return;
      }
      if (finished && log.DurablePrefix() == log.ReservedTail()) return;
    }
  });
  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int t = 0; t < kWriters; ++t) {
    writers.emplace_back([&, t] {
      std::mt19937 rng(static_cast<uint32_t>(t));
      std::uniform_int_distribution<std::size_t> words(0, 60);
      const auto shard = static_cast<core::ShardId>(t + 1);
      for (int seq = 0; seq < kFrames; ++seq) {
        AppendFrame(log, Frame(shard, static_cast<core::SequenceId>(seq),
                               kWriteFrameOverhead + (8 * words(rng))));
      }
      done.fetch_add(1, std::memory_order_release);
    });
  }
  for (auto& writer : writers) writer.join();
  flusher.join();
  ASSERT_GE(log.ReservedTail(), 7 * kSpace) << "the writes reached past both seeded segments";
  EXPECT_EQ(log.free_count(), 0U);
  EXPECT_EQ(log.FilledPrefix(), log.ReservedTail());
  ASSERT_EQ(flushed.size(), static_cast<std::size_t>(kWriters * kFrames));
  std::map<core::ShardId, core::SequenceId> next;
  for (const auto& [shard, seq] : flushed) EXPECT_EQ(seq, next[shard]++);

  Log::Cursor cursor(log);
  std::size_t walked = 0;
  for (std::optional<LogPosition> pos = 2 * kSpace; pos.has_value();) {
    auto view = cursor.Read(*pos);
    ASSERT_TRUE(view.has_value()) << view.error().message();
    ASSERT_TRUE(frame::DecodeEntry(*view).has_value());
    ++walked;
    auto step = cursor.Next(*pos);
    ASSERT_TRUE(step.has_value()) << step.error().message();
    pos = *step;
  }
  EXPECT_EQ(walked, flushed.size() + 1);
}

// One reservation holds P while others commit several rings' worth of
// completions. With the combiner held the ring fills and committers
// wait; once released, a committer that finds the ring full drains it
// and takes its own completion straight into the heap. None is lost.
TEST(LogStressTest, AHeldHoleOutlastsSeveralRingsOfCompletions) {
  const testing::TempDir dir("log_ring_stress");
  auto opened = Log::Open(Config(dir), {});
  ASSERT_TRUE(opened.has_value()) << opened.error().message();
  Log& log = **opened;
  const auto large = Frame(0, 0, 4096);
  auto hole = log.Reserve(static_cast<uint32_t>(large.size()));
  ASSERT_TRUE(hole.has_value());
  log.PauseCombinerForTesting();

  constexpr int kWriters = 4;
  constexpr int kFrames = 1500;
  constexpr std::size_t kSmall = 64;
  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int t = 0; t < kWriters; ++t) {
    writers.emplace_back([&, t] {
      for (int seq = 0; seq < kFrames; ++seq) {
        ReserveAndCommit(log, Frame(static_cast<core::ShardId>(t + 1),
                                    static_cast<core::SequenceId>(seq), kSmall));
      }
    });
  }
  // Wait until every writer is stuck behind a full ring.
  LogPosition tail = 0;
  for (int stable = 0; stable < 5;) {
    std::this_thread::sleep_for(10ms);
    const LogPosition now = log.ReservedTail();
    stable = now == tail ? stable + 1 : 0;
    tail = now;
  }
  const uint64_t queued = (tail - hole->pos - large.size()) / kSmall;
  EXPECT_GT(queued, 1000U) << "the ring filled";
  EXPECT_LT(queued, static_cast<uint64_t>(kWriters * kFrames)) << "and held the writers";
  EXPECT_EQ(log.FilledPrefix(), hole->pos);

  log.ResumeCombinerForTesting();
  for (auto& writer : writers) writer.join();
  EXPECT_EQ(log.FilledPrefix(), hole->pos);

  log.Commit(*hole, large);
  EXPECT_EQ(log.FilledPrefix(), log.ReservedTail());
  auto flushed = log.Flush({});
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(flushed->entries, static_cast<uint64_t>(kWriters * kFrames) + 1);
}

}  // namespace
}  // namespace abyss::queue
