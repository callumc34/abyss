// Reserve and Complete against a real WAL: cross-shard batches through
// the flush walk, recovery and a kill -9.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
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
#include "abyss/queue/reservation.h"
#include "abyss/queue/wal_queue.h"
#include "crash_harness.h"
#include "durability_printer.h"
#include "latch.h"
#include "on_exit.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;
using abyss::testing::Latch;

constexpr auto kProcess = core::Durability::kProcessCrash;
constexpr auto kPower = core::Durability::kPowerLoss;
constexpr core::SequenceId kFirst = core::kFirstSeq;
constexpr uint64_t kHeaderBlock = 4096;
constexpr LogPosition kNoPosition = ~LogPosition{0};
constexpr std::size_t kSegmentBytes = std::size_t{1} << 20;
constexpr uint64_t kFrameSpace = kSegmentBytes - kHeaderBlock;
// Past kLockHoldFrameBytes once framed, so Complete fills it.
constexpr std::size_t kLarge = std::size_t{20} << 10;

core::QueueEntry MakeWrite(const std::string& key, std::size_t value_bytes = 8) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Write{.cmd = core::RespCommand{{"SET", key, std::string(value_bytes, 'v')}}},
  };
}

std::string KeyOf(const core::QueueEntry& entry) {
  return std::get<core::entry::Write>(entry.payload).cmd.args.at(1);
}

WalConfig ReserveConfig(const std::filesystem::path& dir, core::Durability durability) {
  return WalConfig{
      .wal_path = dir.string(),
      .segment_size_bytes = kSegmentBytes,
      .shard_count = 4,
      .durability = durability,
      .min_retention = 0s,
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
      .offset_fsync_interval = std::chrono::hours{1},
  };
}

// The segment file holding `pos`, and the file offset of `pos` in it.
std::pair<std::filesystem::path, uint64_t> FileAt(const std::filesystem::path& wal_path,
                                                  LogPosition pos) {
  const uint64_t ordinal = pos / kFrameSpace;
  std::string name = std::to_string(ordinal);
  name.insert(0, 20 - name.size(), '0');
  return {wal_path / "log-0000" / (name + ".seg"), kHeaderBlock + (pos - (ordinal * kFrameSpace))};
}

// The frame at `pos` as its segment file holds it, CRC unchecked.
frame::View InspectAt(const std::filesystem::path& wal_path, LogPosition pos,
                      std::vector<std::byte>& bytes) {
  const auto [path, offset] = FileAt(wal_path, pos);
  std::ifstream in(path, std::ios::binary);
  in.seekg(static_cast<std::streamoff>(offset));
  bytes.assign(kLarge * 2, std::byte{0});
  in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
  bytes.resize(static_cast<std::size_t>(in.gcount()));
  if (bytes.size() < frame::kMinFrameBytes) return {};
  uint64_t word = 0;
  std::memcpy(&word, bytes.data(), sizeof(word));
  return frame::Inspect(word, bytes, static_cast<uint32_t>(pos / kFrameSpace), 0, false);
}

class WalQueueReserveTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override {
    queue_.reset();
    metrics::testing::Reset();
  }

  void OpenWith(const WalConfig& config) {
    queue_.reset();
    auto opened = WalQueue::Open(config);
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  void AwaitPowerDurable(core::ShardId shard, core::SequenceId seq) {
    auto durable = queue_->AwaitDurable(shard, seq, kPower, 10s);
    ASSERT_TRUE(durable.has_value() && *durable) << "shard " << shard << " seq " << seq;
  }

  std::vector<core::QueueEntry> ReadAll(core::ShardId shard) {
    auto read = queue_->Read(shard, kFirst, 1000, 0ms, kProcess);
    EXPECT_TRUE(read.has_value()) << read.error().message();
    return read.has_value() ? std::move(*read) : std::vector<core::QueueEntry>{};
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"wal_reserve"};
  std::unique_ptr<WalQueue> queue_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

class FreshShardTest : public WalQueueReserveTest,
                       public ::testing::WithParamInterface<core::Durability> {};

// An empty shard's exclusive ends are kFirstSeq at both classes, so a
// fence on 0 returns at once and an empty scan delivers nothing; its
// first write, appended or reserved, publishes and resolves.
TEST_P(FreshShardTest, EndsStartAtTheFirstSeqAndTheFirstWriteResolves) {
  OpenWith(ReserveConfig(dir_.Path(), GetParam()));
  for (const core::Durability durability : {kProcess, kPower}) {
    SCOPED_TRACE(core::DurabilityName(durability));
    EXPECT_EQ(queue_->DurableEnd(0, durability).value(), core::kFirstSeq);
    EXPECT_TRUE(queue_->AwaitDurable(0, 0, durability, 0ms).value()) << "a fence on 0 waited";
  }
  EXPECT_EQ(queue_->FirstSeq(0).value(), core::kFirstSeq);
  EXPECT_EQ(queue_->TailSeq(0).value(), 0U);

  const std::vector<core::SequenceId> empty(4, core::kFirstSeq);
  std::size_t delivered = 0;
  const std::atomic<bool> cancel{false};
  auto scanned = queue_->Scan(
      empty, empty, 2,
      [&delivered](core::ShardId, std::vector<core::QueueEntry>& batch) {
        delivered += batch.size();
        return core::Result<void>{};
      },
      cancel);
  ASSERT_TRUE(scanned.has_value()) << scanned.error().message();
  EXPECT_EQ(delivered, 0U);

  auto appended = queue_->Append(0, MakeWrite("a"));
  ASSERT_TRUE(appended.has_value()) << appended.error().message();
  EXPECT_EQ(appended->seq, core::kFirstSeq);
  ASSERT_EQ(appended->durable.wait_for(10s), std::future_status::ready);
  EXPECT_TRUE(appended->durable.get().has_value());

  std::vector<core::QueueEntry> b{MakeWrite("b")};
  auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 1, .entries = b}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  EXPECT_EQ(reserved->ranges().front().first, core::kFirstSeq);
  for (ShardDurable& part : queue_->Complete(std::move(*reserved))) {
    ASSERT_EQ(part.durable.wait_for(10s), std::future_status::ready);
    EXPECT_TRUE(part.durable.get().has_value());
  }
  for (const core::ShardId shard : {0U, 1U}) {
    SCOPED_TRACE(shard);
    EXPECT_EQ(queue_->DurableEnd(shard, GetParam()).value(), core::kFirstSeq + 1);
    auto read = queue_->Read(shard, core::kFirstSeq, 10, 0ms, GetParam());
    ASSERT_TRUE(read.has_value()) << read.error().message();
    ASSERT_EQ(read->size(), 1U);
    EXPECT_EQ(read->front().seq, core::kFirstSeq);
  }
}

INSTANTIATE_TEST_SUITE_P(BothClasses, FreshShardTest, ::testing::Values(kProcess, kPower),
                         [](const ::testing::TestParamInfo<core::Durability>& param) {
                           return param.param == kProcess ? "ProcessCrash" : "PowerLoss";
                         });

// The flush walk raises every shard of a batch at its last frame: a
// flush that ends inside the batch moves no shard's power end, even
// once the batch is published.
TEST_F(WalQueueReserveTest, ACrossShardBatchIsPowerDurableOnlyOnceItsLastFrameIs) {
  auto config = ReserveConfig(dir_.Path(), kPower);
  config.durability_window = 60s;
  OpenWith(config);
  auto first_entered = std::make_shared<Latch>();
  auto first_flush = std::make_shared<Latch>();
  auto stall = std::make_shared<std::atomic<bool>>(false);
  auto stalled = std::make_shared<Latch>();
  auto unstall = std::make_shared<Latch>();
  auto in_batch = std::make_shared<Latch>();
  auto finish_batch = std::make_shared<Latch>();
  auto flushes = std::make_shared<std::atomic<int>>(0);
  std::future<DurableFutures> batch;
  const abyss::testing::OnExit release([=] {
    first_flush->Open();
    finish_batch->Open();
    unstall->Open();
  });
  queue_->SetFlushHookForTesting([=](uint32_t) -> core::Result<void> {
    if (flushes->fetch_add(1) == 0) {
      first_entered->Open();
      first_flush->Wait();
    } else if (stall->load()) {
      stalled->Open();
      unstall->Wait();
    }
    return {};
  });
  queue_->SetBatchCommitHookForTesting([=](std::size_t committed) {
    if (committed != 2) return;
    in_batch->Open();
    finish_batch->Wait();
  });

  // The first flush holds a snapshot of shard 3's first frame alone;
  // the second is queued behind it.
  ASSERT_TRUE(queue_->Append(3, MakeWrite("y0")).has_value());
  ASSERT_TRUE(first_entered->Wait());
  ASSERT_TRUE(queue_->Append(3, MakeWrite("y1")).has_value());
  batch = std::async(std::launch::async, [this] {
    std::vector<core::QueueEntry> a{MakeWrite("a")};
    std::vector<core::QueueEntry> b{MakeWrite("b")};
    std::vector<core::QueueEntry> c{MakeWrite("c")};
    auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = a},
                                               ShardEntries{.shard = 1, .entries = b},
                                               ShardEntries{.shard = 2, .entries = c}});
    if (!reserved.has_value()) return DurableFutures{};
    return queue_->Complete(std::move(*reserved));
  });
  ASSERT_TRUE(in_batch->Wait());

  // The second flush ends after shard 1's frame, inside the batch.
  first_flush->Open();
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(3, kFirst + 1));
  stall->store(true);
  finish_batch->Open();
  ASSERT_TRUE(stalled->Wait());
  ASSERT_EQ(batch.wait_for(10s), std::future_status::ready);
  DurableFutures durable = batch.get();
  ASSERT_EQ(durable.size(), 3U);
  for (ShardDurable& part : durable) {
    SCOPED_TRACE(part.shard);
    EXPECT_EQ(queue_->DurableEnd(part.shard, kProcess).value(), kFirst + 1);
    EXPECT_EQ(queue_->DurableEnd(part.shard, kPower).value(), kFirst);
    EXPECT_EQ(part.durable.wait_for(0s), std::future_status::timeout);
  }
  EXPECT_FALSE(queue_->AwaitDurable(0, kFirst, kPower, 20ms).value());

  unstall->Open();
  for (ShardDurable& part : durable) {
    SCOPED_TRACE(part.shard);
    ASSERT_EQ(part.durable.wait_for(10s), std::future_status::ready);
    EXPECT_TRUE(part.durable.get().has_value());
    EXPECT_EQ(queue_->DurableEnd(part.shard, kPower).value(), kFirst + 1);
  }
}

// Batch closure is by position, so a torn last frame drops the batch
// on every shard it spans; each shard's seqs then continue from where
// they stood before it.
TEST_F(WalQueueReserveTest, ACrossShardBatchWithATornLastFrameIsDroppedOnEveryShard) {
  const auto config = ReserveConfig(dir_.Path(), kPower);
  OpenWith(config);
  for (core::ShardId shard = 0; shard < 4; ++shard) {
    ASSERT_TRUE(queue_->Append(shard, MakeWrite("before")).has_value());
    ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(shard, kFirst));
  }
  std::vector<core::QueueEntry> a{MakeWrite("a0"), MakeWrite("a1")};
  std::vector<core::QueueEntry> b{MakeWrite("b0", kLarge)};
  std::vector<core::QueueEntry> d{MakeWrite("d0", 100)};
  const std::size_t last_size = frame::EntryFrameSize(d[0]);
  auto reserved = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = a},
                                             ShardEntries{.shard = 1, .entries = b},
                                             ShardEntries{.shard = 3, .entries = d}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  for (ShardDurable& part : queue_->Complete(std::move(*reserved))) {
    ASSERT_EQ(part.durable.wait_for(10s), std::future_status::ready);
  }
  for (core::ShardId shard = 0; shard < 4; ++shard) {
    ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(shard, kFirst));
  }
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(3, kFirst + 1));
  const LogPosition last = queue_->PositionForTesting(3, kFirst + 1).value_or(kNoPosition);
  ASSERT_NE(last, kNoPosition);
  queue_.reset();

  // One byte of the last frame's value is wrong: its CRC fails.
  {
    const auto [path, offset] = FileAt(dir_.Path(), last + last_size - 16);
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(static_cast<std::streamoff>(offset));
    file.put('x');
    ASSERT_TRUE(file.good());
  }

  OpenWith(config);
  for (core::ShardId shard = 0; shard < 4; ++shard) {
    SCOPED_TRACE(shard);
    EXPECT_EQ(queue_->TailSeq(shard).value(), kFirst);
    const auto read = ReadAll(shard);
    ASSERT_EQ(read.size(), 1U);
    EXPECT_EQ(KeyOf(read[0]), "before");
  }

  std::vector<core::QueueEntry> again_a{MakeWrite("x0")};
  std::vector<core::QueueEntry> again_d{MakeWrite("x3")};
  auto again = queue_->Reserve(std::array{ShardEntries{.shard = 0, .entries = again_a},
                                          ShardEntries{.shard = 3, .entries = again_d}});
  ASSERT_TRUE(again.has_value()) << again.error().message();
  EXPECT_EQ(again->ranges()[0].first, kFirst + 1);
  EXPECT_EQ(again->ranges()[1].first, kFirst + 1);
  for (ShardDurable& part : queue_->Complete(std::move(*again))) {
    ASSERT_EQ(part.durable.wait_for(10s), std::future_status::ready);
  }

  // Recovery's per-shard seq check holds across the new batch.
  OpenWith(config);
  for (const core::ShardId shard : {0U, 3U}) {
    const auto read = ReadAll(shard);
    ASSERT_EQ(read.size(), 2U) << shard;
    EXPECT_EQ(read[1].seq, kFirst + 1);
    EXPECT_EQ(KeyOf(read[1]), "x" + std::to_string(shard));
  }
}

#ifdef _WIN32

TEST(WalReserveCrashTest, AHalfFilledCrossShardBatchIsDroppedOnEveryShard) {
  GTEST_SKIP() << "out-of-process crash simulation is POSIX-only";
}

#else

constexpr const char* kVictimDirEnv = "ABYSS_WAL_RESERVE_VICTIM_DIR";
constexpr const char* kReadyFile = "reserve_victim.ready";

// Commits a three-shard batch's two small frames and leaves its large
// last frame unfilled, then parks holding the reservation.
TEST(WalReserveCrashVictim, HalfFilledBatch) {
  bool is_victim = false;
  const auto dir = testing::VictimDirFromEnv(kVictimDirEnv, &is_victim);
  if (!is_victim) GTEST_SKIP() << "crash victim; driven out-of-process by WalReserveCrashTest";

  auto queue = WalQueue::Open(ReserveConfig(dir, kProcess));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();
  for (core::ShardId shard = 0; shard < 3; ++shard) {
    auto appended = (*queue)->Append(shard, MakeWrite("before"));
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
    ASSERT_TRUE(appended->durable.get().has_value());
  }
  std::vector<core::QueueEntry> a{MakeWrite("a")};
  std::vector<core::QueueEntry> b{MakeWrite("b")};
  std::vector<core::QueueEntry> c{MakeWrite("c", kLarge)};
  auto reserved = (*queue)->Reserve(std::array{ShardEntries{.shard = 0, .entries = a},
                                               ShardEntries{.shard = 1, .entries = b},
                                               ShardEntries{.shard = 2, .entries = c}});
  ASSERT_TRUE(reserved.has_value()) << reserved.error().message();
  std::ostringstream positions;
  for (core::ShardId shard = 0; shard < 3; ++shard) {
    const auto pos = (*queue)->PositionForTesting(shard, kFirst + 1);
    ASSERT_TRUE(pos.has_value());
    positions << *pos << ' ';
  }
  testing::SignalReadyAndPark(dir, kReadyFile, positions.str());
}

class WalReserveCrashTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"wal_reserve_crash"};
};

TEST_F(WalReserveCrashTest, AHalfFilledCrossShardBatchIsDroppedOnEveryShard) {
  const auto outcome = testing::SpawnAndKillVictim(testing::VictimSpec{
      .gtest_filter = "WalReserveCrashVictim.HalfFilledBatch",
      .dir_env_var = kVictimDirEnv,
      .dir = dir_.Path(),
      .ready_file_name = kReadyFile,
  });
  ASSERT_TRUE(outcome.error.empty()) << outcome.error;
  ASSERT_TRUE(outcome.reached_ready) << "crash victim never reached its ready point";
  ASSERT_TRUE(outcome.died_by_signal);

  std::array<LogPosition, 3> positions{};
  {
    std::istringstream in(outcome.ready_payload);
    for (LogPosition& pos : positions) in >> pos;
    ASSERT_FALSE(in.fail()) << "victim report unreadable: '" << outcome.ready_payload << "'";
  }
  // The crash left the batch's first two frames filled and its last not.
  std::vector<std::byte> bytes;
  for (core::ShardId shard = 0; shard < 2; ++shard) {
    const frame::View view = InspectAt(dir_.Path(), positions.at(shard), bytes);
    ASSERT_EQ(view.state, frame::State::kFilled) << shard;
    EXPECT_EQ(view.header.shard, shard);
    EXPECT_EQ(view.header.seq, kFirst + 1);
  }
  EXPECT_EQ(InspectAt(dir_.Path(), positions[2], bytes).state, frame::State::kUnfilled);

  auto queue = WalQueue::Open(ReserveConfig(dir_.Path(), kProcess));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();
  for (core::ShardId shard = 0; shard < 3; ++shard) {
    SCOPED_TRACE(shard);
    EXPECT_EQ((*queue)->TailSeq(shard).value(), kFirst);
    auto read = (*queue)->Read(shard, kFirst, 100, 0ms, kProcess);
    ASSERT_TRUE(read.has_value()) << read.error().message();
    ASSERT_EQ(read->size(), 1U);
    EXPECT_EQ(KeyOf(read->front()), "before");
    auto appended = (*queue)->Append(shard, MakeWrite("after"));
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
    EXPECT_EQ(appended->seq, kFirst + 1);
  }
}

#endif

}  // namespace
}  // namespace abyss::queue
