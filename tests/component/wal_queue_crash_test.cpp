// WAL durability across a kill -9, under both durability classes.
//
// Appends land in the segment's write watermark immediately, and a group
// commit makes some prefix of them power-durable later. Every ack-vs-flush
// ordering defect the audit found lives in that window. A kill -9 is a
// process crash, which both classes survive, so each run distinguishes the
// two kinds of write explicitly:
//
//   * CONFIRMED: the caller waited on the append's durability future and it
//     resolved OK (at publish under process_crash, after the flush under
//     power_loss). The client would have been told OK. These MUST survive.
//   * IN-FLIGHT: appended, never awaited. These may or may not survive; the
//     client never saw OK, so either outcome is correct.
//
// The victim publishes the confirmed watermark before parking, so the parent
// asserts against what was actually promised rather than against a guess.
//
// The recovered log must also be structurally sound regardless of where the
// crash truncated it: sequence ids contiguous from the start, no duplicates,
// and no partial AppendBatch (format 1.1 batch atomicity).

#include <gtest/gtest.h>

#include <string>

#ifndef _WIN32

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <future>
#include <span>
#include <sstream>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/platform/fs.h"
#include "abyss/platform/mapped_file.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/wal_queue.h"
#include "crash_harness.h"
#include "durability_printer.h"

#endif

namespace abyss::queue {
namespace {

#ifdef _WIN32

TEST(WalCrashTest, ConfirmedWritesSurviveKillNine) {
  GTEST_SKIP() << "out-of-process crash simulation is POSIX-only";
}

TEST(WalCrashTest, AcknowledgedWritesOnEveryShardSurviveKillNine) {
  GTEST_SKIP() << "out-of-process crash simulation is POSIX-only";
}

#else

using namespace std::chrono_literals;

constexpr const char* kVictimDirEnv = "ABYSS_WAL_CRASH_VICTIM_DIR";
constexpr const char* kReadyFile = "wal_victim.ready";

const char* VictimFilter(core::Durability durability) {
  return durability == core::Durability::kPowerLoss ? "WalCrashVictim.PowerLoss"
                                                    : "WalCrashVictim.ProcessCrash";
}

// Confirmed one at a time so the durable watermark is unambiguous, then a burst
// left in flight so the crash lands inside a group-commit window.
constexpr int kConfirmedCount = 40;
// Large enough that the parent's kill reliably lands inside the stream rather
// than after it. The victim parks if it somehow finishes first.
constexpr int kInFlightCount = 2000000;
constexpr size_t kBatchSize = 5;

core::QueueEntry MakeWrite(std::vector<std::string> args) {
  core::QueueEntry e;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return e;
}

WalConfig VictimConfig(const std::filesystem::path& dir, core::Durability durability) {
  return WalConfig{
      .wal_path = dir.string(),
      .segment_size_bytes = 8192,
      .shard_count = 1,
      .durability = durability,
      .min_retention = 1s,
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
  };
}

void RunVictim(core::Durability durability) {
  bool is_victim = false;
  const auto dir = testing::VictimDirFromEnv(kVictimDirEnv, &is_victim);
  if (!is_victim) {
    GTEST_SKIP() << "crash victim; driven out-of-process by WalCrashTest";
  }

  auto queue = WalQueue::Open(VictimConfig(dir, durability));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();

  // Confirmed writes: await durability, so the client would have seen OK.
  core::SequenceId highest_confirmed = 0;
  bool any_confirmed = false;
  for (int i = 0; i < kConfirmedCount; ++i) {
    auto r = (*queue)->Append(0, MakeWrite({"SET", "confirmed", std::to_string(i)}));
    ASSERT_TRUE(r.has_value()) << r.error().message();
    ASSERT_TRUE(r->durable.get().has_value()) << "durability future reported failure";
    highest_confirmed = r->seq;
    any_confirmed = true;
  }
  ASSERT_TRUE(any_confirmed);

  // Signal BEFORE the in-flight stream, then keep appending until killed. This
  // is load-bearing: if the victim went quiet before signalling, the group
  // committer would drain every pending append while the parent was still
  // polling for the marker, the recovered log would always be complete, and the
  // truncation assertions below would never be exercised at all. Crashing
  // mid-stream is what puts real un-fsynced entries at the tail.
  std::ostringstream payload;
  payload << highest_confirmed;
  testing::SignalReady(dir, kReadyFile, payload.str());

  // Never awaited. Interleaves single appends and batches so the kill can land
  // mid-batch, which is what exercises batch atomicity. Bounded only so a
  // delayed kill cannot spin forever; the parent kills long before this.
  for (int i = 0; i < kInFlightCount; ++i) {
    if (i % 8 == 0) {
      std::vector<core::QueueEntry> batch;
      batch.reserve(kBatchSize);
      for (size_t b = 0; b < kBatchSize; ++b) {
        batch.push_back(MakeWrite({"SET", "batch", std::to_string(i) + "_" + std::to_string(b)}));
      }
      [[maybe_unused]] auto batch_r = (*queue)->AppendBatch(0, batch);
    } else {
      [[maybe_unused]] auto r =
          (*queue)->Append(0, MakeWrite({"SET", "flight", std::to_string(i)}));
    }
  }

  // Ran out of work before the kill landed; hold everything open and wait.
  for (;;) {
    ::pause();
  }
}

TEST(WalCrashVictim, ProcessCrash) { RunVictim(core::Durability::kProcessCrash); }
TEST(WalCrashVictim, PowerLoss) { RunVictim(core::Durability::kPowerLoss); }

// 64 shards on one log, 64 KiB segments so the kill lands among
// rotations and rolls, and 8 appenders each spreading singles and
// batches over every shard. Each appender records the highest seq it
// was acknowledged on each shard in a MAP_SHARED file, whose dirty
// pages outlive the SIGKILL in the page cache. Acknowledged is the
// append returning at process_crash, its future resolving at
// power_loss.
constexpr core::ShardId kShards = 64;
constexpr size_t kAppenders = 8;
constexpr size_t kMaxBatch = 5;
constexpr const char* kAckedFile = "acked.slots";
constexpr size_t kAckedSlots = kAppenders * kShards;
// Some 40 segments in; the kill lands soon after.
constexpr uint64_t kReadyAfterOps = 20000;
// Bounds a victim whose kill is late.
constexpr uint64_t kMaxOpsPerAppender = 250000;
// Unresolved power_loss futures an appender holds before it waits.
constexpr size_t kAwaitWindow = 32;
constexpr auto kAwaitTimeout = 10s;

const char* ShardsVictimFilter(core::Durability durability) {
  return durability == core::Durability::kPowerLoss ? "WalCrashVictim.ShardsPowerLoss"
                                                    : "WalCrashVictim.ShardsProcessCrash";
}

WalConfig ShardsConfig(const std::filesystem::path& dir, core::Durability durability) {
  return WalConfig{
      .wal_path = dir.string(),
      .segment_size_bytes = size_t{64} << 10,
      .shard_count = kShards,
      .ring_entries = 4096,
      .durability = durability,
      .min_retention = 1s,
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
  };
}

// Slot appender * kShards + shard: that appender's highest acknowledged
// seq on the shard plus one, or zero.
class AckedSlots {
 public:
  explicit AckedSlots(const std::filesystem::path& path) {
    auto file = platform::fs::Open(
        path, {.mode = platform::fs::OpenMode::kReadWrite, .create = true, .truncate = true});
    if (!file.has_value()) return;
    if (!platform::fs::Ftruncate(*file, kAckedSlots * sizeof(uint64_t)).has_value()) return;
    auto map = platform::fs::MappedFile::Map(*file, kAckedSlots * sizeof(uint64_t));
    if (!map.has_value()) return;
    map_ = std::move(*map);
    slots_ = {reinterpret_cast<uint64_t*>(map_.data()), kAckedSlots};
  }

  bool ok() const { return !slots_.empty(); }

  void Record(size_t appender, core::ShardId shard, core::SequenceId seq) {
    std::atomic_ref<uint64_t>(slots_[(appender * kShards) + shard])
        .store(seq + 1, std::memory_order_relaxed);
  }

 private:
  platform::fs::MappedFile map_;
  std::span<uint64_t> slots_;
};

// Batch members carry {appender, op} in the key and pos/size in the
// value, so the parent can tell a whole batch from part of one.
std::vector<core::QueueEntry> MakeOp(size_t appender, uint64_t op, size_t size) {
  std::vector<core::QueueEntry> entries;
  entries.reserve(size);
  const std::string key = "a" + std::to_string(appender) + "_" + std::to_string(op);
  for (size_t pos = 0; pos < size; ++pos) {
    entries.push_back(MakeWrite({"SET", key, std::to_string(pos) + "/" + std::to_string(size)}));
  }
  return entries;
}

struct Unsettled {
  core::ShardId shard = 0;
  core::SequenceId last = 0;
  DurabilityFuture durable;
};

// Returns when its ops run out, an append fails, or a future stays
// unresolved for kAwaitTimeout; the parent kills it long before.
void RunShardsAppender(WalQueue& queue, size_t appender, AckedSlots& acked,
                       std::atomic<uint64_t>& ops, const std::filesystem::path& dir) {
  uint64_t rng = (appender + 1) * 0x9e3779b97f4a7c15ULL;
  std::deque<Unsettled> unsettled;
  const auto settle = [&](bool wait) {
    while (!unsettled.empty()) {
      Unsettled& front = unsettled.front();
      if (front.durable.wait_for(wait ? kAwaitTimeout : 0s) != std::future_status::ready) {
        return !wait;
      }
      if (!front.durable.get().has_value()) return false;
      acked.Record(appender, front.shard, front.last);
      unsettled.pop_front();
      wait = false;
    }
    return true;
  };
  for (uint64_t op = 0; op < kMaxOpsPerAppender; ++op) {
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    const auto shard = static_cast<core::ShardId>(rng % kShards);
    const size_t size = (rng >> 8) % 3 == 0 ? 1 + ((rng >> 16) % kMaxBatch) : 1;
    const auto entries = MakeOp(appender, op, size);
    if (size == 1) {
      auto appended = queue.Append(shard, entries.front());
      if (!appended.has_value()) return;
      unsettled.push_back({shard, appended->seq, std::move(appended->durable)});
    } else {
      auto appended = queue.AppendBatch(shard, entries);
      if (!appended.has_value()) return;
      unsettled.push_back({shard, appended->last_seq, std::move(appended->durable)});
    }
    if (!settle(unsettled.size() > kAwaitWindow)) return;
    if (ops.fetch_add(1, std::memory_order_relaxed) + 1 == kReadyAfterOps) {
      testing::SignalReady(dir, kReadyFile, "");
    }
  }
}

void RunShardsVictim(core::Durability durability) {
  bool is_victim = false;
  const auto dir = testing::VictimDirFromEnv(kVictimDirEnv, &is_victim);
  if (!is_victim) {
    GTEST_SKIP() << "crash victim; driven out-of-process by WalCrashTest";
  }
  AckedSlots acked(dir / kAckedFile);
  ASSERT_TRUE(acked.ok());
  auto queue = WalQueue::Open(ShardsConfig(dir, durability));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();

  std::atomic<uint64_t> ops{0};
  std::vector<std::thread> appenders;
  appenders.reserve(kAppenders);
  for (size_t a = 0; a < kAppenders; ++a) {
    appenders.emplace_back([&, a] { RunShardsAppender(**queue, a, acked, ops, dir); });
  }
  for (auto& appender : appenders) appender.join();
  // Exiting before the ready marker fails the parent at once.
  ASSERT_GE(ops.load(), kReadyAfterOps) << "the appenders stopped early";
  for (;;) {
    ::pause();
  }
}

TEST(WalCrashVictim, ShardsProcessCrash) { RunShardsVictim(core::Durability::kProcessCrash); }
TEST(WalCrashVictim, ShardsPowerLoss) { RunShardsVictim(core::Durability::kPowerLoss); }

class WalCrashTest : public ::testing::TestWithParam<core::Durability> {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_wal_crash_XXXXXX";
    std::string path = tmpl.string();
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    tmp_dir_ = path;
  }

  void TearDown() override {
    if (!tmp_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_dir_, ec);
    }
  }

  std::filesystem::path
      tmp_dir_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_P(WalCrashTest, ConfirmedWritesSurviveKillNine) {
  const auto outcome = testing::SpawnAndKillVictim(testing::VictimSpec{
      .gtest_filter = VictimFilter(GetParam()),
      .dir_env_var = kVictimDirEnv,
      .dir = tmp_dir_,
      .ready_file_name = kReadyFile,
  });

  ASSERT_TRUE(outcome.error.empty()) << outcome.error;
  ASSERT_TRUE(outcome.reached_ready) << "crash victim never reached its ready point";
  ASSERT_TRUE(outcome.died_by_signal)
      << "victim must die by signal: a clean exit would flush on the way out and the test would "
         "prove nothing about the group-commit window";
  EXPECT_EQ(outcome.term_signal, SIGKILL);

  core::SequenceId highest_confirmed = 0;
  {
    std::istringstream in(outcome.ready_payload);
    in >> highest_confirmed;
    ASSERT_FALSE(in.fail()) << "victim report unreadable: '" << outcome.ready_payload << "'";
  }

  auto queue = WalQueue::Open(VictimConfig(tmp_dir_, GetParam()));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();

  auto read = (*queue)->Read(0, 0, 10000, 100ms, core::Durability::kPowerLoss);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  const auto& entries = *read;

  // (1) Every write the client was told was durable survived the kill. This is
  // invariant 2 -- a client never receives OK for a write that is then lost.
  ASSERT_FALSE(entries.empty());
  EXPECT_GE(entries.back().seq, highest_confirmed)
      << "recovery lost a write whose durability future had already resolved OK";

  // (2) The recovered log is structurally sound wherever it was truncated:
  // seq ids start at 0 and are strictly contiguous. A gap would mean recovery
  // exposed an entry past a hole (unreachable data below it); a duplicate would
  // mean the tail was replayed twice. The old assertion only spot-checked the
  // confirmed prefix by index and could not see either.
  for (size_t i = 0; i < entries.size(); ++i) {
    ASSERT_EQ(entries[i].seq, static_cast<core::SequenceId>(i))
        << "sequence ids are not contiguous from 0 at index " << i << " (gap or duplicate)";
  }

  // (3) No partial batch survived. Every batch is 5 entries appended together;
  // format 1.1 advances the durable tail only at a batch's closing entry, so a
  // recovered log must never end part-way through one.
  size_t batch_entries = 0;
  for (const auto& e : entries) {
    const auto* w = std::get_if<core::entry::Write>(&e.payload);
    ASSERT_NE(w, nullptr);
    if (w->cmd.args.size() >= 2 && w->cmd.args[1] == "batch") ++batch_entries;
  }
  EXPECT_EQ(batch_entries % kBatchSize, 0U)
      << "recovery exposed a partial AppendBatch (" << batch_entries
      << " batch entries is not a multiple of " << kBatchSize << ")";
}

// Every append acknowledged before the kill survives on its shard; each
// shard's log is contiguous from FirstSeq and holds only whole batches.
TEST_P(WalCrashTest, AcknowledgedWritesOnEveryShardSurviveKillNine) {
  const auto outcome = testing::SpawnAndKillVictim(testing::VictimSpec{
      .gtest_filter = ShardsVictimFilter(GetParam()),
      .dir_env_var = kVictimDirEnv,
      .dir = tmp_dir_,
      .ready_file_name = kReadyFile,
      .ready_deadline = 20s,
  });
  ASSERT_TRUE(outcome.error.empty()) << outcome.error;
  ASSERT_TRUE(outcome.reached_ready) << "crash victim never reached its ready point";
  ASSERT_TRUE(outcome.died_by_signal);
  EXPECT_EQ(outcome.term_signal, SIGKILL);

  std::vector<uint64_t> slots(kAckedSlots, 0);
  {
    std::ifstream in(tmp_dir_ / kAckedFile, std::ios::binary);
    in.read(reinterpret_cast<char*>(slots.data()),
            static_cast<std::streamsize>(slots.size() * sizeof(uint64_t)));
    ASSERT_TRUE(in.good()) << "the acknowledged slots are unreadable";
  }
  std::vector<core::SequenceId> acked_end(kShards, 0);
  uint64_t acked_shards = 0;
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    for (size_t a = 0; a < kAppenders; ++a) {
      acked_end[shard] = std::max(acked_end[shard], slots[(a * kShards) + shard]);
    }
    if (acked_end[shard] > 0) ++acked_shards;
  }
  ASSERT_EQ(acked_shards, kShards) << "the victim was killed before every shard was acknowledged";

  auto queue = WalQueue::Open(ShardsConfig(tmp_dir_, GetParam()));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    SCOPED_TRACE("shard " + std::to_string(shard));
    const core::SequenceId first = (*queue)->FirstSeq(shard).value();
    ASSERT_EQ(first, 0U) << "nothing was committed, so nothing may be reclaimed";
    const core::SequenceId end = (*queue)->DurableEnd(shard, core::Durability::kPowerLoss).value();
    // Open synced the recovered log, so it is all power-durable.
    ASSERT_EQ(end, (*queue)->TailSeq(shard).value() + 1);
    EXPECT_GE(end, acked_end[shard]) << "recovery lost an acknowledged write";

    core::SequenceId next = first;
    std::string open_batch;
    size_t batch_size = 0;
    size_t batch_pos = 0;
    while (next < end) {
      auto read = (*queue)->Read(shard, next, 4096, 0ms, core::Durability::kPowerLoss);
      ASSERT_TRUE(read.has_value()) << read.error().message();
      ASSERT_FALSE(read->empty()) << "seq " << next << " is below the end but unreadable";
      for (const auto& entry : *read) {
        ASSERT_EQ(entry.seq, next) << "a gap or duplicate in the recovered log";
        ++next;
        const auto* write = std::get_if<core::entry::Write>(&entry.payload);
        ASSERT_NE(write, nullptr);
        ASSERT_EQ(write->cmd.args.size(), 3U);
        const std::string& key = write->cmd.args[1];
        const std::string& value = write->cmd.args[2];
        const size_t slash = value.find('/');
        ASSERT_NE(slash, std::string::npos) << value;
        const size_t pos = std::stoul(value.substr(0, slash));
        const size_t size = std::stoul(value.substr(slash + 1));
        if (batch_pos == batch_size) {
          ASSERT_EQ(pos, 0U) << "seq " << entry.seq << " starts inside a batch";
          open_batch = key;
          batch_size = size;
          batch_pos = 0;
        }
        ASSERT_EQ(key, open_batch) << "seq " << entry.seq << " interleaves two batches";
        ASSERT_EQ(pos, batch_pos);
        ASSERT_EQ(size, batch_size);
        ++batch_pos;
      }
    }
    EXPECT_EQ(batch_pos, batch_size) << "recovery exposed part of batch " << open_batch;
  }
}

INSTANTIATE_TEST_SUITE_P(Durability, WalCrashTest,
                         ::testing::Values(core::Durability::kProcessCrash,
                                           core::Durability::kPowerLoss),
                         [](const ::testing::TestParamInfo<core::Durability>& info) {
                           return info.param == core::Durability::kPowerLoss ? "PowerLoss"
                                                                             : "ProcessCrash";
                         });

#endif  // !_WIN32

}  // namespace
}  // namespace abyss::queue
