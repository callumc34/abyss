// WAL durability across a kill -9, exercised under GROUP COMMIT.
//
// Group commit is the default policy and the one the durability spine was built
// for: appends land in the segment's in-memory write watermark immediately, and
// a batched fsync makes some prefix of them durable later. Every ack-vs-fsync
// ordering defect the audit found lives in that window. A per-write-fsync test
// cannot reach it -- it collapses the window to zero -- so this test drives
// kGroupCommit and distinguishes the two classes of write explicitly:
//
//   * CONFIRMED: the caller waited on the append's durability future and it
//     resolved OK. The client would have been told OK. These MUST survive.
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

#include <chrono>
#include <filesystem>
#include <sstream>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/wal_queue.h"
#include "crash_harness.h"

#endif

namespace abyss::queue {
namespace {

#ifdef _WIN32

TEST(WalCrashTest, ConfirmedWritesSurviveKillNineUnderGroupCommit) {
  GTEST_SKIP() << "out-of-process crash simulation is POSIX-only";
}

#else

using namespace std::chrono_literals;

constexpr const char* kVictimDirEnv = "ABYSS_WAL_CRASH_VICTIM_DIR";
constexpr const char* kVictimFilter = "WalCrashVictim.Run";
constexpr const char* kReadyFile = "wal_victim.ready";

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

WalConfig VictimConfig(const std::filesystem::path& dir) {
  return WalConfig{
      .wal_path = dir.string(),
      .segment_size_bytes = 4096,
      .shard_count = 1,
      // The whole point: a real batching window between append and fsync.
      .commit = {.policy = FsyncPolicy::kGroupCommit,
                 .interval = std::chrono::microseconds{2000},
                 .max_bytes = size_t{1024} * 1024},
      .min_retention = 1s,
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
  };
}

TEST(WalCrashVictim, Run) {
  bool is_victim = false;
  const auto dir = testing::VictimDirFromEnv(kVictimDirEnv, &is_victim);
  if (!is_victim) {
    GTEST_SKIP() << "crash victim; driven out-of-process by WalCrashTest";
  }

  auto queue = WalQueue::Open(VictimConfig(dir));
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

class WalCrashTest : public ::testing::Test {
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

TEST_F(WalCrashTest, ConfirmedWritesSurviveKillNineUnderGroupCommit) {
  const auto outcome = testing::SpawnAndKillVictim(testing::VictimSpec{
      .gtest_filter = kVictimFilter,
      .dir_env_var = kVictimDirEnv,
      .dir = tmp_dir_,
      .ready_file_name = kReadyFile,
  });

  ASSERT_TRUE(outcome.error.empty()) << outcome.error;
  ASSERT_TRUE(outcome.reached_ready) << "crash victim never reached its ready point";
  ASSERT_TRUE(outcome.died_by_signal)
      << "victim must die by signal: a clean exit would fsync on the way out and the test would "
         "prove nothing about the group-commit window";
  EXPECT_EQ(outcome.term_signal, SIGKILL);

  core::SequenceId highest_confirmed = 0;
  {
    std::istringstream in(outcome.ready_payload);
    in >> highest_confirmed;
    ASSERT_FALSE(in.fail()) << "victim report unreadable: '" << outcome.ready_payload << "'";
  }

  auto queue = WalQueue::Open(VictimConfig(tmp_dir_));
  ASSERT_TRUE(queue.has_value()) << queue.error().message();

  auto read = (*queue)->Read(0, 0, 10000, 100ms);
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

#endif  // !_WIN32

}  // namespace
}  // namespace abyss::queue
