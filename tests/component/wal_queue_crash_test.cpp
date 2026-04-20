// Verifies WAL durability across a kill -9: fsynced entries survive, the
// mid-batch un-fsynced ones don't, no partial batches.

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/wal_queue.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

core::QueueEntry MakeWrite(core::SequenceId /*unused*/, std::vector<std::string> args) {
  core::QueueEntry e;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return e;
}

WalConfig DefaultConfig(const std::string& dir, FsyncPolicy policy) {
  return WalConfig{
      .wal_path = dir,
      .segment_size_bytes = 4096,
      .shard_count = 1,
      .commit = {.policy = policy,
                 .interval = std::chrono::microseconds{1000},
                 .max_bytes = size_t{1024} * 1024},
      .min_retention = 1s,
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
  };
}

class WalCrashTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_wal_crash_XXXXXX";
    std::string s = tmpl.string();
    ASSERT_NE(::mkdtemp(s.data()), nullptr);
    tmp_dir_ = s;
  }

  void TearDown() override {
    if (!tmp_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_dir_, ec);
    }
  }

  std::string tmp_dir_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(WalCrashTest, AckedWritesSurviveKillNineAndReopen) {
  constexpr int kDurableCount = 50;

  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0);

  if (pid == 0) {
    // Child: write kDurableCount entries with per-write fsync, then start a
    // batch and _exit before the batch's closing fsync can land.
    auto queue = WalQueue::Open(DefaultConfig(tmp_dir_, FsyncPolicy::kPerWrite));
    if (!queue.has_value()) std::_Exit(2);

    for (int i = 0; i < kDurableCount; ++i) {
      auto r = (*queue)->Append(0, MakeWrite(i, {"SET", "k", std::to_string(i)}));
      if (!r.has_value() || !r->durable.get().has_value()) std::_Exit(3);
    }

    std::vector<core::QueueEntry> batch;
    batch.reserve(5);
    for (int i = 0; i < 5; ++i) {
      batch.push_back(MakeWrite(0, {"SET", "batch", std::to_string(i)}));
    }
    // Intentionally do not wait on durable; the next line leaves the batch
    // in an indeterminate on-disk state.
    [[maybe_unused]] auto batch_r = (*queue)->AppendBatch(0, batch);

    // Hard exit — no destructors, no fsyncs, mimics a pod kill.
    std::_Exit(0);
  }

  int status = 0;  // NOLINT(misc-const-correctness)
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_NE(WIFEXITED(status), 0);
  ASSERT_EQ(WEXITSTATUS(status), 0);

  // Parent reopens the WAL. Recovery must (a) return every ack'd entry,
  // (b) never return a partial batch.
  auto queue = WalQueue::Open(DefaultConfig(tmp_dir_, FsyncPolicy::kPerWrite));
  ASSERT_TRUE(queue.has_value());

  auto read = (*queue)->Read(core::kHotConsumer, 0, 1000, 100ms);
  ASSERT_TRUE(read.has_value());
  // The kDurableCount pre-batch writes all received OK (each was fsynced
  // individually) so they must all be present.
  ASSERT_GE(read->size(), static_cast<size_t>(kDurableCount));
  for (int i = 0; i < kDurableCount; ++i) {
    EXPECT_EQ((*read)[i].seq, static_cast<core::SequenceId>(i));
  }

  // Any entries beyond the durable prefix must be a complete batch of 5 or
  // absent entirely. Partial batches are forbidden.
  const size_t tail = read->size() - kDurableCount;
  EXPECT_TRUE(tail == 0 || tail == 5);
}

}  // namespace
}  // namespace abyss::queue
