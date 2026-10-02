#include <benchmark/benchmark.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/wal_queue.h"

namespace abyss::queue {
namespace {

using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_bench_XXXXXX";
    std::string s = tmpl.string();
    if (::mkdtemp(s.data()) == nullptr) std::abort();
    path_ = s;
  }
  // NOLINTNEXTLINE(bugprone-exception-escape)
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
  TempDir(TempDir&&) = delete;
  TempDir& operator=(TempDir&&) = delete;
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

core::QueueEntry MakeEntry(size_t value_size) {
  core::QueueEntry e;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{
      .cmd = core::RespCommand{{"SET", "key", std::string(value_size, 'x')}},
  };
  return e;
}

std::unique_ptr<WalQueue> MakeQueue(const std::string& dir, FsyncPolicy policy) {
  auto result = WalQueue::Open({
      .wal_path = dir,
      .segment_size_bytes = size_t{16} * 1024 * 1024,
      .shard_count = 1,
      .commit = {.policy = policy, .interval = 1ms, .max_bytes = size_t{1024} * 1024},
      .min_retention = 1s,
  });
  if (!result.has_value()) std::abort();
  return std::move(*result);
}

bool AppendDurably(WalQueue& queue, size_t value_size) {
  auto appended = queue.Append(0, MakeEntry(value_size));
  return appended.has_value() && appended->durable.get().has_value();
}

void BM_AppendAndWaitDurable(benchmark::State& state, FsyncPolicy policy) {
  TempDir tmp;
  auto queue = MakeQueue(tmp.path(), policy);
  const auto value_size = static_cast<size_t>(state.range(0));

  for ([[maybe_unused]] auto _ : state) {
    if (!AppendDurably(*queue, value_size)) {
      state.SkipWithError("append failed");
      break;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BM_AppendPerWrite(benchmark::State& state) {
  BM_AppendAndWaitDurable(state, FsyncPolicy::kPerWrite);
}
BENCHMARK(BM_AppendPerWrite)->Arg(64)->Arg(1024);

void BM_AppendGroupCommit(benchmark::State& state) {
  BM_AppendAndWaitDurable(state, FsyncPolicy::kGroupCommit);
}
BENCHMARK(BM_AppendGroupCommit)->Arg(64)->Arg(1024);

void BM_AppendNone(benchmark::State& state) { BM_AppendAndWaitDurable(state, FsyncPolicy::kNone); }
BENCHMARK(BM_AppendNone)->Arg(64)->Arg(1024);

// One queue and shard shared by every thread of a run; Setup opens it
// before the threads start and Teardown closes it after they finish.
struct SharedQueue {
  std::unique_ptr<TempDir> dir;
  std::unique_ptr<WalQueue> queue;
};

SharedQueue& Shared() {
  static SharedQueue shared;
  return shared;
}

void OpenShared(FsyncPolicy policy) {
  auto& shared = Shared();
  shared.dir = std::make_unique<TempDir>();
  shared.queue = MakeQueue(shared.dir->path(), policy);
}

void CloseShared(const benchmark::State& /*state*/) {
  auto& shared = Shared();
  shared.queue.reset();
  shared.dir.reset();
}

void BM_ConcurrentAppend(benchmark::State& state) {
  WalQueue& queue = *Shared().queue;
  const auto value_size = static_cast<size_t>(state.range(0));

  for ([[maybe_unused]] auto _ : state) {
    if (!AppendDurably(queue, value_size)) {
      state.SkipWithError("append failed");
      break;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_ConcurrentAppend)
    ->Name("BM_ConcurrentAppendGroupCommit")
    ->Setup([](const benchmark::State&) { OpenShared(FsyncPolicy::kGroupCommit); })
    ->Teardown(CloseShared)
    ->Arg(64)
    ->Threads(1)
    ->Threads(4)
    ->Threads(8)
    ->UseRealTime();
BENCHMARK(BM_ConcurrentAppend)
    ->Name("BM_ConcurrentAppendNone")
    ->Setup([](const benchmark::State&) { OpenShared(FsyncPolicy::kNone); })
    ->Teardown(CloseShared)
    ->Arg(64)
    ->Threads(1)
    ->Threads(4)
    ->Threads(8)
    ->UseRealTime();

// Holds 1<<20 small entries without rotating.
constexpr size_t kPositionSegmentBytes = size_t{256} * 1024 * 1024;
constexpr size_t kPositionFillBatch = 1024;
constexpr size_t kPositionValueBytes = 16;

// Cost of one tail Read against the reader's position in the active
// segment: O(position) while Read re-decodes from the segment start.
void BM_ReadAtSegmentPosition(benchmark::State& state) {
  TempDir tmp;
  const auto n = static_cast<core::SequenceId>(state.range(0));
  auto opened = WalQueue::Open({
      .wal_path = tmp.path(),
      .segment_size_bytes = kPositionSegmentBytes,
      .shard_count = 1,
      .commit = {.policy = FsyncPolicy::kNone},
      .min_retention = 1s,
      .volatile_consumers = {core::kHotConsumer},
  });
  if (!opened.has_value()) {
    state.SkipWithError("open failed");
    return;
  }
  auto& queue = **opened;

  const std::vector<core::QueueEntry> batch(kPositionFillBatch, MakeEntry(kPositionValueBytes));
  for (core::SequenceId filled = 0; filled < n; filled += kPositionFillBatch) {
    const auto count = std::min<size_t>(kPositionFillBatch, n - filled);
    if (!queue.AppendBatch(0, std::span{batch}.first(count)).has_value()) {
      state.SkipWithError("fill failed");
      return;
    }
  }
  if (!queue.ListSealedSegments().empty()) {
    state.SkipWithError("fill rotated out of the active segment");
    return;
  }

  // Positions the reader at the newest entry. An explicit start seq on
  // Read replaces this Ack.
  if (!queue.Ack(core::kHotConsumer, 0, n - 2).has_value()) {
    state.SkipWithError("position failed");
    return;
  }

  for ([[maybe_unused]] auto _ : state) {
    auto read = queue.Read(core::kHotConsumer, 0, 256, core::Duration{0});
    if (!read.has_value() || read->size() != 1) {
      state.SkipWithError("tail read failed");
      return;
    }
    benchmark::DoNotOptimize(read);
  }
  state.SetComplexityN(static_cast<benchmark::ComplexityN>(n));
}
BENCHMARK(BM_ReadAtSegmentPosition)
    ->Arg(1 << 10)
    ->Arg(1 << 14)
    ->Arg(1 << 17)
    ->Arg(1 << 20)
    ->Unit(benchmark::kMicrosecond)
    ->Complexity();

}  // namespace
}  // namespace abyss::queue
