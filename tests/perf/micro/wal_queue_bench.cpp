#include <benchmark/benchmark.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/reservation.h"
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

std::unique_ptr<WalQueue> MakeQueue(const std::string& dir, core::Durability durability) {
  auto result = WalQueue::Open({
      .wal_path = dir,
      .segment_size_bytes = size_t{16} * 1024 * 1024,
      .shard_count = 1,
      .durability = durability,
      .min_retention = 1s,
  });
  if (!result.has_value()) std::abort();
  return std::move(*result);
}

bool AppendDurably(WalQueue& queue, size_t value_size) {
  auto appended = queue.Append(0, MakeEntry(value_size));
  return appended.has_value() && appended->durable.get().has_value();
}

// The ack each class gives: at publish under process_crash, after the
// covering flush under power_loss.
void BM_AppendAndWaitDurable(benchmark::State& state, core::Durability durability) {
  TempDir tmp;
  auto queue = MakeQueue(tmp.path(), durability);
  const auto value_size = static_cast<size_t>(state.range(0));

  for ([[maybe_unused]] auto _ : state) {
    if (!AppendDurably(*queue, value_size)) {
      state.SkipWithError("append failed");
      break;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BM_AppendPowerLoss(benchmark::State& state) {
  BM_AppendAndWaitDurable(state, core::Durability::kPowerLoss);
}
BENCHMARK(BM_AppendPowerLoss)->Arg(64)->Arg(1024);

void BM_AppendProcessCrash(benchmark::State& state) {
  BM_AppendAndWaitDurable(state, core::Durability::kProcessCrash);
}
BENCHMARK(BM_AppendProcessCrash)->Arg(64)->Arg(1024);

// One segment holds a whole run, so no frame waits for a spare; the
// iteration counts below keep each run inside it.
constexpr size_t kWholeRunSegment = size_t{64} << 20;

std::unique_ptr<WalQueue> MakeWholeRunQueue(const std::string& dir) {
  auto result = WalQueue::Open({
      .wal_path = dir,
      .segment_size_bytes = kWholeRunSegment,
      .shard_count = 1,
      .min_retention = 1s,
  });
  if (!result.has_value()) std::abort();
  return std::move(*result);
}

// One frame per iteration through Reserve and Complete at
// process_crash. Rebuilding a value Reserve moved out is untimed.
void BM_ReserveComplete(benchmark::State& state) {
  TempDir tmp;
  auto queue = MakeWholeRunQueue(tmp.path());
  const auto value_size = static_cast<size_t>(state.range(0));
  std::vector<core::QueueEntry> entries{MakeEntry(value_size)};
  const std::array parts{ShardEntries{.shard = 0, .entries = entries}};
  const bool moved = frame::EntryFrameSize(entries[0]) > core::kLockHoldFrameBytes;
  for ([[maybe_unused]] auto _ : state) {
    auto reserved = queue->Reserve(parts);
    if (!reserved.has_value()) {
      state.SkipWithError("reserve failed");
      break;
    }
    benchmark::DoNotOptimize(queue->Complete(std::move(*reserved)));
    if (moved) {
      state.PauseTiming();
      entries[0] = MakeEntry(value_size);
      state.ResumeTiming();
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * value_size));
}
BENCHMARK(BM_ReserveComplete)->Arg(200)->Iterations(100000);
BENCHMARK(BM_ReserveComplete)->Arg(16 << 10)->Iterations(3000);
BENCHMARK(BM_ReserveComplete)->Arg(1 << 20)->Iterations(50);

// The same through BeginAppend, which takes the entry: its rebuild is
// untimed.
void BM_AppendMovedEntry(benchmark::State& state) {
  TempDir tmp;
  auto queue = MakeWholeRunQueue(tmp.path());
  const auto value_size = static_cast<size_t>(state.range(0));
  core::QueueEntry entry = MakeEntry(value_size);
  for ([[maybe_unused]] auto _ : state) {
    auto appended = queue->Append(0, std::move(entry));
    if (!appended.has_value()) {
      state.SkipWithError("append failed");
      break;
    }
    benchmark::DoNotOptimize(appended->seq);
    state.PauseTiming();
    entry = MakeEntry(value_size);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * value_size));
}
BENCHMARK(BM_AppendMovedEntry)->Arg(200)->Iterations(100000);
BENCHMARK(BM_AppendMovedEntry)->Arg(16 << 10)->Iterations(3000);
BENCHMARK(BM_AppendMovedEntry)->Arg(1 << 20)->Iterations(50);

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

void OpenShared(core::Durability durability) {
  auto& shared = Shared();
  shared.dir = std::make_unique<TempDir>();
  shared.queue = MakeQueue(shared.dir->path(), durability);
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
    ->Name("BM_ConcurrentAppendPowerLoss")
    ->Setup([](const benchmark::State&) { OpenShared(core::Durability::kPowerLoss); })
    ->Teardown(CloseShared)
    ->Arg(64)
    ->Threads(1)
    ->Threads(4)
    ->Threads(8)
    ->UseRealTime();
BENCHMARK(BM_ConcurrentAppend)
    ->Name("BM_ConcurrentAppendProcessCrash")
    ->Setup([](const benchmark::State&) { OpenShared(core::Durability::kProcessCrash); })
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

// Cost of one tail Read at a position deep in the active segment;
// flat in the position when the segment index resolves it.
void BM_ReadAtSegmentPosition(benchmark::State& state) {
  TempDir tmp;
  const auto n = static_cast<core::SequenceId>(state.range(0));
  auto opened = WalQueue::Open({
      .wal_path = tmp.path(),
      .segment_size_bytes = kPositionSegmentBytes,
      .shard_count = 1,
      .durability = core::Durability::kProcessCrash,
      .min_retention = 1s,
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

  for ([[maybe_unused]] auto _ : state) {
    auto read = queue.Read(0, n - 1, 256, core::Duration{0}, core::Durability::kProcessCrash);
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

// Each thread appends to a shard of its own, all on one log, so they
// contend on the log's reservation tail rather than on a shard lock.
constexpr size_t kContentionShards = 64;

void OpenContention(const benchmark::State& /*state*/) {
  auto& shared = Shared();
  shared.dir = std::make_unique<TempDir>();
  auto result = WalQueue::Open({
      .wal_path = shared.dir->path(),
      .segment_size_bytes = size_t{16} * 1024 * 1024,
      .shard_count = kContentionShards,
      .log_count = 1,
      .durability = core::Durability::kProcessCrash,
      .min_retention = 1s,
  });
  if (!result.has_value()) std::abort();
  shared.queue = std::move(*result);
}

void BM_ReserveContention(benchmark::State& state) {
  WalQueue& queue = *Shared().queue;
  const auto shard = static_cast<core::ShardId>(state.thread_index() % kContentionShards);
  const core::QueueEntry entry = MakeEntry(64);
  for ([[maybe_unused]] auto _ : state) {
    auto appended = queue.Append(shard, entry);
    if (!appended.has_value()) {
      state.SkipWithError("append failed");
      break;
    }
    benchmark::DoNotOptimize(appended->seq);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_ReserveContention)
    ->Setup(OpenContention)
    ->Teardown(CloseShared)
    ->Threads(1)
    ->Threads(8)
    ->Threads(64)
    ->UseRealTime();

// 64 shards interleaved on one log. A seq inside its shard's ring is
// found at once; one behind it starts from the sparse index and steps
// over every other shard's frames in between.
constexpr size_t kInterleavedShards = 64;
constexpr size_t kInterleavedRing = 4096;
constexpr core::SequenceId kInterleavedPerShard = 4 * kInterleavedRing;
constexpr size_t kInterleavedBatch = 8;
constexpr core::SequenceId kRingMargin = 256;

void BM_ReadInterleaved(benchmark::State& state) {
  const bool from_index = state.range(0) != 0;
  TempDir tmp;
  auto opened = WalQueue::Open({
      .wal_path = tmp.path(),
      .segment_size_bytes = size_t{64} * 1024 * 1024,
      .shard_count = kInterleavedShards,
      .log_count = 1,
      .ring_entries = kInterleavedRing,
      .durability = core::Durability::kProcessCrash,
      .min_retention = 1s,
  });
  if (!opened.has_value()) {
    state.SkipWithError("open failed");
    return;
  }
  auto& queue = **opened;
  const std::vector<core::QueueEntry> batch(kInterleavedBatch, MakeEntry(kPositionValueBytes));
  for (core::SequenceId filled = 0; filled < kInterleavedPerShard; filled += kInterleavedBatch) {
    for (core::ShardId shard = 0; shard < kInterleavedShards; ++shard) {
      if (!queue.AppendBatch(shard, batch).has_value()) {
        state.SkipWithError("fill failed");
        return;
      }
    }
  }

  // Seqs stride across the range, so no read reuses a position hint.
  const core::SequenceId lo =
      from_index ? 0 : kInterleavedPerShard - kInterleavedRing + kRingMargin;
  const core::SequenceId span = from_index ? kInterleavedPerShard - kInterleavedRing - kRingMargin
                                           : kInterleavedRing - kRingMargin;
  uint64_t i = 0;
  for ([[maybe_unused]] auto _ : state) {
    const auto shard = static_cast<core::ShardId>(i % kInterleavedShards);
    const core::SequenceId seq = lo + ((i * 7919) % span);
    ++i;
    auto read = queue.Read(shard, seq, 1, core::Duration{0}, core::Durability::kProcessCrash);
    if (!read.has_value() || read->size() != 1 || read->front().seq != seq) {
      state.SkipWithError("read failed");
      return;
    }
    benchmark::DoNotOptimize(read);
  }
}
BENCHMARK(BM_ReadInterleaved)->ArgName("from_index")->Arg(0)->Arg(1)->Unit(benchmark::kMicrosecond);

}  // namespace
}  // namespace abyss::queue
