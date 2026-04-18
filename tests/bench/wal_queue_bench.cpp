#include <benchmark/benchmark.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/queue_entry.h"
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
      .segment_size_bytes = 16 * 1024 * 1024,
      .shard_count = 1,
      .commit = {.policy = policy, .interval = 1ms, .max_bytes = 1024 * 1024},
      .min_retention = 1s,
  });
  if (!result.has_value()) std::abort();
  return std::move(*result);
}

void BM_AppendAndWaitDurable(benchmark::State& state, FsyncPolicy policy) {
  TempDir tmp;
  auto queue = MakeQueue(tmp.path(), policy);
  const auto value_size = static_cast<size_t>(state.range(0));

  for (auto _ : state) {
    auto r = queue->Append(0, MakeEntry(value_size));
    if (!r.has_value()) state.SkipWithError("append failed");
    auto durable = r->durable.get();
    if (!durable.has_value()) state.SkipWithError("durable failed");
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

void BM_ConcurrentAppendGroupCommit(benchmark::State& state) {
  TempDir tmp;
  auto queue = MakeQueue(tmp.path(), FsyncPolicy::kGroupCommit);
  const auto value_size = static_cast<size_t>(state.range(0));

  for (auto _ : state) {
    auto r = queue->Append(0, MakeEntry(value_size));
    if (!r.has_value()) state.SkipWithError("append failed");
    auto durable = r->durable.get();
    if (!durable.has_value()) state.SkipWithError("durable failed");
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_ConcurrentAppendGroupCommit)->Arg(64)->Threads(1)->Threads(4)->Threads(8);

}  // namespace
}  // namespace abyss::queue
