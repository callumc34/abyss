#include <benchmark/benchmark.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer_pool.h"
#include "abyss/consumer/resolver_pool.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue_entry.h"
#include "abyss/engine/bounded_thread_shard_scheduler.h"
#include "abyss/engine/recovery_coordinator.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/wal_queue.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

class BenchTempDir {
 public:
  BenchTempDir() {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_recovery_bench_XXXXXX";
    std::string s = tmpl.string();
    if (::mkdtemp(s.data()) == nullptr) std::abort();
    path_ = s;
  }
  ~BenchTempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  BenchTempDir(const BenchTempDir&) = delete;
  BenchTempDir& operator=(const BenchTempDir&) = delete;
  BenchTempDir(BenchTempDir&&) = delete;
  BenchTempDir& operator=(BenchTempDir&&) = delete;
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

constexpr uint32_t kShardCount = 4;

core::QueueEntry MakeEntry(int64_t i) {
  core::QueueEntry e;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{
      .cmd = core::RespCommand{{"SET", "key" + std::to_string(i), "v"}},
  };
  return e;
}

void PreloadQueue(const std::string& wal_path, int64_t n_entries) {
  auto queue = queue::WalQueue::Open({
      .wal_path = wal_path,
      .segment_size_bytes = 64 * 1024 * 1024,
      .shard_count = kShardCount,
      .commit = {.policy = queue::FsyncPolicy::kGroupCommit,
                 .interval = 1ms,
                 .max_bytes = 1024 * 1024},
      .min_retention = 24h,
      .retention_consumers = {core::kColdConsumer, core::kResolverConsumer},
      .volatile_consumers = {core::kHotConsumer},
  });
  if (!queue.has_value()) std::abort();
  for (int64_t i = 0; i < n_entries; ++i) {
    const core::ShardId shard = static_cast<core::ShardId>(i % kShardCount);
    auto r = (*queue)->Append(shard, MakeEntry(i));
    if (!r.has_value()) std::abort();
    if (!r->durable.get().has_value()) std::abort();
  }
}

void BM_RecoveryColdHot(benchmark::State& state) {
  const int64_t n = state.range(0);
  for (auto _ : state) {
    state.PauseTiming();
    BenchTempDir dir;
    PreloadQueue(dir.path() + "/wal", n);
    state.ResumeTiming();

    // Fresh in-process recovery: open queue + cold + hot, build coordinator,
    // Run(), tear down. Measures end-to-end coordinator latency over a WAL of
    // `n` entries.
    auto queue = queue::WalQueue::Open({
        .wal_path = dir.path() + "/wal",
        .segment_size_bytes = 64 * 1024 * 1024,
        .shard_count = kShardCount,
        .commit = {.policy = queue::FsyncPolicy::kGroupCommit,
                   .interval = 1ms,
                   .max_bytes = 1024 * 1024},
        .min_retention = 24h,
        .retention_consumers = {core::kColdConsumer, core::kResolverConsumer},
        .volatile_consumers = {core::kHotConsumer},
    });
    if (!queue.has_value()) state.SkipWithError("queue open");

    auto cold = cold::backends::RocksdbStore::Create({
        .data_path = dir.path() + "/cold",
        .write_buffer_size_bytes = 64 * 1024 * 1024,
    });
    if (!cold.has_value()) state.SkipWithError("cold open");

    auto hot = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = 256 * 1024 * 1024,
        .shard_count = kShardCount,
    });
    core::ConsumerRpc rpc;
    core::ApplyNotifier notifier;
    core::EvictionPolicy eviction_policy{86400s};

    auto cold_pool = std::make_unique<consumer::ColdConsumerPool>(
        **queue, **cold, consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy);
    auto hot_pool = std::make_unique<consumer::HotConsumerPool>(
        **queue, *hot, rpc, notifier, consumer::HotConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy);
    auto resolver_pool = std::make_unique<consumer::ResolverPool>(
        **queue, **cold, *cold_pool, rpc, notifier,
        consumer::ResolverPool::Config{.shard_count = kShardCount});

    BoundedThreadShardScheduler scheduler(4);
    RecoveryCoordinator coord(**queue, *resolver_pool, *cold_pool, *hot_pool, scheduler,
                              RecoveryConfig{});
    std::atomic<bool> cancel{false};
    auto r = coord.Run(cancel);
    if (!r.has_value()) state.SkipWithError("recovery failed");
  }
  state.SetItemsProcessed(state.iterations() * n);
}

BENCHMARK(BM_RecoveryColdHot)->Arg(1000)->Arg(10000)->Arg(100000)->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace abyss::engine
