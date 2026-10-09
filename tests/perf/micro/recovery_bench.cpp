#include <benchmark/benchmark.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue_entry.h"
#include "abyss/engine/bounded_thread_shard_scheduler.h"
#include "abyss/engine/recovery_coordinator.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/wal_queue.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

class BenchTempDir {
 public:
  BenchTempDir() {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_recovery_bench_XXXXXX";
    std::string s = tmpl.string();
    if (::mkdtemp(s.data()) == nullptr) std::abort();
    path_ = s;
  }
  // NOLINTNEXTLINE(bugprone-exception-escape)
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

constexpr uint32_t kShardCount = 64;
constexpr size_t kMiB = size_t{1024} * 1024;
constexpr size_t kSegmentBytes = 64 * kMiB;
// Appends per shard per batch, so the 64 streams interleave in the log.
constexpr size_t kInterleave = 16;
// Cold's commit trails FirstSeq by this many entries per shard.
constexpr core::SequenceId kColdLag = 16;
constexpr int64_t kKeySpace = 100000;

queue::WalConfig QueueConfig(const std::string& wal_path) {
  return queue::WalConfig{
      .wal_path = wal_path,
      .segment_size_bytes = kSegmentBytes,
      .shard_count = kShardCount,
      .log_count = 1,
      .durability = core::Durability::kProcessCrash,
      .min_retention = 24h,
      .retention_consumers = {core::kColdConsumer},
  };
}

core::QueueEntry MakeEntry(int64_t i) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Write{
              .cmd = core::RespCommand{{"SET", "key" + std::to_string(i % kKeySpace), "v"}}},
  };
}

// `n` entries across every shard of one log. Cold commits kColdLag
// entries into each shard, so recovery is the cold and hot rebuild.
// Returns the entry frame bytes, or nullopt on failure.
std::optional<uint64_t> PreloadQueue(const std::string& wal_path, int64_t n) {
  auto opened = queue::WalQueue::Open(QueueConfig(wal_path));
  if (!opened.has_value()) return std::nullopt;
  auto& queue = **opened;
  uint64_t frame_bytes = 0;
  std::vector<std::byte> encoded;
  std::vector<core::QueueEntry> batch(kInterleave);
  for (int64_t i = 0; i < n;) {
    for (core::ShardId shard = 0; shard < kShardCount && i < n; ++shard) {
      for (auto& entry : batch) {
        entry = MakeEntry(i++);
        encoded.clear();
        frame_bytes += queue::frame::EncodeEntry(entry, shard, encoded);
      }
      if (!queue.AppendBatch(shard, batch).has_value()) return std::nullopt;
    }
  }
  for (core::ShardId shard = 0; shard < kShardCount; ++shard) {
    const auto tail = queue.TailSeq(shard);
    if (!tail.has_value()) return std::nullopt;
    auto durable = queue.AwaitDurable(shard, *tail, core::Durability::kPowerLoss, 60s);
    if (!durable.has_value() || !*durable) return std::nullopt;
    if (!queue.CommitOffset(core::kColdConsumer, shard, kColdLag - 1).has_value()) {
      return std::nullopt;
    }
  }
  if (!queue.FlushOffsets().has_value()) return std::nullopt;
  return frame_bytes;
}

double ScanBytes() {
  return metrics::testing::GetCounterValue(metrics::names::kWalScanBytesTotal).value_or(0);
}

// RC1: Open, then rebuild hot and cold through one scan. Reports both
// times and how much of the retained log the scan read, which must be
// about once.
void BM_RecoveryColdHot(benchmark::State& state) {
  const int64_t n = state.range(0);
  double open_ms = 0;
  double rebuild_ms = 0;
  double scan_ratio = 0;
  for ([[maybe_unused]] auto _ : state) {
    state.PauseTiming();
    BenchTempDir dir;
    const auto retained = PreloadQueue(dir.path() + "/wal", n);
    if (!retained.has_value()) {
      state.SkipWithError("preload failed");
      return;
    }
    const double scanned_before = ScanBytes();
    state.ResumeTiming();

    const auto opening = Clock::now();
    auto queue = queue::WalQueue::Open(QueueConfig(dir.path() + "/wal"));
    const auto opened = Clock::now();
    if (!queue.has_value()) {
      state.SkipWithError("queue open");
      return;
    }

    auto cold = cold::backends::RocksdbStore::Create({
        .data_path = dir.path() + "/cold",
        .shard_count = kShardCount,
        .write_buffer_size_bytes = 64 * kMiB,
    });
    if (!cold.has_value()) {
      state.SkipWithError("cold open");
      return;
    }
    auto hot = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = 1024 * kMiB,
        .shard_count = kShardCount,
    });
    core::EvictionPolicy eviction_policy{86400s};
    auto cold_pool = std::make_unique<consumer::ColdConsumerPool>(
        **queue, **cold, consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy);

    BoundedThreadShardScheduler scheduler(4);
    RecoveryCoordinator coord(**queue, *cold_pool, *hot, scheduler, RecoveryConfig{});
    const std::atomic<bool> cancel{false};
    const auto rebuilding = Clock::now();
    auto r = coord.Run(cancel);
    const auto rebuilt = Clock::now();
    if (!r.has_value()) {
      state.SkipWithError("recovery failed");
      return;
    }

    state.PauseTiming();
    open_ms = std::chrono::duration<double, std::milli>(opened - opening).count();
    rebuild_ms = std::chrono::duration<double, std::milli>(rebuilt - rebuilding).count();
    // Padding at each segment end is walked too, so a little over 1.
    scan_ratio = (ScanBytes() - scanned_before) / static_cast<double>(*retained);
    if (scan_ratio < 1.0 || scan_ratio > 1.05) {
      state.SkipWithError("the scan read " + std::to_string(scan_ratio) +
                          "x the retained frame bytes, not about once");
      return;
    }
    state.ResumeTiming();
  }
  state.counters["open_ms"] = open_ms;
  state.counters["rebuild_ms"] = rebuild_ms;
  state.counters["scan_ratio"] = scan_ratio;
  state.SetItemsProcessed(state.iterations() * n);
}

BENCHMARK(BM_RecoveryColdHot)
    ->Arg(1'000'000)
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

}  // namespace
}  // namespace abyss::engine
