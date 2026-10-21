#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/shard_router.h"
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

// What a run replays, into how much hot memory, and whether the log
// is read from the device rather than the page cache.
struct Shape {
  int64_t entries = 0;
  int64_t keys = 0;
  size_t value_bytes = 0;
  size_t hot_bytes = 0;
  bool drop_cache = false;
};

Shape ShapeOf(const benchmark::State& state) {
  return Shape{.entries = state.range(0),
               .keys = state.range(1),
               .value_bytes = static_cast<size_t>(state.range(2)),
               .hot_bytes = static_cast<size_t>(state.range(3)) * kMiB,
               .drop_cache = state.range(4) != 0};
}

// Drops the clean pages of every file under `dir` from the page cache,
// as a reboot would, and returns the bytes still resident. macOS's
// `purge` needs root; msync(MS_INVALIDATE) drops one file's pages.
std::optional<uint64_t> DropPageCache(const std::filesystem::path& dir) {
  const auto page = static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
  uint64_t resident = 0;
  std::error_code ec;
  for (const auto& file : std::filesystem::recursive_directory_iterator(dir, ec)) {
    if (!file.is_regular_file() || file.file_size() == 0) continue;
    const auto size = static_cast<size_t>(file.file_size());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    const int fd = ::open(file.path().c_str(), O_RDONLY);
    if (fd < 0) return std::nullopt;
#ifdef __linux__
    (void)::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
#endif
    void* map = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    ::close(fd);
    if (map == MAP_FAILED) return std::nullopt;
#ifndef __linux__
    (void)::msync(map, size, MS_INVALIDATE);
#endif
#ifdef __APPLE__
    std::vector<char> pages((size + page - 1) / page);
#else
    std::vector<unsigned char> pages((size + page - 1) / page);
#endif
    if (::mincore(map, size, pages.data()) == 0) {
      for (const auto p : pages) resident += (static_cast<unsigned>(p) & 1U) != 0 ? page : 0;
    }
    ::munmap(map, size);
  }
  if (ec) return std::nullopt;
  return resident;
}

// A SET, flagged as the sequencer logs one: it replaces the key's
// state, so replay installs it whether or not hot holds the key.
core::QueueEntry MakeEntry(int64_t i, const Shape& shape) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Write{.cmd =
                                 core::RespCommand{{"SET", "key" + std::to_string(i % shape.keys),
                                                    std::string(shape.value_bytes, 'v')}}},
      .replaces_state = true,
  };
}

// A SADD or ZADD of four members creating key `i`, flagged as the
// sequencer logs a write that creates its key when `replaces` is set.
core::QueueEntry MakeCollectionEntry(int64_t i, bool replaces) {
  std::string key = "c" + std::to_string(i);
  core::RespCommand cmd =
      i % 2 == 0
          ? core::RespCommand{{"SADD", std::move(key), "m1", "m2", "m3", "m4"}}
          : core::RespCommand{{"ZADD", std::move(key), "1", "m1", "2", "m2", "3", "m3", "4", "m4"}};
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = std::move(cmd)},
      .replaces_state = replaces,
  };
}

// `n` entries from `make` across every shard of one log. Cold commits
// kColdLag entries into each shard, so recovery is the cold and hot
// rebuild. Returns the entry frame bytes, or nullopt on failure.
std::optional<uint64_t> PreloadQueue(const std::string& wal_path, int64_t n,
                                     const std::function<core::QueueEntry(int64_t)>& make) {
  auto opened = queue::WalQueue::Open(QueueConfig(wal_path));
  if (!opened.has_value()) return std::nullopt;
  auto& queue = **opened;
  uint64_t frame_bytes = 0;
  std::vector<std::byte> encoded;
  // Each entry on its key's shard, as the sequencer logs it; replay
  // refuses an entry on another shard's stream.
  std::vector<std::vector<core::QueueEntry>> pending(kShardCount);
  const auto append = [&queue, &pending](core::ShardId shard) {
    if (pending[shard].empty()) return true;
    const bool ok = queue.AppendBatch(shard, pending[shard]).has_value();
    pending[shard].clear();
    return ok;
  };
  for (int64_t i = 0; i < n; ++i) {
    core::QueueEntry entry = make(i);
    const auto& cmd = std::get<core::entry::Write>(entry.payload).cmd;
    const core::ShardId shard = core::ComputeShard(cmd.args[1], kShardCount);
    encoded.clear();
    frame_bytes += queue::frame::EncodeEntry(entry, shard, encoded);
    pending[shard].push_back(std::move(entry));
    if (pending[shard].size() == kInterleave && !append(shard)) return std::nullopt;
  }
  for (core::ShardId shard = 0; shard < kShardCount; ++shard) {
    if (!append(shard)) return std::nullopt;
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
// about once. Arguments: entries, distinct keys, value bytes, hot MiB,
// and whether the log's pages are dropped from the page cache first; a
// log larger than hot evicts during replay. Cold commits almost nothing
// before the crash, so it absorbs the whole log.
void BM_RecoveryColdHot(benchmark::State& state) {
  const Shape shape = ShapeOf(state);
  const int64_t n = shape.entries;
  double open_ms = 0;
  double rebuild_ms = 0;
  double scan_ratio = 0;
  double log_mib = 0;
  double hot_keys = 0;
  double hot_mib = 0;
  double cached_mib = 0;
  for ([[maybe_unused]] auto _ : state) {
    state.PauseTiming();
    BenchTempDir dir;
    const auto retained = PreloadQueue(dir.path() + "/wal", shape.entries,
                                       [&shape](int64_t i) { return MakeEntry(i, shape); });
    if (!retained.has_value()) {
      state.SkipWithError("preload failed");
      return;
    }
    if (shape.drop_cache) {
      const auto resident = DropPageCache(dir.path() + "/wal");
      if (!resident.has_value()) {
        state.SkipWithError("dropping the log's pages failed");
        return;
      }
      cached_mib = static_cast<double>(*resident) / static_cast<double>(kMiB);
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
    core::EvictionPolicy eviction_policy{86400s};
    auto cold_pool = std::make_unique<consumer::ColdConsumerPool>(
        **queue, **cold, consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy);
    // As the server wires it: hot evicts nothing cold has not drained.
    auto hot = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = shape.hot_bytes,
        .shard_count = kShardCount,
        .drained = [pool = cold_pool.get()](core::ShardId shard) -> core::SequenceId {
          return pool->ConsumerFor(shard).LatestDrainedSeq();
        },
    });

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
    log_mib = static_cast<double>(*retained) / static_cast<double>(kMiB);
    if (auto stats = hot->Stats(); stats.has_value()) {
      hot_keys = static_cast<double>(stats->key_count);
      hot_mib = static_cast<double>(stats->used_bytes) / static_cast<double>(kMiB);
    }
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
  state.counters["log_mib"] = log_mib;
  state.counters["hot_keys"] = hot_keys;
  state.counters["hot_mib"] = hot_mib;
  state.counters["cached_mib"] = cached_mib;
  state.SetItemsProcessed(state.iterations() * n);
}

// Cold's half of a rebuild over a log of SADDs and ZADDs, each creating
// its own key: absorbing the scan into the buffer, then flushing it to
// RocksDB. `replaces` flags each write as creating its key, so cold
// applies it as a DEL and then the write. Arguments: entries, replaces.
void BM_ColdReplayCollections(benchmark::State& state) {
  const int64_t n = state.range(0);
  const bool replaces = state.range(1) != 0;
  double absorb_ms = 0;
  double flush_ms = 0;
  double log_mib = 0;
  for ([[maybe_unused]] auto _ : state) {
    state.PauseTiming();
    BenchTempDir dir;
    const auto retained = PreloadQueue(
        dir.path() + "/wal", n, [replaces](int64_t i) { return MakeCollectionEntry(i, replaces); });
    if (!retained.has_value()) {
      state.SkipWithError("preload failed");
      return;
    }
    auto queue = queue::WalQueue::Open(QueueConfig(dir.path() + "/wal"));
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
    core::EvictionPolicy eviction_policy{86400s};
    consumer::ColdConsumerPool pool(**queue, **cold,
                                    consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
                                    eviction_policy);
    std::vector<core::SequenceId> from(kShardCount);
    std::vector<core::SequenceId> end(kShardCount);
    for (core::ShardId s = 0; s < kShardCount; ++s) {
      auto begun = pool.ConsumerFor(s).BeginReplay();
      auto durable = (*queue)->DurableEnd(s, core::Durability::kProcessCrash);
      if (!begun.has_value() || !durable.has_value()) {
        state.SkipWithError("replay setup");
        return;
      }
      from[s] = *begun;
      end[s] = *durable;
    }
    const std::atomic<bool> cancel{false};
    state.ResumeTiming();

    const auto absorbing = Clock::now();
    auto scanned = (*queue)->Scan(
        from, end, 4,
        [&pool, &cancel](core::ShardId shard, std::vector<core::QueueEntry>& batch) {
          return pool.ConsumerFor(shard).ApplyReplayBatch(batch, cancel);
        },
        cancel);
    const auto flushing = Clock::now();
    if (!scanned.has_value()) {
      state.SkipWithError("cold replay failed");
      return;
    }
    for (core::ShardId s = 0; s < kShardCount; ++s) {
      if (!pool.ConsumerFor(s).FinishReplay(end[s], cancel).has_value()) {
        state.SkipWithError("cold flush failed");
        return;
      }
    }
    const auto flushed = Clock::now();

    state.PauseTiming();
    absorb_ms = std::chrono::duration<double, std::milli>(flushing - absorbing).count();
    flush_ms = std::chrono::duration<double, std::milli>(flushed - flushing).count();
    log_mib = static_cast<double>(*retained) / static_cast<double>(kMiB);
    state.ResumeTiming();
  }
  state.counters["absorb_ms"] = absorb_ms;
  state.counters["flush_ms"] = flush_ms;
  state.counters["log_mib"] = log_mib;
  state.counters["flushed_per_s"] = flush_ms > 0 ? static_cast<double>(n) * 1e3 / flush_ms : 0;
  state.SetItemsProcessed(state.iterations() * n);
}

BENCHMARK(BM_ColdReplayCollections)
    ->ArgNames({"entries", "replaces"})
    ->Args({1'000'000, 0})
    ->Args({1'000'000, 1})
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_RecoveryColdHot)
    ->ArgNames({"entries", "keys", "value", "hot_mib", "drop_cache"})
    ->Args({1'000'000, 100'000, 1, 1024, 0})
    ->Args({1'000'000, 100'000, 1, 1024, 1})
    // The log is about four times hot: replay evicts as it goes.
    ->Args({1'000'000, 1'000'000, 1024, 256, 0})
    ->Args({1'000'000, 1'000'000, 1024, 256, 1})
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

}  // namespace
}  // namespace abyss::engine
