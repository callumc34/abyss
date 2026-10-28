#include <sys/resource.h>

#include <CLI/CLI.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/config/config.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
#include "abyss/engine/io_pool.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/eviction_worker.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"
#include "abyss/platform/fs.h"
#include "abyss/platform/mapped_file.h"
#include "abyss/queue/wal_queue.h"
#include "common.h"
#include "histogram.h"
#include "metrics_scraper.h"
#include "sync_dispatch.h"
#include "temp_dir.h"
#include "workload.h"
#include "workload_shape.h"

namespace {

namespace pfs = abyss::platform::fs;
using abyss::core::Error;
using abyss::core::ErrorCode;
using abyss::core::Result;
using Clock = std::chrono::steady_clock;

constexpr std::string_view kOpWrite = "write_ack";
constexpr std::string_view kOpRead = "read";
constexpr std::string_view kOpFlush = "device_flush";
constexpr std::string_view kOpFlushConcurrent = "device_flush_concurrent";
constexpr size_t kFlushBlockBytes = 4096;
// The calibration region: one block per sample, at most 16 MiB.
constexpr uint64_t kFlushRegionBlocks = 4096;
// W1 (process_crash): write-path overhead; the ack waits for no flush.
constexpr int64_t kOverheadP99Us = 20;
// W2 (power_loss): twice the device flush p99 plus this headroom.
constexpr int64_t kDurableHeadroomUs = 50;
constexpr size_t kPrefillBatch = 1024;
// The server's kMapReserveBytesPerKey (apps/abyss-server/server.cpp).
constexpr size_t kMapReserveBytesPerKey = 256;
constexpr std::chrono::minutes kPrefillDrainTimeout{10};
// Keys whose TTL is read, at most, to measure the share carrying one.
constexpr uint64_t kTtlSamples = 20'000;
// --count-faults: a write slower than this counts as slow.
constexpr std::chrono::microseconds kSlowWrite{100};
// --hold-wal-flushes raises the durability window to its limits, and
// the run must end before unflushed data outlives the age bound.
constexpr std::chrono::milliseconds kHeldWindow{60'000};
constexpr uint64_t kHeldWindowBytes = uint64_t{4} << 30;
constexpr std::chrono::seconds kHeldRunLimit{50};
#ifdef __linux__
// Workers write inline: the writing thread's own faults.
constexpr int kFaultScope = RUSAGE_THREAD;
constexpr std::string_view kFaultScopeName = "thread";
#else
// macOS has no per-thread fault count: every thread's faults count.
constexpr int kFaultScope = RUSAGE_SELF;
constexpr std::string_view kFaultScopeName = "process";
#endif

// Recovery is not run, so the directory must hold no prior state.
Result<void> PrepareFreshDirectory(const std::filesystem::path& dir) {
  std::error_code ec;
  if (!std::filesystem::exists(dir, ec)) {
    if (ec || !std::filesystem::create_directories(dir, ec) || ec) {
      return std::unexpected(Error(ErrorCode::kInternal, "cannot create " + dir.string()));
    }
    return {};
  }
  if (!std::filesystem::is_directory(dir, ec) || !std::filesystem::is_empty(dir, ec) || ec) {
    return std::unexpected(
        Error(ErrorCode::kFailedPrecondition,
              dir.string() + " must be an empty directory: recovery is not run"));
  }
  return {};
}

struct FlushCalibration {
  abyss::perf::Histogram histogram;
  uint64_t samples = 0;
  std::chrono::nanoseconds elapsed{0};
};

// The WAL's flush: a 4 KiB write through a mapping of a region written
// before use, then a data-only sync; times the write-back and sync. A
// WAL segment is zero-filled or recycled before it goes live, so an
// extending file's metadata cost would overstate the flush.
Result<FlushCalibration> FlushFile(const std::filesystem::path& path, uint64_t samples) {
  auto file =
      pfs::Open(path, {.mode = pfs::OpenMode::kReadWrite, .create = true, .exclusive = true});
  if (!file.has_value()) return std::unexpected(file.error());
  const uint64_t blocks = std::clamp<uint64_t>(samples, 1, kFlushRegionBlocks);
  const uint64_t region = blocks * kFlushBlockBytes;

  FlushCalibration out;
  Result<void> status = pfs::ZeroFill(*file, region);
  if (status.has_value()) status = pfs::Fsync(*file, pfs::SyncMode::kDurableData);
  if (status.has_value()) {
    auto map = pfs::MappedFile::Map(*file, region);
    if (!map.has_value()) {
      status = std::unexpected(map.error());
    } else {
      const auto start = Clock::now();
      for (uint64_t i = 0; i < samples; ++i) {
        const uint64_t offset = (i % blocks) * kFlushBlockBytes;
        std::memset(map->data() + offset, 0x5a, kFlushBlockBytes);
        const auto t0 = Clock::now();
        status = map->WriteBack(offset, kFlushBlockBytes);
        if (status.has_value()) status = pfs::Fsync(*file, pfs::SyncMode::kDurableData);
        const auto t1 = Clock::now();
        if (!status.has_value()) break;
        out.histogram.Record(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        ++out.samples;
      }
      out.elapsed = Clock::now() - start;
    }
  }
  file->Close();
  const auto removed = pfs::Unlink(path);
  if (!status.has_value()) return std::unexpected(status.error());
  if (!removed.has_value()) return std::unexpected(removed.error());
  return out;
}

// `concurrency` files flushed in parallel share `samples` between them,
// separating the device floor from flush contention.
Result<FlushCalibration> FlushFilesConcurrently(const std::filesystem::path& dir, uint64_t samples,
                                                uint32_t concurrency) {
  const uint64_t per_file = std::max<uint64_t>(samples / concurrency, 1);
  std::vector<Result<FlushCalibration>> runs;
  runs.reserve(concurrency);
  for (uint32_t k = 0; k < concurrency; ++k) {
    runs.emplace_back(std::unexpected(Error(ErrorCode::kInternal, "flush thread did not run")));
  }
  std::vector<std::thread> threads;
  threads.reserve(concurrency);
  const auto start = Clock::now();
  for (uint32_t k = 0; k < concurrency; ++k) {
    threads.emplace_back([&, k] {
      runs[k] = FlushFile(dir / (".write_probe_device_flush_" + std::to_string(k)), per_file);
    });
  }
  for (auto& t : threads) t.join();

  FlushCalibration out;
  out.elapsed = Clock::now() - start;
  for (auto& run : runs) {
    if (!run.has_value()) return std::unexpected(run.error());
    out.histogram.Merge(run->histogram);
    out.samples += run->samples;
  }
  return out;
}

// The RESP pipeline's write, minus the wire codec: parsed to validate
// it, then dispatched as sent.
Result<void> Validate(const abyss::core::RespCommand& cmd) {
  if (auto parsed = abyss::core::ops::ParseWriteOp(cmd.args.front(), cmd); !parsed.has_value()) {
    return std::unexpected(parsed.error());
  }
  return {};
}

abyss::perf::WorkloadTargets DeriveTargets(abyss::core::Durability durability,
                                           const abyss::perf::Histogram& device_flush) {
  abyss::perf::TargetSpec spec;
  spec.p99_us = durability == abyss::core::Durability::kProcessCrash
                    ? kOverheadP99Us
                    : (2 * device_flush.PercentileNs(99.0) / 1000) + kDurableHeadroomUs;
  abyss::perf::WorkloadTargets targets;
  targets.per_op[std::string{kOpWrite}] = spec;
  return targets;
}

// The write-path object graph of Server::Initialize
// (apps/abyss-server/server.cpp) without RESP, TCP, admin or recovery;
// keep the two in step. Declared in construction order.
struct WritePath {
  std::unique_ptr<abyss::core::EvictionPolicy> eviction_policy;
  std::unique_ptr<abyss::hot::ShardedHotStore> hot_store;
  std::unique_ptr<abyss::queue::WalQueue> queue;
  std::unique_ptr<abyss::core::ColdStore> cold_store;
  std::unique_ptr<abyss::consumer::ColdConsumerPool> cold_pool;
  std::unique_ptr<abyss::engine::Loader> loader;
  std::unique_ptr<abyss::engine::Sequencer> sequencer;
  std::unique_ptr<abyss::engine::IoPool> io_pool;
  std::unique_ptr<abyss::engine::ReadPath> reads;
  std::unique_ptr<abyss::engine::TieringEngine> engine;
  std::unique_ptr<abyss::hot::EvictionWorker> hot_eviction_worker;
};

Result<std::unique_ptr<WritePath>> BuildWritePath(const abyss::config::Config& config) {
  namespace consumer = abyss::consumer;
  auto wp = std::make_unique<WritePath>();

  std::vector<abyss::core::EvictionRule> overrides;
  overrides.reserve(config.hot.eviction_overrides.size());
  for (const auto& o : config.hot.eviction_overrides) {
    overrides.push_back({.prefix = o.prefix, .eviction = o.eviction});
  }
  wp->eviction_policy = std::make_unique<abyss::core::EvictionPolicy>(config.hot.default_eviction,
                                                                      std::move(overrides));

  wp->hot_store = std::make_unique<abyss::hot::ShardedHotStore>(abyss::hot::ShardedHotStoreConfig{
      .max_memory_bytes = config.hot.max_memory_bytes,
      .shard_count = config.hot.shard_count,
      .reserve_bytes_per_key = kMapReserveBytesPerKey,
      .stub_memory_fraction = config.hot.stub_memory_fraction,
      .negative_max_entries = config.hot.negative_max_entries,
      .fill_max_fraction = config.hot.fill_max_fraction,
      .backpressure_ratio = config.hot.backpressure_ratio,
      // The cold pool is built later; until then nothing has drained.
      .drained = [path = wp.get()](abyss::core::ShardId shard) -> abyss::core::SequenceId {
        return path->cold_pool ? path->cold_pool->ConsumerFor(shard).LatestDrainedSeq() : 0;
      },
      .eviction_policy = wp->eviction_policy.get(),
  });
  const uint32_t shards = wp->hot_store->shard_count();

  auto queue = abyss::queue::WalQueue::Open(abyss::queue::WalConfig{
      .wal_path = config.queue.wal_path,
      .segment_size_bytes = config.queue.segment_size_bytes,
      .max_value_size_bytes = config.queue.max_value_size_bytes,
      .shard_count = shards,
      .log_count = config.queue.log_count,
      .ring_entries = config.queue.ring_entries,
      .durability = config.queue.durability,
      .durability_window_bytes = config.queue.durability_window_bytes,
      .durability_window = config.queue.durability_window,
      .min_retention = config.queue.min_retention,
      .retention_consumers = {abyss::core::kColdConsumer},
      .offset_fsync_interval = config.queue.offset_fsync_interval,
  });
  if (!queue.has_value()) return std::unexpected(queue.error());
  if ((*queue)->IsRecovering()) {
    return std::unexpected(Error(ErrorCode::kInternal, "WAL opened but still recovering"));
  }
  wp->queue = std::move(*queue);

  auto cold = abyss::cold::backends::RocksdbStore::Create(abyss::cold::backends::RocksdbConfig{
      .data_path = config.cold.data_path,
      .shard_count = shards,
      .write_buffer_size_bytes = config.cold.write_buffer_size_bytes,
      .log_clock = [path = wp.get()](abyss::core::ShardId shard) -> uint64_t {
        return path->cold_pool ? path->cold_pool->LogClockMs(shard) : 0;
      },
      .ttl_scanner = config.cold.ttl_scanner,
  });
  if (!cold.has_value()) return std::unexpected(cold.error());
  wp->cold_store = std::move(*cold);

  const auto& cc = config.cold_consumer;
  wp->cold_pool = std::make_unique<consumer::ColdConsumerPool>(
      *wp->queue, *wp->cold_store,
      consumer::ColdConsumerPool::Config{
          .shard_count = shards,
          .consumer =
              consumer::ColdConsumer::Config{
                  .quiet_threshold = cc.quiet_threshold,
                  .safety_margin = cc.safety_margin,
                  .jitter_fraction = cc.jitter_fraction,
                  .buffer_high_water_bytes = cc.buffer_high_water_bytes,
                  .buffer_low_water_bytes = cc.buffer_low_water_bytes,
                  .max_flush_batch_size = cc.max_flush_batch_size,
                  .queue_read_max_count = cc.queue_read_max_count,
                  .queue_read_timeout = cc.queue_read_timeout,
                  .retry_initial_backoff = cc.retry_initial_backoff,
                  .retry_max_backoff = cc.retry_max_backoff,
                  .checkpoint_max_flushes = cc.checkpoint_max_flushes,
                  .checkpoint_min_interval = cc.checkpoint_min_interval,
                  .loop_initial_backoff = cc.loop_initial_backoff,
                  .loop_max_backoff = cc.loop_max_backoff,
                  .drain_grace = cc.drain_grace,
              },
      },
      *wp->eviction_policy);

  wp->loader =
      std::make_unique<abyss::engine::Loader>(*wp->hot_store, *wp->cold_pool, *wp->cold_store);
  wp->sequencer = std::make_unique<abyss::engine::Sequencer>(
      *wp->hot_store, *wp->queue, *wp->loader, *wp->cold_pool,
      abyss::engine::SequencerConfig{
          .command_timeout = config.engine.command_timeout,
      });
  wp->io_pool = std::make_unique<abyss::engine::IoPool>(abyss::engine::IoPoolConfig{
      .threads = abyss::engine::ColdIoThreads(config.engine.cold_io_threads,
                                              std::thread::hardware_concurrency()),
  });
  wp->reads =
      std::make_unique<abyss::engine::ReadPath>(*wp->hot_store, *wp->loader, *wp->io_pool,
                                                abyss::engine::ReadPathConfig{
                                                    .fill_doorkeeper = config.hot.fill_doorkeeper,
                                                    .fill_max_members = config.hot.fill_max_members,
                                                });
  wp->engine =
      std::make_unique<abyss::engine::TieringEngine>(*wp->reads, *wp->sequencer, *wp->io_pool);

  wp->hot_eviction_worker = std::make_unique<abyss::hot::EvictionWorker>(
      *wp->hot_store, abyss::hot::EvictionWorker::Config{.tick = config.hot.eviction_tick});

  return wp;
}

// Server::Run's start order once recovery has completed.
void StartWritePath(WritePath& wp) {
  wp.hot_eviction_worker->Start();
  wp.cold_pool->Start();
  if (auto rc = wp.cold_store->Start(); !rc.has_value()) {
    std::cerr << "write_probe: cold store start failed: " << rc.error().message() << '\n';
  }
}

// Server::Shutdown's order.
void StopWritePath(WritePath& wp, std::chrono::seconds drain_grace) {
  wp.io_pool->Shutdown();
  wp.hot_eviction_worker->Stop();
  wp.cold_pool->Stop(std::chrono::duration_cast<std::chrono::milliseconds>(drain_grace));
  if (auto rc = wp.cold_store->Stop(); !rc.has_value()) {
    std::cerr << "write_probe: cold store stop failed: " << rc.error().message() << '\n';
  }
}

// A worker's commands, as one connection's, each run to its reply.
using Connection = abyss::testing::SyncConnection;

bool Write(Connection& connection, abyss::core::RespCommand cmd) {
  if (!Validate(cmd).has_value()) return false;
  auto reply = connection.Write(std::move(cmd));
  return reply.has_value() && !reply->IsError();
}

bool Read(Connection& connection, abyss::core::RespCommand cmd) {
  const std::string name = cmd.args.front();
  auto reply = connection.Read(name, std::move(cmd));
  return reply.has_value() && !reply->IsError();
}

// Writes `entries` keys through the engine, then waits until cold has
// drained them so the measured window has no backlog. A fixed shape
// writes one MSET of a shard's keys at a time; a trace shape creates
// each key as its type, one write each.
Result<void> Prefill(WritePath& wp, uint64_t entries, uint64_t key_count,
                     const abyss::perf::probe::WorkloadShape& shape, bool trace,
                     const std::string& value, uint64_t seed) {
  const uint32_t shards = wp.hot_store->shard_count();
  Connection connection(*wp.engine);
  std::mt19937_64 rng{seed};
  for (uint64_t i = 0; trace && i < entries; ++i) {
    for (auto& cmd : shape.Create(i % key_count, rng)) {
      if (!Write(connection, std::move(cmd))) {
        return std::unexpected(Error(ErrorCode::kInternal, "a prefill write failed"));
      }
    }
  }
  if (trace) entries = 0;
  std::vector<abyss::core::RespCommand> pending(shards);
  const auto write = [&](uint32_t shard) -> Result<void> {
    auto& mset = pending[shard];
    if (mset.args.empty()) return {};
    auto reply = connection.FanOut(abyss::core::MultiKeyKind::kMset, std::move(mset));
    mset = {};
    if (!reply.has_value()) return std::unexpected(reply.error());
    if (reply->IsError()) {
      return std::unexpected(Error(ErrorCode::kInternal, std::string(reply->ErrorMessage())));
    }
    return {};
  };
  for (uint64_t i = 0; i < entries; ++i) {
    std::string key = shape.Key(i % key_count);
    const auto shard = abyss::core::ComputeShard(key, shards);
    auto& mset = pending[shard];
    if (mset.args.empty()) mset.args.emplace_back("MSET");
    mset.args.push_back(std::move(key));
    mset.args.push_back(value);
    if ((mset.args.size() - 1) / 2 == kPrefillBatch) {
      if (auto r = write(shard); !r.has_value()) return r;
    }
  }
  for (uint32_t shard = 0; shard < shards; ++shard) {
    if (auto r = write(shard); !r.has_value()) return r;
  }

  const auto deadline = Clock::now() + kPrefillDrainTimeout;
  for (uint32_t shard = 0; shard < shards; ++shard) {
    const auto tail = wp.queue->TailSeq(shard);
    if (!tail.has_value()) return std::unexpected(tail.error());
    const auto drained = [&] {
      return wp.cold_pool->ConsumerFor(shard).LatestDrainedSeq() >= *tail;
    };
    while (!drained()) {
      if (Clock::now() > deadline) {
        return std::unexpected(Error(ErrorCode::kTimeout, "consumers did not drain the prefill"));
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
  }
  return {};
}

// How many of up to kTtlSamples keys, spread over the key space, carry
// a TTL, read through the read path as PTTL.
struct TtlShare {
  uint64_t with_ttl = 0;
  uint64_t without = 0;
  uint64_t missing = 0;
};

TtlShare SampleTtls(WritePath& wp, const abyss::perf::probe::WorkloadShape& shape,
                    uint64_t key_count) {
  TtlShare share;
  Connection connection(*wp.engine);
  const uint64_t n = std::min(key_count, kTtlSamples);
  for (uint64_t i = 0; i < n; ++i) {
    const auto reply = connection.Read(
        "PTTL", abyss::core::RespCommand{.args = {"PTTL", shape.Key(i * key_count / n)}});
    if (!reply.has_value() || !reply->IsInteger()) continue;
    if (reply->AsInteger() >= 0) {
      ++share.with_ttl;
    } else if (reply->AsInteger() == -1) {
      ++share.without;
    } else {
      ++share.missing;
    }
  }
  return share;
}

void RecordTtlShare(abyss::perf::RunReport& report, const std::string& when,
                    const TtlShare& share) {
  const uint64_t present = share.with_ttl + share.without;
  report.measured["ttl_share_" + when] =
      present > 0 ? static_cast<double>(share.with_ttl) / static_cast<double>(present) : 0.0;
  report.measured["ttl_keys_sampled_" + when] = static_cast<double>(present + share.missing);
  report.measured["ttl_keys_missing_" + when] = static_cast<double>(share.missing);
}

// Page faults around each measured write, from getrusage.
struct FaultTally {
  uint64_t writes = 0;
  uint64_t faulted = 0;
  uint64_t minor = 0;
  uint64_t major = 0;
  uint64_t slow = 0;
  uint64_t slow_faulted = 0;
  uint64_t slow_minor = 0;

  void Add(const FaultTally& other) {
    writes += other.writes;
    faulted += other.faulted;
    minor += other.minor;
    major += other.major;
    slow += other.slow;
    slow_faulted += other.slow_faulted;
    slow_minor += other.slow_minor;
  }
};

bool CountedWrite(Connection& connection, abyss::core::RespCommand cmd, FaultTally& tally) {
  rusage before{};
  getrusage(kFaultScope, &before);
  const auto start = Clock::now();
  const bool ok = Write(connection, std::move(cmd));
  const auto elapsed = Clock::now() - start;
  rusage after{};
  getrusage(kFaultScope, &after);
  const auto minor = static_cast<uint64_t>(after.ru_minflt - before.ru_minflt);
  const auto major = static_cast<uint64_t>(after.ru_majflt - before.ru_majflt);
  const bool slow = elapsed > kSlowWrite;
  ++tally.writes;
  tally.faulted += minor + major > 0 ? 1 : 0;
  tally.minor += minor;
  tally.major += major;
  tally.slow += slow ? 1 : 0;
  tally.slow_faulted += slow && minor + major > 0 ? 1 : 0;
  tally.slow_minor += slow ? minor : 0;
  return ok;
}

// Holds every WAL flush until opened: --hold-wal-flushes.
struct FlushGate {
  std::mutex mu;
  std::condition_variable cv;
  bool open = false;

  void Open() {
    {
      const std::scoped_lock lock(mu);
      open = true;
    }
    cv.notify_all();
  }
};

// The server's io_threads default; workers emulate its reactors, each
// running each write to its reply.
int ReactorCount() {
  const auto hw = std::thread::hardware_concurrency();
  return static_cast<int>(std::clamp<uint32_t>(hw == 0 ? 1U : hw, 1U, 16U));
}

abyss::perf::MetricSnapshot SnapshotOf(std::string phase, const std::string& scrape) {
  return {
      .phase = std::move(phase),
      .metrics = abyss::perf::MetricsScraper::ParseMetrics(scrape),
  };
}

}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
  CLI::App app{"In-process write path probe (ADP-015)"};
  abyss::perf::probe::ProbeArgs args;
  args.workers = ReactorCount();
  abyss::perf::probe::RegisterCliOptions(app, args);

  auto config = abyss::config::Config::Defaults();
  std::string wal_path;
  std::string cold_path;
  std::string durability_name{abyss::core::DurabilityName(config.queue.durability)};
  uint64_t flush_samples = 1000;
  uint32_t flush_concurrency = 0;
  uint64_t prefill_entries = 0;
  std::string metrics_out;
  std::string shape_name = "fixed";
  double large_fraction = 0.0;
  double collection_fraction = 0.2;
  bool count_faults = false;
  bool hold_wal_flushes = false;
  auto quiet_threshold_s = static_cast<uint64_t>(config.cold_consumer.quiet_threshold.count());
  app.add_option("--wal-path", wal_path, "WAL directory; must be empty or absent (default: temp)");
  app.add_option("--cold-path", cold_path,
                 "Cold store directory; must be empty or absent (default: temp)");
  app.add_option("--durability", durability_name,
                 "process_crash (evaluates W1) | power_loss (evaluates W2)");
  app.add_option("--shards", config.hot.shard_count, "Shard count");
  app.add_option("--hot-max-memory-bytes", config.hot.max_memory_bytes, "Hot store memory budget");
  app.add_option("--fill-doorkeeper", config.hot.fill_doorkeeper,
                 "Fill hot from a read only on a key's second miss");
  app.add_option("--segment-size-bytes", config.queue.segment_size_bytes, "WAL segment size");
  app.add_option("--log-count", config.queue.log_count, "Physical WAL logs, a power of two");
  app.add_option("--quiet-threshold-s", quiet_threshold_s, "Cold consumer quiet threshold");
  app.add_option("--prefill-entries", prefill_entries,
                 "WAL entries appended and drained before measuring");
  app.add_option("--shape", shape_name,
                 "fixed (one value size, no TTL, strings) | trace (cache-trace keys, values, "
                 "TTLs and types)")
      ->check(CLI::IsMember({"fixed", "trace"}));
  app.add_option("--large-fraction", large_fraction, "fixed shape: share of values of 16-64 KiB")
      ->check(CLI::Range(0.0, 1.0));
  app.add_option("--collection-fraction", collection_fraction,
                 "trace shape: share of keys that are sets and sorted sets, half each")
      ->check(CLI::Range(0.0, 1.0));
  app.add_flag("--count-faults", count_faults,
               "Diagnostic: count page faults around each measured write, the writing "
               "thread's on Linux and the whole process's on macOS (getrusage)");
  app.add_flag("--hold-wal-flushes", hold_wal_flushes,
               "Diagnostic, process_crash only: the WAL flushes nothing during the run, "
               "so cold, which persists only power-durable entries, persists nothing");
  app.add_option("--metrics-out", metrics_out,
                 "Also write the raw registry scrapes to <path>.start.prom and <path>.end.prom");
  app.add_option("--flush-samples", flush_samples, "Device flush calibration samples")
      ->check(CLI::PositiveNumber);
  app.add_option("--flush-concurrency", flush_concurrency,
                 "Files flushed in parallel for device_flush_concurrent (default: --shards)");
  CLI11_PARSE(app, argc, argv);

  const auto durability = abyss::core::ParseDurability(durability_name);
  if (!durability.has_value()) {
    std::cerr << "invalid --durability: " << durability_name
              << " (expected process_crash or power_loss)\n";
    return 2;
  }
  // W1 and W2 are defined at a fixed fraction of saturation.
  const bool open_loop = args.target_rate_ops > 0;
  if (args.gate && !open_loop) {
    std::cerr
        << "write_probe: --gate needs --target-rate-ops; closed-loop runs are not evaluated\n";
    return 2;
  }
  if (flush_concurrency == 0) flush_concurrency = config.hot.shard_count;

  abyss::perf::OperationMix mix;
  mix.weights[std::string{kOpWrite}] = 1.0;
  if (!args.mix_spec.empty()) {
    auto parsed = abyss::perf::probe::ParseMixSpec(args.mix_spec);
    if (!parsed.has_value() || !parsed->weights.contains(std::string{kOpWrite}) ||
        std::ranges::any_of(parsed->weights, [](const auto& weight) {
          return weight.first != kOpWrite && weight.first != kOpRead;
        })) {
      std::cerr << "invalid mix: the write probe runs " << kOpWrite << ", and optionally "
                << kOpRead << '\n';
      return 2;
    }
    mix = *std::move(parsed);
  }
  if (hold_wal_flushes) {
    if (*durability != abyss::core::Durability::kProcessCrash) {
      std::cerr << "write_probe: --hold-wal-flushes needs --durability process_crash\n";
      return 2;
    }
    if (args.warmup + args.duration > kHeldRunLimit) {
      std::cerr << "write_probe: --hold-wal-flushes needs --warmup plus --duration of at most "
                << kHeldRunLimit.count() << " s\n";
      return 2;
    }
    config.queue.durability_window = kHeldWindow;
    config.queue.durability_window_bytes = kHeldWindowBytes;
  }
  const bool trace = shape_name == "trace";

  std::unique_ptr<abyss::testing::TempDir> tmp_owner;
  if (wal_path.empty() || cold_path.empty()) {
    tmp_owner = std::make_unique<abyss::testing::TempDir>("write_probe");
    if (wal_path.empty()) wal_path = tmp_owner->Sub("wal").string();
    if (cold_path.empty()) cold_path = tmp_owner->Sub("cold").string();
  }
  for (const auto& dir : {wal_path, cold_path}) {
    if (auto fresh = PrepareFreshDirectory(dir); !fresh.has_value()) {
      std::cerr << "write_probe: " << fresh.error().message() << '\n';
      return 2;
    }
  }

  config.queue.wal_path = wal_path;
  config.cold.data_path = cold_path;
  config.queue.durability = *durability;
  config.cold_consumer.quiet_threshold = std::chrono::seconds{quiet_threshold_s};
  if (auto valid = config.Validate(); !valid.has_value()) {
    std::cerr << "invalid configuration: " << valid.error().message() << '\n';
    return 2;
  }
  abyss::metrics::Registry::Instance().SetEnabled(config.metrics.enabled);

  auto calibration =
      FlushFile(std::filesystem::path{wal_path} / ".write_probe_device_flush", flush_samples);
  if (!calibration.has_value()) {
    std::cerr << "write_probe: device flush calibration failed: " << calibration.error().message()
              << '\n';
    return 1;
  }
  auto concurrent = FlushFilesConcurrently(wal_path, flush_samples, flush_concurrency);
  if (!concurrent.has_value()) {
    std::cerr << "write_probe: concurrent flush calibration failed: "
              << concurrent.error().message() << '\n';
    return 1;
  }
  const auto targets = DeriveTargets(*durability, calibration->histogram);

  auto write_path = BuildWritePath(config);
  if (!write_path.has_value()) {
    std::cerr << "write_probe: write path setup failed: " << write_path.error().message() << '\n';
    return 1;
  }
  auto& wp = **write_path;
  StartWritePath(wp);

  const abyss::perf::probe::WorkloadShape shape{{.trace = trace,
                                                 .value_bytes = args.value_size_bytes,
                                                 .large_fraction = large_fraction,
                                                 .collection_fraction = collection_fraction},
                                                args.key_count};
  const std::string prefill_value(args.value_size_bytes, 'x');
  if (auto prefilled =
          Prefill(wp, prefill_entries, args.key_count, shape, trace, prefill_value, args.seed);
      !prefilled.has_value()) {
    std::cerr << "write_probe: prefill failed: " << prefilled.error().message() << '\n';
    StopWritePath(wp, config.cold_consumer.drain_grace);
    return 1;
  }

  std::vector<std::mt19937_64> rngs;
  rngs.reserve(static_cast<size_t>(args.workers));
  for (int w = 0; w < args.workers; ++w) {
    rngs.emplace_back(args.seed + (static_cast<uint64_t>(w) * 7919));
  }
  const TtlShare ttl_after_prefill = trace ? SampleTtls(wp, shape, args.key_count) : TtlShare{};
  std::vector<FaultTally> faults(static_cast<size_t>(args.workers));
  std::vector<std::unique_ptr<Connection>> connections;
  connections.reserve(static_cast<size_t>(args.workers));
  for (int w = 0; w < args.workers; ++w) {
    connections.push_back(std::make_unique<Connection>(*wp.engine));
  }
  std::atomic<bool> measuring{false};
  abyss::perf::OpFn op_fn = [&](int worker_id, std::string_view op_name, uint64_t key_index) {
    auto& rng = rngs[static_cast<size_t>(worker_id)];
    Connection& connection = *connections[static_cast<size_t>(worker_id)];
    if (op_name == kOpRead) return Read(connection, shape.Read(key_index, rng));
    if (count_faults && measuring.load(std::memory_order_relaxed)) {
      return CountedWrite(connection, shape.Write(key_index, rng),
                          faults[static_cast<size_t>(worker_id)]);
    }
    return Write(connection, shape.Write(key_index, rng));
  };

  const auto gate = std::make_shared<FlushGate>();
  if (hold_wal_flushes) {
    wp.queue->SetFlushHookForTesting([gate](uint32_t /*log*/) -> Result<void> {
      std::unique_lock lock(gate->mu);
      gate->cv.wait(lock, [&gate] { return gate->open; });
      return {};
    });
  }
  // Scraped as the warmup ends, so server histograms cover what the
  // client's latencies do.
  std::string scrape_start;
  abyss::consumer::ColdConsumerPool::AggregateMetrics cold_start;
  auto run_config = abyss::perf::probe::MakeRunLoopConfig(args, mix);
  run_config.at_measure_start = [&] {
    scrape_start = abyss::metrics::Registry::Instance().Scrape();
    cold_start = wp.cold_pool->Snapshot();
    measuring.store(true, std::memory_order_relaxed);
  };
  auto result = abyss::perf::RunLoop(run_config, op_fn);
  const std::string scrape_end = abyss::metrics::Registry::Instance().Scrape();
  const auto cold_end = wp.cold_pool->Snapshot();
  gate->Open();
  const TtlShare ttl_at_end = trace ? SampleTtls(wp, shape, args.key_count) : TtlShare{};

  const auto workload = abyss::perf::probe::MakeWorkloadConfig(args, mix, targets);
  auto report = abyss::perf::probe::BuildReport(args, workload, result);
  if (!open_loop) {
    report.targets_not_evaluated = "targets not evaluated (closed-loop)";
    abyss::perf::EvaluateTargets(report);
  }
  report.config = {
      {"durability", std::string{abyss::core::DurabilityName(config.queue.durability)}},
      {"shard_count", std::to_string(config.hot.shard_count)},
      {"hot_max_memory_bytes", std::to_string(config.hot.max_memory_bytes)},
      {"fill_doorkeeper", config.hot.fill_doorkeeper ? "true" : "false"},
      {"segment_size_bytes", std::to_string(config.queue.segment_size_bytes)},
      {"log_count", std::to_string(config.queue.log_count)},
      {"cold_quiet_threshold_s", std::to_string(quiet_threshold_s)},
      {"value_size_bytes", std::to_string(args.value_size_bytes)},
      {"shape", shape_name},
      {"large_fraction", std::to_string(large_fraction)},
      {"collection_fraction", std::to_string(collection_fraction)},
      {"hold_wal_flushes", hold_wal_flushes ? "true" : "false"},
      {"durability_window_ms", std::to_string(config.queue.durability_window.count())},
      {"prefill_entries", std::to_string(prefill_entries)},
      {"workers", std::to_string(args.workers)},
      {"workers_model", "reactor"},
      {"flush_samples", std::to_string(flush_samples)},
      {"flush_concurrency", std::to_string(flush_concurrency)},
  };
  report.operations[std::string{kOpFlush}] = abyss::perf::StatsFromHistogram(
      calibration->histogram, calibration->samples, calibration->elapsed);
  report.operations[std::string{kOpFlushConcurrent}] = abyss::perf::StatsFromHistogram(
      concurrent->histogram, concurrent->samples, concurrent->elapsed);
  const double measured_s = std::chrono::duration<double>(result.measured_duration).count();
  if (measured_s > 0) {
    const auto flushes = [](const abyss::consumer::ColdConsumerPool::AggregateMetrics& m) {
      return m.flushes_quiet + m.flushes_deadline + m.flushes_aggressive;
    };
    report.measured["cold_ops_flushed_per_s"] =
        static_cast<double>(cold_end.ops_flushed - cold_start.ops_flushed) / measured_s;
    report.measured["cold_flushes_per_s"] =
        static_cast<double>(flushes(cold_end) - flushes(cold_start)) / measured_s;
  }
  if (trace) {
    RecordTtlShare(report, "after_prefill", ttl_after_prefill);
    RecordTtlShare(report, "at_end", ttl_at_end);
  }
  if (count_faults) {
    FaultTally total;
    for (const FaultTally& tally : faults) total.Add(tally);
    report.config["fault_scope"] = std::string{kFaultScopeName};
    report.measured["fault_writes"] = static_cast<double>(total.writes);
    report.measured["fault_writes_faulted"] = static_cast<double>(total.faulted);
    report.measured["fault_minor"] = static_cast<double>(total.minor);
    report.measured["fault_major"] = static_cast<double>(total.major);
    report.measured["fault_slow_writes"] = static_cast<double>(total.slow);
    report.measured["fault_slow_writes_faulted"] = static_cast<double>(total.slow_faulted);
    report.measured["fault_slow_minor"] = static_cast<double>(total.slow_minor);
  }
  report.server_metrics = {SnapshotOf("start", scrape_start), SnapshotOf("end", scrape_end)};
  report.server_histograms = abyss::perf::SummariseHistograms(scrape_start, scrape_end);
  if (!metrics_out.empty()) {
    std::ofstream(metrics_out + ".start.prom") << scrape_start;
    std::ofstream(metrics_out + ".end.prom") << scrape_end;
  }
  result.per_op_histograms.insert_or_assign(std::string{kOpFlush},
                                            std::move(calibration->histogram));
  result.per_op_histograms.insert_or_assign(std::string{kOpFlushConcurrent},
                                            std::move(concurrent->histogram));

  const bool written = abyss::perf::probe::WriteOutputs(report, args, result);
  StopWritePath(wp, config.cold_consumer.drain_grace);
  if (!written) return 1;

  if (const auto errors = abyss::perf::TotalErrors(result); errors > 0) {
    std::cerr << "write_probe: " << errors << " op errors during run\n";
    return abyss::perf::probe::kExitOpErrors;
  }
  if (args.gate && !report.pass) {
    std::cerr << "write_probe: one or more targets failed\n";
    return 3;
  }
  return 0;
}
