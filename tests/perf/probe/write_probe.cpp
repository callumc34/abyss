#include <CLI/CLI.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
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
#include "temp_dir.h"
#include "workload.h"

namespace {

namespace pfs = abyss::platform::fs;
using abyss::core::Error;
using abyss::core::ErrorCode;
using abyss::core::Result;
using Clock = std::chrono::steady_clock;

constexpr std::string_view kOpWrite = "write_ack";
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
constexpr std::chrono::minutes kPrefillDrainTimeout{10};

std::string KeyFor(uint64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020llu", static_cast<unsigned long long>(idx));
  return buf.data();
}

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

// The RESP pipeline's SET, minus the wire codec: parsed to validate it,
// then dispatched as sent.
Result<abyss::core::RespCommand> CanonicalSet(std::string key, const std::string& value) {
  abyss::core::RespCommand cmd{.args = {"SET", std::move(key), value}};
  if (auto parsed = abyss::core::ops::ParseWriteOp("SET", cmd); !parsed.has_value()) {
    return std::unexpected(parsed.error());
  }
  return cmd;
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
      .admission_timeout = config.engine.write_timeout,
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
          .write_timeout = config.engine.write_timeout,
      });
  wp->reads =
      std::make_unique<abyss::engine::ReadPath>(*wp->hot_store, *wp->loader, *wp->sequencer,
                                                abyss::engine::ReadPathConfig{
                                                    .write_timeout = config.engine.write_timeout,
                                                    .fill_doorkeeper = config.hot.fill_doorkeeper,
                                                    .fill_max_members = config.hot.fill_max_members,
                                                });
  wp->engine = std::make_unique<abyss::engine::TieringEngine>(*wp->reads, *wp->sequencer);

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
  wp.hot_eviction_worker->Stop();
  wp.cold_pool->Stop(std::chrono::duration_cast<std::chrono::milliseconds>(drain_grace));
  if (auto rc = wp.cold_store->Stop(); !rc.has_value()) {
    std::cerr << "write_probe: cold store stop failed: " << rc.error().message() << '\n';
  }
}

// Writes `entries` SETs through the engine, one MSET of a shard's keys
// at a time, then waits until cold has drained them so the measured
// window has no backlog.
Result<void> Prefill(WritePath& wp, uint64_t entries, uint64_t key_count,
                     const std::string& value) {
  const uint32_t shards = wp.hot_store->shard_count();
  std::vector<abyss::core::RespCommand> pending(shards);
  const auto write = [&](uint32_t shard) -> Result<void> {
    auto& mset = pending[shard];
    if (mset.args.empty()) return {};
    auto reply = wp.engine->DispatchFanOut(abyss::core::MultiKeyKind::kMset, std::move(mset));
    mset = {};
    if (!reply.has_value()) return std::unexpected(reply.error());
    if (reply->IsError()) {
      return std::unexpected(Error(ErrorCode::kInternal, std::string(reply->ErrorMessage())));
    }
    return {};
  };
  for (uint64_t i = 0; i < entries; ++i) {
    std::string key = KeyFor(i % key_count);
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

// The server's io_threads default; workers emulate its reactors, each
// blocking in DispatchWrite.
int ReactorCount() {
  const auto hw = std::thread::hardware_concurrency();
  return static_cast<int>(std::clamp<uint32_t>(hw == 0 ? 1U : hw, 1U, 16U));
}

abyss::perf::MetricSnapshot SnapshotRegistry(std::string phase) {
  return {
      .phase = std::move(phase),
      .metrics =
          abyss::perf::MetricsScraper::ParseMetrics(abyss::metrics::Registry::Instance().Scrape()),
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
  auto quiet_threshold_s = static_cast<uint64_t>(config.cold_consumer.quiet_threshold.count());
  app.add_option("--wal-path", wal_path, "WAL directory; must be empty or absent (default: temp)");
  app.add_option("--cold-path", cold_path,
                 "Cold store directory; must be empty or absent (default: temp)");
  app.add_option("--durability", durability_name,
                 "process_crash (evaluates W1) | power_loss (evaluates W2)");
  app.add_option("--shards", config.hot.shard_count, "Shard count");
  app.add_option("--segment-size-bytes", config.queue.segment_size_bytes, "WAL segment size");
  app.add_option("--log-count", config.queue.log_count, "Physical WAL logs, a power of two");
  app.add_option("--quiet-threshold-s", quiet_threshold_s, "Cold consumer quiet threshold");
  app.add_option("--prefill-entries", prefill_entries,
                 "WAL entries appended and drained before measuring");
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
    if (!parsed.has_value() || parsed->weights.size() != 1 ||
        !parsed->weights.contains(std::string{kOpWrite})) {
      std::cerr << "invalid mix: the write probe runs " << kOpWrite << " only\n";
      return 2;
    }
  }

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

  const std::string prefill_value(args.value_size_bytes, 'x');
  if (auto prefilled = Prefill(wp, prefill_entries, args.key_count, prefill_value);
      !prefilled.has_value()) {
    std::cerr << "write_probe: prefill failed: " << prefilled.error().message() << '\n';
    StopWritePath(wp, config.cold_consumer.drain_grace);
    return 1;
  }

  std::vector<std::string> values(static_cast<size_t>(args.workers),
                                  std::string(args.value_size_bytes, 'x'));
  abyss::perf::OpFn op_fn = [&](int worker_id, std::string_view /*op_name*/, uint64_t key_index) {
    auto cmd = CanonicalSet(KeyFor(key_index), values[static_cast<size_t>(worker_id)]);
    if (!cmd.has_value()) return false;
    auto reply =
        wp.engine->DispatchWrite("SET", std::move(*cmd), abyss::core::PredicateFlags::kNone);
    return reply.has_value() && !reply->IsError();
  };

  auto server_start = SnapshotRegistry("start");
  auto result = abyss::perf::RunLoop(abyss::perf::probe::MakeRunLoopConfig(args, mix), op_fn);
  auto server_end = SnapshotRegistry("end");

  const auto workload = abyss::perf::probe::MakeWorkloadConfig(args, mix, targets);
  auto report = abyss::perf::probe::BuildReport(args, workload, result);
  if (!open_loop) {
    report.targets_not_evaluated = "targets not evaluated (closed-loop)";
    abyss::perf::EvaluateTargets(report);
  }
  report.config = {
      {"durability", std::string{abyss::core::DurabilityName(config.queue.durability)}},
      {"shard_count", std::to_string(config.hot.shard_count)},
      {"segment_size_bytes", std::to_string(config.queue.segment_size_bytes)},
      {"log_count", std::to_string(config.queue.log_count)},
      {"cold_quiet_threshold_s", std::to_string(quiet_threshold_s)},
      {"value_size_bytes", std::to_string(args.value_size_bytes)},
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
  report.server_metrics = {std::move(server_start), std::move(server_end)};
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
