#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"

// The read path's cache fill with and without the doorkeeper. Cold
// holds ten times hot's budget; GETs follow Zipf 0.99, Zipf 1.2 or a
// uniform control, with stage 6's value sizes. Reports hot's hit ratio
// after a warm-up, and the mean read.

namespace abyss::engine {
namespace {

constexpr uint64_t kKeys = 200'000;
constexpr uint32_t kShards = 4;
constexpr uint64_t kWarmup = 200'000;

class NoBuffers : public consumer::CompactionBufferRouter {
 public:
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& /*op*/,
                                     std::optional<core::Duration> /*deadline*/) override {
    return std::unexpected(core::Error{core::ErrorCode::kNotFound, "none"});
  }
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view /*key*/) const override {
    return std::nullopt;
  }
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }
};

std::filesystem::path TempPath(std::string_view name) {
  return std::filesystem::temp_directory_path() /
         (std::string(name) +
          std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
}

// 30% of values 80 B, 65% 220 B, 5% 20 KiB: a median near 200 B.
size_t ValueSize(uint64_t i) {
  const uint64_t bucket = (i * 2654435761U) % 100;
  if (bucket < 30) return 80;
  if (bucket < 95) return 220;
  return size_t{20} * 1024;
}

struct ColdData {
  std::filesystem::path dir = TempPath("abyss_read_path_bench_");
  std::unique_ptr<cold::backends::RocksdbStore> cold;
  std::vector<std::string> keys;
  uint64_t bytes = 0;

  ColdData() {
    auto created =
        cold::backends::RocksdbStore::Create({.data_path = dir.string(), .shard_count = kShards});
    if (!created.has_value()) std::abort();
    cold = std::move(*created);
    keys.reserve(kKeys);
    for (uint64_t i = 0; i < kKeys; ++i) keys.push_back("user:" + std::to_string(i * 7919 % kKeys));
    constexpr uint64_t kBatch = 10'000;
    for (uint64_t start = 0; start < kKeys; start += kBatch) {
      std::vector<std::string> values;
      std::vector<core::ops::WriteOp> batch;
      values.reserve(kBatch);
      for (uint64_t i = start; i < start + kBatch; ++i) {
        values.emplace_back(ValueSize(i), 'v');
        bytes += values.back().size() + keys[i].size();
      }
      for (uint64_t i = start; i < start + kBatch; ++i) {
        batch.emplace_back(core::ops::StringSet{.key = keys[i], .value = values[i - start]});
      }
      if (!cold->ApplyBatch(batch, 0).has_value()) std::abort();
    }
  }
  ~ColdData() {
    cold.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }
  ColdData(const ColdData&) = delete;
  ColdData& operator=(const ColdData&) = delete;
  ColdData(ColdData&&) = delete;
  ColdData& operator=(ColdData&&) = delete;

  static ColdData& Get() {
    static ColdData data;
    return data;
  }
};

// Ranks drawn by Zipf(theta) over the keys, or uniformly at theta 0.
class Ranks {
 public:
  Ranks(double theta, uint64_t seed) : rng_(seed), theta_(theta) {
    if (theta == 0) return;
    cdf_.reserve(kKeys);
    double sum = 0;
    for (uint64_t i = 0; i < kKeys; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i + 1), theta);
      cdf_.push_back(sum);
    }
    for (double& c : cdf_) c /= sum;
  }
  uint64_t Next() {
    const double u = uniform_(rng_);
    if (theta_ == 0) return static_cast<uint64_t>(u * static_cast<double>(kKeys)) % kKeys;
    return static_cast<uint64_t>(std::ranges::lower_bound(cdf_, u) - cdf_.begin());
  }

 private:
  std::mt19937_64 rng_;
  std::uniform_real_distribution<double> uniform_{0.0, 1.0};
  double theta_;
  std::vector<double> cdf_;
};

double Counter(const metrics::CounterDesc<metrics::Tier>& desc, metrics::Tier tier) {
  return metrics::testing::GetCounterValue(desc, tier).value_or(0.0);
}

void BM_ReadMixFill(benchmark::State& state) {
  const bool doorkeeper = state.range(0) == 1;
  const double theta = static_cast<double>(state.range(1)) / 100.0;
  ColdData& data = ColdData::Get();
  const auto wal = TempPath("abyss_read_path_bench_wal_");
  auto queue = queue::WalQueue::Open({.wal_path = wal.string(),
                                      .segment_size_bytes = size_t{16} << 20,
                                      .max_value_size_bytes = size_t{1} << 20,
                                      .shard_count = kShards,
                                      .retention_consumers = {core::kColdConsumer}});
  if (!queue.has_value()) std::abort();
  hot::ShardedHotStore hot{hot::ShardedHotStoreConfig{
      .max_memory_bytes = static_cast<size_t>(data.bytes / 10), .shard_count = kShards}};
  NoBuffers buffers;
  Loader loader(hot, buffers, *data.cold);
  Sequencer sequencer(hot, **queue, loader, buffers, SequencerConfig{});
  ReadPath reads(hot, loader, sequencer,
                 ReadPathConfig{.cold_read_deadline = std::chrono::milliseconds{200},
                                .fill_doorkeeper = doorkeeper});
  Ranks ranks(theta, 42);
  const auto read = [&] {
    const auto& key = data.keys[ranks.Next()];
    auto value = reads.Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    if (!value.has_value() || !value->IsBulkString()) std::abort();
  };
  for (uint64_t i = 0; i < kWarmup; ++i) read();

  const double hot_before = Counter(metrics::names::kHitsTotal, metrics::Tier::kHot);
  const double cold_before = Counter(metrics::names::kHitsTotal, metrics::Tier::kCold);
  for ([[maybe_unused]] auto _ : state) read();
  const double hot_hits = Counter(metrics::names::kHitsTotal, metrics::Tier::kHot) - hot_before;
  const double cold_hits = Counter(metrics::names::kHitsTotal, metrics::Tier::kCold) - cold_before;
  state.counters["hot_hit_ratio"] = hot_hits / std::max(hot_hits + cold_hits, 1.0);
  state.counters["evictions"] = static_cast<double>(hot.Stats()->eviction_count);

  queue->reset();
  std::error_code ec;
  std::filesystem::remove_all(wal, ec);
}
BENCHMARK(BM_ReadMixFill)
    ->ArgNames({"doorkeeper", "theta_x100"})
    ->ArgsProduct({{0, 1}, {0, 99, 120}})
    ->Iterations(500'000)
    ->Unit(benchmark::kMicrosecond);

}  // namespace
}  // namespace abyss::engine
