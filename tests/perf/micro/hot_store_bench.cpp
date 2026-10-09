#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"

// Single-threaded lower-bound measurement of hot store Get / Apply. Provides
// a fixed reference point for regression tracking; the realistic-load
// measurement of these targets lives in tests/perf/probe/hot_probe.cpp.

namespace abyss::hot {
namespace {

constexpr uint32_t kShardCount = 4;
constexpr size_t kMaxMemoryBytes = 256ULL * 1024ULL * 1024ULL;

std::string KeyFor(int64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020lld", static_cast<long long>(idx));
  return buf.data();
}

struct HotFixture {
  core::EvictionPolicy eviction{core::EvictionTTL{86400}};
  ShardedHotStore store;
  std::string value;

  explicit HotFixture(int64_t key_count, size_t value_size)
      : store(ShardedHotStoreConfig{
            .max_memory_bytes = kMaxMemoryBytes,
            .shard_count = kShardCount,
            .eviction_policy = &eviction,
        }),
        value(value_size, 'x') {
    for (int64_t i = 0; i < key_count; ++i) {
      const auto key = KeyFor(i);
      const core::ops::StringSet set_op{.key = key, .value = value, .abs_ttl_ms = 0};
      const core::ops::WriteOp op = set_op;
      auto rc = store.Apply(op, /*seq=*/core::kFirstSeq);
      if (!rc.has_value()) std::abort();
    }
  }
};

void BM_HotStringGet(benchmark::State& state) {
  const auto key_count = state.range(0);
  const auto value_size = static_cast<size_t>(state.range(1));
  HotFixture fixture{key_count, value_size};
  int64_t i = 0;
  for ([[maybe_unused]] auto _ : state) {
    const auto key = KeyFor(i % key_count);
    const core::ops::StringGet op{.key = key};
    benchmark::DoNotOptimize(fixture.store.Exec(op));
    ++i;
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_HotStringGet)->Args({100000, 64})->Args({1000000, 64})->Unit(benchmark::kNanosecond);

void BM_HotStringSet(benchmark::State& state) {
  const auto key_count = state.range(0);
  const auto value_size = static_cast<size_t>(state.range(1));
  HotFixture fixture{key_count, value_size};
  int64_t i = 0;
  for ([[maybe_unused]] auto _ : state) {
    const auto key = KeyFor(i % key_count);
    const core::ops::StringSet set_op{.key = key, .value = fixture.value, .abs_ttl_ms = 0};
    const core::ops::WriteOp op = set_op;
    benchmark::DoNotOptimize(fixture.store.Apply(op, /*seq=*/core::kFirstSeq));
    ++i;
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_HotStringSet)->Args({100000, 64})->Args({100000, 1024})->Unit(benchmark::kNanosecond);

// Read hits from 1 and 8 threads over one store, keys formatted up
// front: whether readers of one shard, or of one key, serialise.
struct ReadScaling {
  core::EvictionPolicy eviction{core::EvictionTTL{86400}};
  ShardedHotStore store;
  std::vector<std::string> keys;

  explicit ReadScaling(uint32_t shards)
      : store(ShardedHotStoreConfig{
            .max_memory_bytes = kMaxMemoryBytes,
            .shard_count = shards,
            .eviction_policy = &eviction,
        }) {
    constexpr int64_t kKeys = 100000;
    keys.reserve(kKeys);
    for (int64_t i = 0; i < kKeys; ++i) {
      keys.push_back(KeyFor(i));
      const core::ops::StringSet set_op{.key = keys.back(), .value = "v"};
      if (!store.Apply(core::ops::WriteOp{set_op}, core::kFirstSeq).has_value()) std::abort();
    }
  }

  static ReadScaling& Get(uint32_t shards) {
    static std::mutex mu;
    static std::map<uint32_t, std::unique_ptr<ReadScaling>> fixtures;
    const std::scoped_lock lock(mu);
    auto& fixture = fixtures[shards];
    if (fixture == nullptr) fixture = std::make_unique<ReadScaling>(shards);
    return *fixture;
  }
};

void BM_HotReadScaling(benchmark::State& state) {
  ReadScaling& fixture = ReadScaling::Get(static_cast<uint32_t>(state.range(0)));
  const bool one_key = state.range(1) != 0;
  uint64_t x = static_cast<uint64_t>(state.thread_index()) + 1;
  for ([[maybe_unused]] auto _ : state) {
    x = (x * 6364136223846793005ULL) + 1442695040888963407ULL;
    const std::string& key = fixture.keys[one_key ? 0 : (x >> 33) % fixture.keys.size()];
    benchmark::DoNotOptimize(
        fixture.store.Read(core::ops::ReadOp{core::ops::StringGet{.key = key}}));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_HotReadScaling)
    ->ArgNames({"shards", "one_key"})
    ->ArgsProduct({{1, 64}, {0, 1}})
    ->Threads(1)
    ->Threads(8)
    ->UseRealTime();

}  // namespace
}  // namespace abyss::hot
