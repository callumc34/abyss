#include <benchmark/benchmark.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/engine/decide.h"
#include "abyss/engine/loader.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"

// A full load of a 1M-member set, hash and zset from RocksDB:
// Loader::Load off the lock, then CompleteLoad, the install's
// exclusive-lock hold, timed alone. Then a fill into a full store: the
// hold while CompleteLoad evicts to make room, and the free after it.

namespace abyss::engine {
namespace {

constexpr int kMembers = 1'000'000;
constexpr int kBatch = 10'000;

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

struct Fixture {
  std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("abyss_loader_bench_" +
       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  std::unique_ptr<cold::backends::RocksdbStore> cold;
  NoBuffers buffers;
  hot::ShardedHotStore hot{hot::ShardedHotStoreConfig{.shard_count = 1}};
  std::unique_ptr<Loader> loader;

  Fixture() {
    std::filesystem::remove_all(dir);
    auto created = cold::backends::RocksdbStore::Create({.data_path = dir.string()});
    if (!created.has_value()) std::abort();
    cold = std::move(*created);
    loader = std::make_unique<Loader>(hot, buffers, *cold);
    for (int start = 0; start < kMembers; start += kBatch) {
      std::vector<std::string> names;
      names.reserve(kBatch);
      for (int i = start; i < start + kBatch; ++i) names.push_back("member:" + std::to_string(i));
      core::ops::SetAdd set{.key = "set"};
      core::ops::HashSet hash{.key = "hash"};
      core::ops::ZsetAdd zset{.key = "zset"};
      for (size_t i = 0; i < names.size(); ++i) {
        set.members.emplace_back(names[i]);
        hash.fields.push_back({.field = names[i], .value = "value"});
        zset.entries.push_back(
            {.score = static_cast<double>(start) + static_cast<double>(i), .member = names[i]});
      }
      const std::vector<core::ops::WriteOp> batch{set, hash, zset};
      if (!cold->ApplyBatch(batch, 0).has_value()) std::abort();
    }
  }
  ~Fixture() {
    cold.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }
  Fixture(const Fixture&) = delete;
  Fixture& operator=(const Fixture&) = delete;
  Fixture(Fixture&&) = delete;
  Fixture& operator=(Fixture&&) = delete;

  static Fixture& Get() {
    static Fixture fixture;
    return fixture;
  }
};

double Ms(std::chrono::steady_clock::duration d) {
  return std::chrono::duration<double, std::milli>(d).count();
}

std::string KeyOf(int64_t type) {
  switch (type) {
    case 0:
      return "set";
    case 1:
      return "hash";
    default:
      return "zset";
  }
}

void BM_FullLoadAndInstall(benchmark::State& state) {
  const std::string key = KeyOf(state.range(0));
  Fixture& fixture = Fixture::Get();
  double load_ms = 0;
  double hold_ms = 0;
  for ([[maybe_unused]] auto _ : state) {
    const auto start = std::chrono::steady_clock::now();
    auto loaded = fixture.loader->Load(0, key, Need::kState,
                                       std::chrono::steady_clock::now() + std::chrono::minutes(5));
    const auto loaded_at = std::chrono::steady_clock::now();
    if (!loaded.has_value()) std::abort();
    hot::SingleShardStore shard{hot::SingleShardConfig{}};
    const auto token = shard.BeginLoad(key);
    if (!token.has_value()) std::abort();
    const auto hold_start = std::chrono::steady_clock::now();
    const bool installed = shard.CompleteLoad(key, *token, *std::move(loaded),
                                              core::EvictionTTL{3600}, hot::kAllDrained);
    const auto hold_end = std::chrono::steady_clock::now();
    if (!installed) std::abort();
    load_ms += Ms(loaded_at - start);
    hold_ms += Ms(hold_end - hold_start);
    state.SetIterationTime(
        std::chrono::duration<double>(hold_end - hold_start + loaded_at - start).count());
  }
  const auto n = static_cast<double>(state.iterations());
  state.counters["load_ms"] = load_ms / n;
  state.counters["install_hold_ms"] = hold_ms / n;
}
BENCHMARK(BM_FullLoadAndInstall)
    ->ArgName("set0_hash1_zset2")
    ->Arg(0)
    ->Arg(1)
    ->Arg(2)
    ->Iterations(3)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);

// SCARD, HLEN and ZCARD of a 1M-member cold key: cold's meta, no member.
void BM_ColdCardinality(benchmark::State& state) {
  const std::string key = KeyOf(state.range(0));
  core::KeyType type = core::KeyType::kZset;
  if (state.range(0) == 0) type = core::KeyType::kSet;
  if (state.range(0) == 1) type = core::KeyType::kHash;
  Fixture& fixture = Fixture::Get();
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
  rocksdb::get_perf_context()->Reset();
  for ([[maybe_unused]] auto _ : state) {
    auto count = fixture.loader->Cardinality(
        0, key, type, std::chrono::steady_clock::now() + std::chrono::milliseconds(5));
    if (!count.has_value() || count->members != kMembers) std::abort();
  }
  state.counters["member_reads"] =
      static_cast<double>(rocksdb::get_perf_context()->iter_next_count);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
}
BENCHMARK(BM_ColdCardinality)
    ->ArgName("set0_hash1_zset2")
    ->Arg(0)
    ->Arg(1)
    ->Arg(2)
    ->Unit(benchmark::kMicrosecond);

// A store at its budget of small drained strings takes a 1M-member
// fill, evicting to make room under the lock. What it evicts goes to
// a graveyard, freed after the hold, as ShardedHotStore::CompleteLoad
// does.
void BM_FillHoldUnderMemoryPressure(benchmark::State& state) {
  constexpr size_t kBudget = size_t{128} << 20;
  const std::string key = KeyOf(state.range(0));
  Fixture& fixture = Fixture::Get();
  double hold_ms = 0;
  double free_ms = 0;
  double evicted = 0;
  for ([[maybe_unused]] auto _ : state) {
    auto loaded = fixture.loader->Load(0, key, Need::kState,
                                       std::chrono::steady_clock::now() + std::chrono::minutes(5));
    if (!loaded.has_value()) std::abort();
    hot::SingleShardStore shard{hot::SingleShardConfig{.max_memory_bytes = kBudget}};
    const std::string value(64, 'v');
    for (uint64_t i = 0; shard.Stats().used_bytes < kBudget - (kBudget / 50); ++i) {
      auto applied = shard.Apply(core::ops::WriteOp{core::ops::StringSet{
                                     .key = "small:" + std::to_string(i), .value = value}},
                                 core::EvictionTTL{3600}, i + 1, hot::kAllDrained);
      if (!applied.has_value()) std::abort();
    }
    const auto before = shard.Stats().eviction_count;
    const auto token = shard.BeginLoad(key);
    if (!token.has_value()) std::abort();
    hot::Graveyard graveyard;
    const auto hold_start = std::chrono::steady_clock::now();
    shard.SetGraveyard(&graveyard);
    const bool installed = shard.CompleteLoad(key, *token, *std::move(loaded),
                                              core::EvictionTTL{3600}, hot::kAllDrained);
    shard.SetGraveyard(nullptr);
    const auto hold_end = std::chrono::steady_clock::now();
    graveyard = {};
    const auto freed = std::chrono::steady_clock::now();
    if (!installed) std::abort();
    hold_ms += Ms(hold_end - hold_start);
    free_ms += Ms(freed - hold_end);
    evicted += static_cast<double>(shard.Stats().eviction_count - before);
    state.SetIterationTime(std::chrono::duration<double>(hold_end - hold_start).count());
  }
  const auto n = static_cast<double>(state.iterations());
  state.counters["install_hold_ms"] = hold_ms / n;
  state.counters["graveyard_free_ms"] = free_ms / n;
  state.counters["evicted"] = evicted / n;
}
BENCHMARK(BM_FillHoldUnderMemoryPressure)
    ->ArgName("set0_hash1_zset2")
    ->Arg(0)
    ->Arg(2)
    ->Iterations(3)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace abyss::engine
