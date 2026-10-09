#include <benchmark/benchmark.h>

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

// A full load of a 1M-member set and a 1M-field hash from RocksDB:
// Loader::Load off the lock, then CompleteLoad, the install's
// exclusive-lock hold, timed alone.

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
  core::Result<core::RespValue> Read(std::string_view /*key*/) const override {
    return std::unexpected(core::Error{core::ErrorCode::kNotFound, "none"});
  }
  consumer::BufferKeyPresence Probe(std::string_view /*key*/) const override {
    return consumer::BufferKeyPresence::kAbsent;
  }
  consumer::HashOverlay HashOverlayFor(std::string_view /*key*/) const override { return {}; }
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
      for (const auto& name : names) {
        set.members.emplace_back(name);
        hash.fields.push_back({.field = name, .value = "value"});
      }
      const std::vector<core::ops::WriteOp> batch{set, hash};
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

void BM_FullLoadAndInstall(benchmark::State& state) {
  const std::string key = state.range(0) == 0 ? "set" : "hash";
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
    ->ArgName("hash")
    ->Arg(0)
    ->Arg(1)
    ->Iterations(3)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace abyss::engine
