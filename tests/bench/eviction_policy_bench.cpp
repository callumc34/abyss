#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <vector>

#include "abyss/core/eviction_policy.h"

namespace abyss::core {
namespace {

std::vector<EvictionRule> MakeOverrides(size_t count) {
  std::vector<EvictionRule> rules;
  rules.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    rules.push_back({
        .prefix = "prefix_" + std::to_string(i) + ":",
        .eviction = EvictionTTL{60 + static_cast<int64_t>(i)},
    });
  }
  return rules;
}

std::string MakeKey(size_t length) {
  std::string key = "no_match:";
  while (key.size() < length) key += 'x';
  key.resize(length);
  return key;
}

void BM_Resolve(benchmark::State& state) {
  const auto override_count = static_cast<size_t>(state.range(0));
  const auto key_length = static_cast<size_t>(state.range(1));

  EvictionPolicy policy{EvictionTTL{86400}, MakeOverrides(override_count)};
  const std::string key = MakeKey(key_length);

  for (auto _ : state) {
    auto ttl = policy.Resolve(key);
    benchmark::DoNotOptimize(ttl);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_Resolve)
    ->Args({1, 16})
    ->Args({1, 64})
    ->Args({10, 16})
    ->Args({10, 64})
    ->Args({10, 256})
    ->Args({50, 64});

}  // namespace
}  // namespace abyss::core
