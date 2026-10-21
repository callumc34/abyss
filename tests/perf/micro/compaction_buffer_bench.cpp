#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <string>

#include "abyss/consumer/buffer_entry.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "footprint.h"

// Single-threaded lower-bound measurement of compaction buffer Read. The
// realistic-load measurement lives in tests/perf/probe/buffer_probe.cpp.

namespace abyss::consumer {
namespace {

std::string KeyFor(int64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020lld", static_cast<long long>(idx));
  return buf.data();
}

struct BufferFixture {
  CompactionBuffer buffer;
  std::string value;

  BufferFixture(int64_t key_count, size_t value_size) : value(value_size, 'x') {
    for (int64_t i = 0; i < key_count; ++i) {
      const auto key = KeyFor(i);
      const core::ops::StringSet set_op{.key = key, .value = value, .abs_ttl_ms = 0};
      const core::ops::WriteOp op = set_op;
      const auto seq = core::kFirstSeq + static_cast<core::SequenceId>(i);
      buffer.Absorb(key, op, core::EvictionTTL{86400}, seq, 0);
    }
  }
};

void BM_BufferRead(benchmark::State& state) {
  const auto key_count = state.range(0);
  const auto value_size = static_cast<size_t>(state.range(1));
  BufferFixture fixture{key_count, value_size};
  int64_t i = 0;
  for ([[maybe_unused]] auto _ : state) {
    const auto key = KeyFor(i % key_count);
    benchmark::DoNotOptimize(fixture.buffer.Snapshot(key));
    ++i;
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_BufferRead)->Args({10000, 64})->Args({100000, 64})->Unit(benchmark::kNanosecond);

// What one buffered key costs: the process's growth and the buffer's
// own estimate, per key, for distinct SETs of small values.
void BM_BufferMemoryPerEntry(benchmark::State& state) {
  const int64_t keys = state.range(0);
  const auto value_bytes = static_cast<size_t>(state.range(1));
  double footprint = 0;
  double estimate = 0;
  for ([[maybe_unused]] auto _ : state) {
    const size_t before = bench::Footprint();
    BufferFixture fixture{keys, value_bytes};
    footprint = static_cast<double>(bench::Footprint() - before) / static_cast<double>(keys);
    estimate = static_cast<double>(fixture.buffer.BytesEstimate()) / static_cast<double>(keys);
  }
  state.counters["footprint_per_key"] = footprint;
  state.counters["estimate_per_key"] = estimate;
  state.counters["sizeof_entry"] = static_cast<double>(sizeof(BufferEntry));
}
BENCHMARK(BM_BufferMemoryPerEntry)
    ->ArgNames({"keys", "value"})
    ->Args({1'000'000, 16})
    ->Args({1'000'000, 230})
    ->Iterations(1)
    ->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace abyss::consumer
