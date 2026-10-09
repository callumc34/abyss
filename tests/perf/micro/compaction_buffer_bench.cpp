#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <string>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"

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
      const auto seq = static_cast<core::SequenceId>(i);
      buffer.Absorb(key, op, core::EvictionTTL{86400}, seq, seq, 0);
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
    benchmark::DoNotOptimize(fixture.buffer.Read(key));
    ++i;
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}
BENCHMARK(BM_BufferRead)->Args({10000, 64})->Args({100000, 64})->Unit(benchmark::kNanosecond);

}  // namespace
}  // namespace abyss::consumer
