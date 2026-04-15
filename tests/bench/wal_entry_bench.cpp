#include <benchmark/benchmark.h>

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

#include "abyss/core/queue.h"
#include "abyss/queue/wal_entry.h"

namespace abyss::queue {
namespace {

core::LogEntry MakeEntry(size_t value_size) {
  core::LogEntry e;
  e.seq = 1;
  e.appended_at = core::WallClock::now();
  e.cmd.args = {"SET", "key", std::string(value_size, 'x')};
  return e;
}

void BM_Encode(benchmark::State& state) {
  const auto entry = MakeEntry(static_cast<size_t>(state.range(0)));
  std::vector<std::byte> buf;
  buf.reserve(1 << 17);

  for (auto _ : state) {
    buf.clear();
    EncodeWalEntry(entry, buf);
    benchmark::DoNotOptimize(buf.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(buf.size()));
}
BENCHMARK(BM_Encode)->Arg(3)->Arg(1 << 10)->Arg(1 << 16);

void BM_Decode(benchmark::State& state) {
  const auto entry = MakeEntry(static_cast<size_t>(state.range(0)));
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  for (auto _ : state) {
    auto result = DecodeWalEntry(buf);
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(buf.size()));
}
BENCHMARK(BM_Decode)->Arg(3)->Arg(1 << 10)->Arg(1 << 16);

}  // namespace
}  // namespace abyss::queue

BENCHMARK_MAIN();
