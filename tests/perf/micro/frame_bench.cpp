#include <benchmark/benchmark.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/frame.h"

namespace abyss::queue {
namespace {

static_assert(std::endian::native == std::endian::little, "frames are little-endian");

constexpr uint64_t kSalt = 0x5eed5a175eed5a17;
constexpr uint32_t kGen = 1;

core::QueueEntry MakeEntry(size_t value_size) {
  return core::QueueEntry{
      .seq = 1,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", "key",
                                                              std::string(value_size, 'x')}}},
  };
}

uint32_t LoadU32(const std::byte* at) {
  uint32_t value = 0;
  std::memcpy(&value, at, sizeof(value));
  return value;
}

// Everything an append computes for a frame in its reserved span:
// encode it in place as a batch of one, then seal its CRC.
uint64_t EncodeSealed(const core::QueueEntry& entry, std::span<std::byte> out) {
  const uint64_t word = frame::CommitWord(frame::EncodeEntryInto(entry, 0, out, out.size()), kGen);
  const uint32_t crc = frame::SealCrc(LoadU32(out.data() + frame::kCommitBytes), kSalt, word);
  std::memcpy(out.data() + frame::kCommitBytes, &crc, sizeof(crc));
  std::memcpy(out.data(), &word, sizeof(word));
  return word;
}

void BM_FrameEncode(benchmark::State& state) {
  const auto entry = MakeEntry(static_cast<size_t>(state.range(0)));
  std::vector<std::byte> buf(frame::EntryFrameSize(entry));
  for ([[maybe_unused]] auto _ : state) {
    benchmark::DoNotOptimize(EncodeSealed(entry, buf));
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(buf.size()));
}
BENCHMARK(BM_FrameEncode)->Arg(64)->Arg(1 << 10)->Arg(1 << 16);

// A consumer read: classify with the CRC verified, then decode.
void BM_FrameDecode(benchmark::State& state) {
  const auto entry = MakeEntry(static_cast<size_t>(state.range(0)));
  std::vector<std::byte> buf(frame::EntryFrameSize(entry));
  const uint64_t word = EncodeSealed(entry, buf);
  for ([[maybe_unused]] auto _ : state) {
    const frame::View view = frame::Inspect(word, buf, kGen, kSalt, true);
    auto decoded = frame::DecodeEntry(view);
    if (!decoded.has_value()) {
      state.SkipWithError("decode failed");
      break;
    }
    benchmark::DoNotOptimize(decoded);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(buf.size()));
}
BENCHMARK(BM_FrameDecode)->Arg(64)->Arg(1 << 10)->Arg(1 << 16);

}  // namespace
}  // namespace abyss::queue

BENCHMARK_MAIN();
