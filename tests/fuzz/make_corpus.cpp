// Writes the seed corpora of frame_decoder_fuzz and log_recovery_fuzz
// with the real encoder, so the seeds follow the format:
//
//   fuzz_make_corpus <corpus dir>
//
// replaces <dir>/frame_decoder and <dir>/log_recovery. The output is
// deterministic: seqs, timestamps, gens and salts are fixed.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/queue/frame.h"
#include "binary_io.h"

namespace core = abyss::core;
namespace frame = abyss::queue::frame;
namespace binary = abyss::queue::binary;

namespace {

using Bytes = std::vector<std::byte>;

constexpr uint64_t kSalt = 0x5eed5a175eed5a17;
constexpr uint32_t kGen = 7;
constexpr std::size_t kBodyAt = frame::kCommitBytes + frame::kCrcBytes;
constexpr std::size_t kKindAt = 0;
// As in the harnesses.
constexpr uint8_t kModeSeal = 0x01;
constexpr uint8_t kModeReframe = 0x01;
constexpr uint8_t kModeSingleBatch = 0x02;
constexpr uint8_t kModeTear = 0x04;
constexpr uint8_t kModeNextSegment = 0x08;

core::QueueEntry At(core::SequenceId seq, decltype(core::QueueEntry::payload) payload) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallTime(std::chrono::microseconds{1'700'000'000'000'000}),
      .payload = std::move(payload),
  };
}

core::QueueEntry Write(core::SequenceId seq, const std::string& key) {
  return At(seq, core::entry::Write{.cmd = core::RespCommand{{"SET", key, "v"}}});
}

core::QueueEntry Flush(core::SequenceId seq) { return At(seq, core::entry::Flush{}); }

// Consecutive frames reserved as one batch, CRCs not yet sealed.
Bytes Batch(std::span<const core::QueueEntry> entries, core::ShardId shard) {
  Bytes out;
  for (const auto& entry : entries) frame::EncodeEntry(entry, shard, out);
  frame::CloseBatch(out);
  return out;
}

Bytes Frame(const core::QueueEntry& entry, core::ShardId shard) {
  return Batch(std::span(&entry, 1), shard);
}

uint32_t LenOf(const Bytes& frame_bytes) {
  return frame::CommitLen(binary::LoadLE<uint64_t>(frame_bytes.data()));
}

// Seals the frame's CRC for `gen` and kSalt and stores its commit word.
uint64_t Seal(Bytes& frame_bytes, uint32_t gen) {
  const uint64_t word = frame::CommitWord(LenOf(frame_bytes), gen);
  const auto body_crc = binary::LoadLE<uint32_t>(frame_bytes.data() + frame::kCommitBytes);
  binary::StoreLE(frame_bytes.data() + frame::kCommitBytes, frame::SealCrc(body_crc, kSalt, word));
  binary::StoreLE(frame_bytes.data(), word);
  return word;
}

// frame_decoder_fuzz input: commit word, gen, salt, mode, frame bytes.
Bytes DecoderSeed(uint64_t word, const Bytes& frame_bytes, uint8_t mode = 0) {
  Bytes out;
  binary::WriteU64LE(out, word);
  binary::WriteU32LE(out, kGen);
  binary::WriteU64LE(out, kSalt);
  binary::WriteU8(out, mode);
  out.insert(out.end(), frame_bytes.begin(), frame_bytes.end());
  return out;
}

Bytes Sealed(const core::QueueEntry& entry) {
  Bytes bytes = Frame(entry, 1);
  const uint64_t word = Seal(bytes, kGen);
  return DecoderSeed(word, bytes);
}

// log_recovery_fuzz reframe records: per frame a length byte, then its
// body.
void AddRecords(Bytes& out, const Bytes& frames) {
  for (std::size_t off = 0; off < frames.size();) {
    const uint32_t len = frame::CommitLen(binary::LoadLE<uint64_t>(frames.data() + off));
    binary::WriteU8(out, static_cast<uint8_t>(len - frame::kHeaderBytes));
    out.insert(out.end(), frames.begin() + static_cast<std::ptrdiff_t>(off + kBodyAt),
               frames.begin() + static_cast<std::ptrdiff_t>(off + kBodyAt + len));
    off += frame::FrameSize(len);
  }
}

Bytes RecoverySeed(uint8_t mode, const std::vector<Bytes>& frames) {
  Bytes out{static_cast<std::byte>(mode)};
  for (const auto& f : frames) AddRecords(out, f);
  return out;
}

bool Emit(const std::filesystem::path& dir, const std::string& name, const Bytes& bytes) {
  std::ofstream out(dir / name, std::ios::binary | std::ios::trunc);
  out.write(reinterpret_cast<const char*>(bytes.data()),
            static_cast<std::streamsize>(bytes.size()));
  return out.good();
}

std::vector<std::pair<std::string, Bytes>> DecoderSeeds() {
  std::vector<std::pair<std::string, Bytes>> seeds;
  seeds.emplace_back("write", Sealed(Write(3, "key")));
  seeds.emplace_back("flush", Sealed(Flush(6)));

  Bytes padding(frame::kMinFrameBytes + 16);
  const uint64_t padding_word = frame::EncodePadding(padding.size(), kGen, kSalt, padding);
  binary::StoreLE(padding.data(), padding_word);
  seeds.emplace_back("padding", DecoderSeed(padding_word, padding));

  Bytes torn = Frame(Write(3, "key"), 1);
  const uint64_t torn_word = Seal(torn, kGen);
  torn[kBodyAt + frame::kHeaderBytes] ^= std::byte{0x40};
  seeds.emplace_back("torn_crc", DecoderSeed(torn_word, torn));

  Bytes stale = Frame(Write(3, "key"), 1);
  seeds.emplace_back("stale_gen", DecoderSeed(Seal(stale, kGen + 1), stale));
  seeds.emplace_back("zero_len", DecoderSeed(frame::CommitWord(0, kGen), Bytes(64)));

  Bytes overrun = Frame(Write(3, "key"), 1);
  const uint64_t overrun_word = Seal(overrun, kGen);
  overrun.resize(overrun.size() - frame::kAlign);
  seeds.emplace_back("overrun", DecoderSeed(overrun_word, overrun));

  Bytes unknown = Frame(Write(3, "key"), 1);
  unknown[kBodyAt + kKindAt] = std::byte{9};
  seeds.emplace_back("unknown_kind",
                     DecoderSeed(frame::CommitWord(LenOf(unknown), kGen), unknown, kModeSeal));

  // 0x01 and 0x02 were the conditional and resolved types; reserved now.
  Bytes reserved = Frame(Write(3, "key"), 1);
  reserved[kBodyAt + kKindAt + 1] = std::byte{0x01};
  seeds.emplace_back("reserved_type",
                     DecoderSeed(frame::CommitWord(LenOf(reserved), kGen), reserved, kModeSeal));

  Bytes garbled = Frame(Write(3, "key"), 1);
  garbled[kBodyAt + frame::kHeaderBytes + 1] = std::byte{0xff};
  seeds.emplace_back("garbled_payload",
                     DecoderSeed(frame::CommitWord(LenOf(garbled), kGen), garbled, kModeSeal));
  return seeds;
}

// The harness's template holds shard 0 seqs 0-1, shard 1 seq 0 and
// shard 2 seqs 0-1; these continue it.
std::vector<std::pair<std::string, Bytes>> RecoverySeeds() {
  const Bytes s1 = Frame(Write(1, "f"), 1);
  const Bytes s3 = Frame(Write(0, "g"), 3);
  const Bytes s0 = Frame(Write(2, "h"), 0);
  const std::vector<core::QueueEntry> pair{Write(2, "i"), Write(3, "j")};
  const Bytes batch = Batch(pair, 2);

  std::vector<std::pair<std::string, Bytes>> seeds;
  seeds.emplace_back("clean_end", Bytes(64));
  // Segment 0's gen with a CRC that cannot hold.
  Bytes raw(65, std::byte{0xa5});
  raw[0] = std::byte{0};
  binary::StoreLE(raw.data() + 1, frame::CommitWord(40, 0));
  seeds.emplace_back("raw_torn", raw);
  seeds.emplace_back("entries", RecoverySeed(kModeReframe | kModeSingleBatch, {s1, s3, s0}));
  seeds.emplace_back("batch", RecoverySeed(kModeReframe, {batch}));
  const auto first = static_cast<std::ptrdiff_t>(frame::FrameSize(LenOf(batch)));
  seeds.emplace_back("cut_batch",
                     RecoverySeed(kModeReframe, {Bytes(batch.begin(), batch.begin() + first)}));
  seeds.emplace_back("torn_tail",
                     RecoverySeed(kModeReframe | kModeSingleBatch | kModeTear, {s1, s3}));
  seeds.emplace_back("seq_gap", RecoverySeed(kModeReframe, {Frame(Write(9, "k"), 0)}));
  Bytes unknown = s1;
  unknown[kBodyAt + kKindAt] = std::byte{9};
  seeds.emplace_back("unknown_kind", RecoverySeed(kModeReframe, {unknown}));
  Bytes garbled = s3;
  garbled[kBodyAt + frame::kHeaderBytes + 1] = std::byte{0xff};
  seeds.emplace_back("garbled_payload", RecoverySeed(kModeReframe, {garbled}));
  seeds.emplace_back("next_segment",
                     RecoverySeed(kModeReframe | kModeSingleBatch | kModeNextSegment, {s1, s3}));
  return seeds;
}

}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: fuzz_make_corpus <corpus dir>\n";
    return 2;
  }
  const std::filesystem::path root(argv[1]);
  for (const auto& [name, seeds] : {std::pair{std::string("frame_decoder"), DecoderSeeds()},
                                    std::pair{std::string("log_recovery"), RecoverySeeds()}}) {
    const auto dir = root / name;
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    if (ec) {
      std::cerr << "fuzz_make_corpus: " << dir.string() << ": " << ec.message() << '\n';
      return 1;
    }
    for (const auto& [seed, bytes] : seeds) {
      if (!Emit(dir, seed, bytes)) {
        std::cerr << "fuzz_make_corpus: cannot write " << (dir / seed).string() << '\n';
        return 1;
      }
    }
  }
  return 0;
}
