// libFuzzer harness for the WAL format 2 frame codec.
//
// Input: commit word u64, gen u32, salt u64 and a mode byte, then the
// frame bytes. With kModeSeal the harness writes the correct CRC for
// the bytes first, so frame::Inspect accepts them and DecodeEntry sees
// fuzzer-chosen payloads under a valid CRC.
//
// Contract: Inspect, with verify_crc either way, never reads past the
// bytes and never returns kFilled for a frame whose CRC fails under
// verification; DecodeEntry either decodes or reports kCorruption.
// Violations abort, so libFuzzer reports them as crashes.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <span>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/queue/frame.h"
#include "binary_io.h"
#include "crc32c.h"

namespace frame = abyss::queue::frame;

namespace {

constexpr std::size_t kWordAt = 0;
constexpr std::size_t kGenAt = 8;
constexpr std::size_t kSaltAt = 12;
constexpr std::size_t kModeAt = 20;
constexpr std::size_t kBytesAt = 21;
constexpr std::size_t kBodyAt = frame::kCommitBytes + frame::kCrcBytes;
constexpr uint8_t kModeSeal = 0x01;

void Require(bool holds) {
  if (!holds) std::abort();
}

// The CRC a frame must carry, computed apart from the codec: CRC32C of
// the covered body, then the salt, then the commit word.
uint32_t ExpectedCrc(std::span<const std::byte> body, std::size_t covered, uint64_t salt,
                     uint64_t word) {
  std::vector<std::byte> sealed(body.begin(), body.begin() + static_cast<std::ptrdiff_t>(covered));
  sealed.resize(covered + (2 * sizeof(uint64_t)));
  abyss::queue::binary::StoreLE(sealed.data() + covered, salt);
  abyss::queue::binary::StoreLE(sealed.data() + covered + sizeof(uint64_t), word);
  return abyss::queue::Crc32c(sealed);
}

bool CrcHolds(std::span<const std::byte> bytes, uint32_t len, uint64_t salt, uint64_t word) {
  const auto body = bytes.subspan(kBodyAt, len);
  const auto stored = abyss::queue::binary::LoadLE<uint32_t>(bytes.data() + frame::kCommitBytes);
  const bool whole = ExpectedCrc(body, len, salt, word) == stored;
  const bool header = ExpectedCrc(body, frame::kHeaderBytes, salt, word) == stored;
  switch (static_cast<frame::Kind>(body[0])) {
    case frame::Kind::kEntry:
      return whole;
    case frame::Kind::kPadding:
      return header;
  }
  return whole || header;
}

void Check(const frame::View& view, uint64_t word, std::span<const std::byte> bytes, uint32_t gen,
           uint64_t salt, bool verify) {
  const uint32_t len = frame::CommitLen(word);
  const bool live = frame::CommitGen(word) == gen && len != 0;
  if (view.state == frame::State::kUnfilled) {
    Require(!live);
    return;
  }
  Require(live);
  if (view.state == frame::State::kTorn) return;

  Require(len >= frame::kHeaderBytes);
  Require(view.size == frame::FrameSize(len) && view.size <= bytes.size());
  Require(view.body.data() == bytes.data() + kBodyAt && view.body.size() == len);
  if (verify) Require(CrcHolds(bytes, len, salt, word));

  auto entry = frame::DecodeEntry(view);
  if (view.header.kind != frame::Kind::kEntry) {
    Require(!entry.has_value() && entry.error().code() == abyss::core::ErrorCode::kInvalidArgument);
  } else if (entry.has_value()) {
    Require(entry->seq == view.header.seq);
  } else {
    Require(entry.error().code() == abyss::core::ErrorCode::kCorruption);
  }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < kBytesAt) return 0;
  const auto* input = reinterpret_cast<const std::byte*>(data);
  auto word = abyss::queue::binary::LoadLE<uint64_t>(input + kWordAt);
  const auto gen = abyss::queue::binary::LoadLE<uint32_t>(input + kGenAt);
  const auto salt = abyss::queue::binary::LoadLE<uint64_t>(input + kSaltAt);
  const auto mode = static_cast<uint8_t>(input[kModeAt]);

  // An exact-size copy, so any read past it is an ASan report.
  std::vector<std::byte> bytes(input + kBytesAt, input + size);
  if ((mode & kModeSeal) != 0) {
    word = frame::CommitWord(frame::CommitLen(word), gen);
    const uint32_t len = frame::CommitLen(word);
    if (len >= frame::kHeaderBytes && kBodyAt + len <= bytes.size()) {
      const auto body = std::span<const std::byte>(bytes).subspan(kBodyAt, len);
      const std::size_t covered =
          static_cast<frame::Kind>(body[0]) == frame::Kind::kPadding ? frame::kHeaderBytes : len;
      abyss::queue::binary::StoreLE(bytes.data() + frame::kCommitBytes,
                                    ExpectedCrc(body, covered, salt, word));
    }
  }

  const auto unverified = frame::Inspect(word, bytes, gen, salt, false);
  Check(unverified, word, bytes, gen, salt, false);
  const auto verified = frame::Inspect(word, bytes, gen, salt, true);
  Check(verified, word, bytes, gen, salt, true);
  // Verification only ever turns a filled frame torn.
  if (verified.state == frame::State::kFilled) {
    Require(unverified.state == frame::State::kFilled && unverified.size == verified.size &&
            unverified.header.seq == verified.header.seq);
  }
  if (unverified.state != frame::State::kFilled) Require(verified.state == unverified.state);
  return 0;
}
