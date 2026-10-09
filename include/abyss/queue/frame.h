#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

// WAL format 2 frames (ADP-009). Every frame starts 8-byte aligned:
//
//   commit word   u64   {len: low u32, gen: high u32}, stored last
//   crc32c        u32   over body[0, covered), the segment's salt, then
//                       the commit word
//   body          len   kind u8, type u8, shard u16, flags u32,
//                       seq u64, batch_rest u64, appended_at_us i64,
//                       then the entry payload
//   pad                 zeros up to the next 8-byte boundary
//
// A frame is filled iff its gen matches the segment's and len != 0. An
// entry's CRC covers its whole body; a padding frame's covers only its
// fixed header, so a roll never checksums the stale bytes it spans.
// The salt is random per segment life, so bytes a client wrote cannot
// pass for a frame of a later life.
// batch_rest is the bytes from a frame's start to the end of its batch,
// which is one reserved range: a batch is whole iff its last frame,
// the one whose batch_rest equals its size, is filled. An unknown kind
// under a valid CRC is corruption (a newer format), never a torn tail.
namespace abyss::queue::frame {

inline constexpr std::size_t kAlign = 8;
inline constexpr std::size_t kCommitBytes = 8;
inline constexpr std::size_t kCrcBytes = 4;
inline constexpr std::size_t kHeaderBytes = 32;
// The smallest frame: a padding frame with its fixed header only.
inline constexpr std::size_t kMinFrameBytes = 48;

enum class Kind : uint8_t { kEntry = 1, kPadding = 2 };

// Header::flags bits. An unknown bit is corruption.
inline constexpr uint32_t kReplacesState = 1U << 0;

struct Header {
  Kind kind = Kind::kEntry;
  core::ShardId shard = 0;
  // Formerly reserved; ADP-009's frame table predates it.
  uint32_t flags = 0;
  core::SequenceId seq = 0;
  uint64_t batch_rest = 0;
  // Zero for padding.
  int64_t appended_at_us = 0;
};

inline uint64_t CommitWord(uint32_t len, uint32_t gen) noexcept {
  return (static_cast<uint64_t>(gen) << 32) | len;
}
inline uint32_t CommitLen(uint64_t word) noexcept { return static_cast<uint32_t>(word); }
inline uint32_t CommitGen(uint64_t word) noexcept { return static_cast<uint32_t>(word >> 32); }

// Bytes a frame with a `len`-byte body occupies, padding included.
std::size_t FrameSize(std::size_t len) noexcept;

// The append path's encoder. Writes `entry`'s frame into `out`, its
// reserved span, as the frame `batch_rest` bytes from its batch's end:
// the body, zeroed padding and the body CRC. The commit word is left
// for Log::CommitInPlace, which takes the returned body length. Fatal
// unless the frame fills `out` exactly.
uint32_t EncodeEntryInto(const core::QueueEntry& entry, core::ShardId shard,
                         std::span<std::byte> out, uint64_t batch_rest);

// Appends `entry`'s frame to `out` as a batch of one and returns its
// size; the commit word holds only len until Log::Commit. CloseBatch
// re-closes consecutive frames as one batch.
std::size_t EncodeEntry(const core::QueueEntry& entry, core::ShardId shard,
                        std::vector<std::byte>& out);
// The size of `entry`'s frame, without encoding it.
std::size_t EntryFrameSize(const core::QueueEntry& entry);

// Closes `frames`, consecutive EncodeEntry outputs reserved together,
// as one batch: writes each frame's batch_rest, then its body CRC into
// the crc slot. The salt and gen are not known until the frames are
// reserved, so Log::Commit seals the CRC with SealCrc.
void CloseBatch(std::span<std::byte> frames);

// The final CRC of a frame whose body CRC is `body_crc`, in a segment
// with `salt`, under `word`.
uint32_t SealCrc(uint32_t body_crc, uint64_t salt, uint64_t word) noexcept;

// Writes a padding frame spanning exactly `span` bytes (a multiple of
// kAlign, at least kMinFrameBytes) into `dst`, all but its commit word,
// and returns the commit word the caller stores last.
uint64_t EncodePadding(std::size_t span, uint32_t gen, uint64_t salt, std::span<std::byte> dst);

enum class State : uint8_t { kFilled, kUnfilled, kTorn };

struct View {
  State state = State::kUnfilled;
  Header header;
  // Whole frame, padding included; 0 unless kFilled.
  std::size_t size = 0;
  std::span<const std::byte> body;
};

// Classifies the frame at the start of `bytes` for generation `gen` in
// a segment with `salt`.
// kUnfilled: gen differs or len is 0 (end of the log). kTorn: it
// overruns `bytes`, or `verify_crc` is set and the CRC fails. The
// commit word must be loaded by the caller with acquire ordering when
// the bytes are live; `commit_word` is that value.
View Inspect(uint64_t commit_word, std::span<const std::byte> bytes, uint32_t gen, uint64_t salt,
             bool verify_crc);

// Decodes a filled entry frame's payload. kCorruption if the CRC holds
// but the payload does not parse (genuine corruption, ADP-009).
core::Result<core::QueueEntry> DecodeEntry(const View& view);

}  // namespace abyss::queue::frame
