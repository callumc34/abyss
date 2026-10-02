#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::queue {

inline constexpr uint8_t kLogFormatMajor = 2;
inline constexpr uint8_t kLogFormatMinor = 0;
// One block, so frames start page-aligned.
inline constexpr std::size_t kLogSegmentHeaderBytes = 4096;
// "AWAL", as in format 1; the major tells them apart.
inline constexpr std::array<std::byte, 4> kLogSegmentMagic{std::byte{0x41}, std::byte{0x57},
                                                           std::byte{0x41}, std::byte{0x4C}};

struct LogSegmentHeader {
  uint8_t format_minor = kLogFormatMinor;
  uint32_t log_id = 0;
  uint64_t ordinal = 0;
  core::WallTime created_at;
  uint32_t shard_count = 0;
  uint64_t durability_window_bytes = 0;
  // Random per segment life; every frame CRC in it covers it.
  uint64_t salt = 0;
};

// Fills all of `out` (kLogSegmentHeaderBytes): the fields, their CRC,
// then zeros.
void EncodeLogSegmentHeader(const LogSegmentHeader& header, std::span<std::byte> out);

// kCorruption for a bad magic, an unknown major or a CRC mismatch.
core::Result<LogSegmentHeader> DecodeLogSegmentHeader(std::span<const std::byte> bytes);

}  // namespace abyss::queue
