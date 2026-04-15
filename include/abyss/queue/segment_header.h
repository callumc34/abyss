#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::queue {

inline constexpr size_t kSegmentHeaderSize = 32;

inline constexpr std::array<std::byte, 4> kSegmentMagic{std::byte{0x41}, std::byte{0x57},
                                                        std::byte{0x41}, std::byte{0x4C}};

struct SegmentHeader {
  uint8_t format_major = 0;
  uint8_t format_minor = 0;
  uint16_t flags = 0;
  core::ShardId shard_id = 0;
  core::SequenceId base_seq = 0;
  core::WallTime created_at;
};

// Encodes the segment header into `out`. Appends exactly kSegmentHeaderSize bytes.
void EncodeSegmentHeader(const SegmentHeader& header, std::vector<std::byte>& out);

// Decodes the segment header. `bytes` must be at least kSegmentHeaderSize bytes.
// Rejects segments whose `format_major` differs from the reader's known major.
core::Result<SegmentHeader> DecodeSegmentHeader(std::span<const std::byte> bytes);

}  // namespace abyss::queue
