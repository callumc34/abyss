#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "abyss/core/result.h"

// Neutral home for the LEB128 varint codec the cold key encoder uses,
// in core so another layer can share its byte-for-byte length-prefix
// scheme without a cold include edge.
namespace abyss::core::encoding {

inline void AppendVarint(std::string& out, uint64_t value) {
  // NOLINTNEXTLINE(bugprone-infinite-loop)
  while (value >= 0x80) {
    out.push_back(static_cast<char>((value & 0x7F) | 0x80));
    value >>= 7;
  }
  out.push_back(static_cast<char>(value & 0x7F));
}

// Consumes the varint from the front of `bytes` on success and advances it.
inline Result<uint64_t> DecodeVarint(std::string_view& bytes) {
  uint64_t value = 0;
  int shift = 0;
  size_t consumed = 0;
  for (; consumed < bytes.size(); ++consumed) {
    const auto byte = static_cast<uint8_t>(bytes[consumed]);

    if (shift >= 70) {
      return std::unexpected(Error(ErrorCode::kCorruption, "varint overflows uint64"));
    }
    if (shift == 63 && (byte & 0x7E) != 0) {
      return std::unexpected(Error(ErrorCode::kCorruption, "varint overflows uint64"));
    }

    value |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {
      bytes.remove_prefix(consumed + 1);
      return value;
    }
    shift += 7;
  }
  return std::unexpected(Error(ErrorCode::kCorruption, "truncated varint"));
}

}  // namespace abyss::core::encoding
