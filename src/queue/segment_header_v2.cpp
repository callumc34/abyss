#include "segment_header_v2.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {

namespace {

// magic[4] major u8 minor u8 reserved u16 log_id u32 shard_count u32
// ordinal u64 created_at_us i64 window u64 salt u64, then crc32c u32.
constexpr std::size_t kFieldBytes = 48;

core::Error HeaderError(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("log segment header: ") + what};
}

}  // namespace

void EncodeLogSegmentHeader(const LogSegmentHeader& header, std::span<std::byte> out) {
  using namespace binary;
  std::vector<std::byte> buf;
  buf.reserve(kFieldBytes + sizeof(uint32_t));
  AppendBytes(buf, kLogSegmentMagic.data(), kLogSegmentMagic.size());
  WriteU8(buf, kLogFormatMajor);
  WriteU8(buf, header.format_minor);
  WriteU16LE(buf, 0);
  WriteU32LE(buf, header.log_id);
  WriteU32LE(buf, header.shard_count);
  WriteU64LE(buf, header.ordinal);
  WriteI64LE(buf, std::chrono::duration_cast<std::chrono::microseconds>(
                      header.created_at.time_since_epoch())
                      .count());
  WriteU64LE(buf, header.durability_window_bytes);
  WriteU64LE(buf, header.salt);
  WriteU32LE(buf, Crc32c(buf));

  std::ranges::fill(out, std::byte{0});
  std::memcpy(out.data(), buf.data(), std::min(buf.size(), out.size()));
}

core::Result<LogSegmentHeader> DecodeLogSegmentHeader(std::span<const std::byte> bytes) {
  using namespace binary;
  if (bytes.size() < kFieldBytes + sizeof(uint32_t)) {
    return std::unexpected(HeaderError("truncated"));
  }
  if (!std::equal(kLogSegmentMagic.begin(), kLogSegmentMagic.end(), bytes.begin())) {
    return std::unexpected(HeaderError("bad magic"));
  }
  std::span<const std::byte> cursor = bytes.subspan(kLogSegmentMagic.size());
  LogSegmentHeader header;
  uint8_t major = 0;
  uint16_t reserved = 0;
  uint32_t stored_crc = 0;
  int64_t created_us = 0;
  // Fixed-width reads cannot fail: the size was checked above.
  ReadU8(cursor, major);
  ReadU8(cursor, header.format_minor);
  ReadU16LE(cursor, reserved);
  ReadU32LE(cursor, header.log_id);
  ReadU32LE(cursor, header.shard_count);
  ReadU64LE(cursor, header.ordinal);
  ReadI64LE(cursor, created_us);
  ReadU64LE(cursor, header.durability_window_bytes);
  ReadU64LE(cursor, header.salt);
  ReadU32LE(cursor, stored_crc);
  if (major != kLogFormatMajor) {
    return std::unexpected(HeaderError("unsupported format major"));
  }
  if (Crc32c(bytes.first(kFieldBytes)) != stored_crc) {
    return std::unexpected(HeaderError("CRC mismatch"));
  }
  header.created_at = core::WallTime(std::chrono::microseconds(created_us));
  return header;
}

}  // namespace abyss::queue
