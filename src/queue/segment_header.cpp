#include "abyss/queue/segment_header.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <string>

#include "abyss/queue/wal_entry.h"
#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {

namespace {

core::Error HeaderError(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("segment header: ") + what};
}

}  // namespace

void EncodeSegmentHeader(const SegmentHeader& header, std::vector<std::byte>& out) {
  using namespace binary;
  const size_t start = out.size();

  AppendBytes(out, kSegmentMagic.data(), kSegmentMagic.size());
  WriteU8(out, header.format_major);
  WriteU8(out, header.format_minor);
  WriteU16LE(out, header.flags);
  WriteU32LE(out, header.shard_id);
  WriteU64LE(out, header.base_seq);

  const auto created_us =
      std::chrono::duration_cast<std::chrono::microseconds>(header.created_at.time_since_epoch())
          .count();
  WriteI64LE(out, created_us);

  const std::span<const std::byte> header_bytes(out.data() + start, out.size() - start);
  WriteU32LE(out, Crc32c(header_bytes));
}

core::Result<SegmentHeader> DecodeSegmentHeader(std::span<const std::byte> bytes) {
  using namespace binary;

  if (bytes.size() < kSegmentHeaderSize) {
    return std::unexpected(HeaderError("truncated"));
  }
  bytes = bytes.first(kSegmentHeaderSize);

  if (!std::equal(kSegmentMagic.begin(), kSegmentMagic.end(), bytes.begin())) {
    return std::unexpected(HeaderError("bad magic"));
  }

  const std::span<const std::byte> crc_covered = bytes.first(kSegmentHeaderSize - sizeof(uint32_t));
  std::span<const std::byte> cursor = bytes.subspan(kSegmentMagic.size());

  SegmentHeader header;

  if (!ReadU8(cursor, header.format_major)) {
    return std::unexpected(HeaderError("missing format_major"));
  }
  if (header.format_major != kWalFormatMajor) {
    return std::unexpected(HeaderError("unsupported format_major"));
  }

  if (!ReadU8(cursor, header.format_minor)) {
    return std::unexpected(HeaderError("missing format_minor"));
  }
  if (!ReadU16LE(cursor, header.flags)) {
    return std::unexpected(HeaderError("missing flags"));
  }

  uint32_t shard_id = 0;
  if (!ReadU32LE(cursor, shard_id)) {
    return std::unexpected(HeaderError("missing shard_id"));
  }
  header.shard_id = shard_id;

  uint64_t base_seq = 0;
  if (!ReadU64LE(cursor, base_seq)) {
    return std::unexpected(HeaderError("missing base_seq"));
  }
  header.base_seq = base_seq;

  int64_t created_us = 0;
  if (!ReadI64LE(cursor, created_us)) {
    return std::unexpected(HeaderError("missing created_at_us"));
  }
  header.created_at = core::WallTime(std::chrono::microseconds(created_us));

  uint32_t stored_crc = 0;
  if (!ReadU32LE(cursor, stored_crc)) {
    return std::unexpected(HeaderError("missing header_crc"));
  }

  if (Crc32c(crc_covered) != stored_crc) {
    return std::unexpected(HeaderError("header CRC mismatch"));
  }

  return header;
}

}  // namespace abyss::queue
