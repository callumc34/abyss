#include "abyss/queue/wal_entry.h"

#include <chrono>
#include <string>

#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {

namespace {

core::Error Truncated(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("WAL entry truncated: ") + what};
}

core::Error Corrupted(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("WAL entry corruption: ") + what};
}

}  // namespace

size_t EncodeWalEntry(const core::LogEntry& entry, std::vector<std::byte>& out) {
  using namespace binary;
  const size_t start = out.size();

  WriteU32LE(out, 0);
  const size_t body_start = out.size();

  WriteU8(out, static_cast<uint8_t>(WalEntryType::kRespCommand));
  WriteU64LE(out, entry.seq);

  const auto appended_us =
      std::chrono::duration_cast<std::chrono::microseconds>(entry.appended_at.time_since_epoch())
          .count();
  WriteI64LE(out, appended_us);

  WriteU32LE(out, static_cast<uint32_t>(entry.cmd.args.size()));
  for (const auto& arg : entry.cmd.args) {
    WriteU32LE(out, static_cast<uint32_t>(arg.size()));
    AppendBytes(out, arg.data(), arg.size());
  }

  const size_t body_len = out.size() - body_start;
  PatchU32LE(out, start, static_cast<uint32_t>(body_len));

  std::span<const std::byte> body(out.data() + body_start, body_len);
  WriteU32LE(out, Crc32c(body));

  return out.size() - start;
}

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes) {
  using namespace binary;
  const size_t initial_size = bytes.size();

  uint32_t body_len = 0;
  if (!ReadU32LE(bytes, body_len)) {
    return std::unexpected(Truncated("length prefix"));
  }
  if (bytes.size() < static_cast<size_t>(body_len) + sizeof(uint32_t)) {
    return std::unexpected(Truncated("body or crc"));
  }

  const std::span<const std::byte> body = bytes.first(body_len);
  bytes = bytes.subspan(body_len);

  uint32_t stored_crc = 0;
  if (!ReadU32LE(bytes, stored_crc)) {
    return std::unexpected(Truncated("crc"));
  }

  if (Crc32c(body) != stored_crc) {
    return std::unexpected(Corrupted("body CRC mismatch"));
  }

  std::span<const std::byte> cursor = body;

  uint8_t type = 0;
  if (!ReadU8(cursor, type)) {
    return std::unexpected(Corrupted("missing type"));
  }
  if (type != static_cast<uint8_t>(WalEntryType::kRespCommand)) {
    return std::unexpected(Corrupted("unknown entry type"));
  }

  core::LogEntry entry;

  uint64_t seq = 0;
  if (!ReadU64LE(cursor, seq)) {
    return std::unexpected(Corrupted("missing seq"));
  }
  entry.seq = seq;

  int64_t appended_us = 0;
  if (!ReadI64LE(cursor, appended_us)) {
    return std::unexpected(Corrupted("missing appended_us"));
  }
  entry.appended_at = core::WallTime(std::chrono::microseconds(appended_us));

  uint32_t arg_count = 0;
  if (!ReadU32LE(cursor, arg_count)) {
    return std::unexpected(Corrupted("missing arg_count"));
  }

  entry.cmd.args.reserve(arg_count);
  for (uint32_t i = 0; i < arg_count; ++i) {
    uint32_t arg_len = 0;
    if (!ReadU32LE(cursor, arg_len)) {
      return std::unexpected(Corrupted("missing arg length"));
    }
    if (cursor.size() < arg_len) {
      return std::unexpected(Corrupted("missing arg bytes"));
    }
    const auto* data = reinterpret_cast<const char*>(cursor.data());
    entry.cmd.args.emplace_back(data, arg_len);
    cursor = cursor.subspan(arg_len);
  }

  return DecodedWalEntry{.entry = std::move(entry), .bytes_consumed = initial_size - bytes.size()};
}

}  // namespace abyss::queue
