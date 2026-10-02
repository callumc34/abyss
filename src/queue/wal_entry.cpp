#include "abyss/queue/wal_entry.h"

#include <chrono>
#include <string>
#include <utility>

#include "binary_io.h"
#include "crc32c.h"
#include "entry_payload.h"

namespace abyss::queue {

namespace {

core::Error Truncated(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("WAL entry truncated: ") + what};
}

core::Error Corrupted(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("WAL entry corruption: ") + what};
}

}  // namespace

size_t EncodeWalEntry(const core::QueueEntry& entry, core::SequenceId batch_last_seq,
                      std::vector<std::byte>& out) {
  using namespace binary;
  const size_t start = out.size();

  WriteU32LE(out, 0);
  const size_t body_start = out.size();

  WriteU8(out, static_cast<uint8_t>(entry_payload::TypeOf(entry)));
  WriteU64LE(out, entry.seq);

  const auto appended_us =
      std::chrono::duration_cast<std::chrono::microseconds>(entry.appended_at.time_since_epoch())
          .count();
  WriteI64LE(out, appended_us);

  entry_payload::Encode(entry, out);

  WriteU64LE(out, batch_last_seq);

  const size_t body_len = out.size() - body_start;
  PatchU32LE(out, start, static_cast<uint32_t>(body_len));

  const std::span<const std::byte> body(out.data() + body_start, body_len);
  WriteU32LE(out, Crc32c(body));

  return out.size() - start;
}

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes,
                                             uint8_t format_minor) {
  WalDecodeFailure ignored = WalDecodeFailure::kNone;
  return DecodeWalEntry(bytes, format_minor, ignored);
}

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes, uint8_t format_minor,
                                             WalDecodeFailure& failure) {
  using namespace binary;
  const size_t initial_size = bytes.size();

  // Pre-CRC failures are torn tails (a partially-written, never-acked record).
  failure = WalDecodeFailure::kTornTail;

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

  // The CRC validated: any further failure is genuine corruption of a
  // durably-written frame, not a torn tail.
  failure = WalDecodeFailure::kCorruptFrame;

  std::span<const std::byte> cursor = body;

  uint8_t type_byte = 0;
  if (!ReadU8(cursor, type_byte)) {
    return std::unexpected(Corrupted("missing type"));
  }

  core::QueueEntry qe;

  uint64_t seq = 0;
  if (!ReadU64LE(cursor, seq)) {
    return std::unexpected(Corrupted("missing seq"));
  }
  qe.seq = seq;

  int64_t appended_us = 0;
  if (!ReadI64LE(cursor, appended_us)) {
    return std::unexpected(Corrupted("missing appended_us"));
  }
  qe.appended_at = core::WallTime(std::chrono::microseconds(appended_us));

  auto payload = entry_payload::Decode(static_cast<WalEntryType>(type_byte), cursor);
  if (!payload.has_value()) return std::unexpected(payload.error());
  qe.payload = std::move(*payload);

  core::SequenceId batch_last_seq = qe.seq;
  if (format_minor >= 1) {
    uint64_t raw = 0;
    if (!ReadU64LE(cursor, raw)) {
      return std::unexpected(Corrupted("missing batch_last_seq"));
    }
    if (raw < qe.seq) {
      return std::unexpected(Corrupted("batch_last_seq < seq"));
    }
    batch_last_seq = raw;
  }

  failure = WalDecodeFailure::kNone;
  return DecodedWalEntry{
      .entry = std::move(qe),
      .bytes_consumed = initial_size - bytes.size(),
      .batch_last_seq = batch_last_seq,
  };
}

}  // namespace abyss::queue
