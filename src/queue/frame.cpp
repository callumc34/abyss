#include "abyss/queue/frame.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <string>
#include <utility>

#include "abyss/core/fatal.h"
#include "binary_io.h"
#include "crc32c.h"
#include "entry_payload.h"

namespace abyss::queue::frame {

namespace {

constexpr std::size_t kBodyAt = kCommitBytes + kCrcBytes;

// Offsets within the body's fixed header.
constexpr std::size_t kTypeAt = 1;
constexpr std::size_t kShardAt = 2;
constexpr std::size_t kFlagsAt = 4;
constexpr std::size_t kSeqAt = 8;
constexpr std::size_t kBatchRestAt = 16;
constexpr std::size_t kAppendedAt = 24;

static_assert(kAppendedAt + sizeof(int64_t) == kHeaderBytes);
static_assert(kMinFrameBytes == ((kBodyAt + kHeaderBytes + kAlign - 1) & ~(kAlign - 1)));

using binary::LoadLE;
using binary::StoreLE;

uint32_t BodyCrc(std::span<const std::byte> body, std::size_t covered) {
  return Crc32c(body.first(covered));
}

// An unknown kind counts as intact under either coverage, so the
// caller can fail closed on it rather than call it torn.
bool CrcHolds(Kind kind, std::span<const std::byte> body, uint64_t salt, uint64_t word,
              uint32_t stored) {
  const auto valid = [&](std::size_t covered) {
    return SealCrc(BodyCrc(body, covered), salt, word) == stored;
  };
  switch (kind) {
    case Kind::kPadding:
      return valid(kHeaderBytes);
    case Kind::kEntry:
      return valid(body.size());
  }
  return valid(body.size()) || valid(kHeaderBytes);
}

core::Error Corrupt(const std::string& what) {
  return {core::ErrorCode::kCorruption, "WAL frame corruption: " + what};
}

[[noreturn]] void Unfilled(core::ShardId shard, core::SequenceId seq, std::size_t size) {
  core::Fatal("WAL frame of shard " + std::to_string(shard) + " seq " + std::to_string(seq) +
              " does not fill its " + std::to_string(size) + "-byte reservation");
}

}  // namespace

std::size_t FrameSize(std::size_t len) noexcept {
  return (kBodyAt + len + kAlign - 1) & ~(kAlign - 1);
}

uint32_t EncodeEntryInto(const core::QueueEntry& entry, core::ShardId shard,
                         std::span<std::byte> out, uint64_t batch_rest) {
  if (out.size() < kMinFrameBytes) Unfilled(shard, entry.seq, out.size());
  std::byte* body = out.data() + kBodyAt;
  body[0] = static_cast<std::byte>(Kind::kEntry);
  body[kTypeAt] = static_cast<std::byte>(entry_payload::TypeOf(entry));
  StoreLE(body + kShardAt, static_cast<uint16_t>(shard));
  StoreLE(body + kFlagsAt, entry.replaces_state ? kReplacesState : uint32_t{0});
  StoreLE(body + kSeqAt, entry.seq);
  StoreLE(body + kBatchRestAt, batch_rest);
  StoreLE(body + kAppendedAt,
          static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                   entry.appended_at.time_since_epoch())
                                   .count()));
  binary::SpanWriter payload(out.subspan(kBodyAt + kHeaderBytes));
  entry_payload::Encode(entry, payload);
  const std::size_t len = kHeaderBytes + payload.written();
  if (payload.overflowed() || FrameSize(len) != out.size()) {
    Unfilled(shard, entry.seq, out.size());
  }
  std::ranges::fill(out.subspan(kBodyAt + len), std::byte{0});
  StoreLE(out.data() + kCommitBytes, Crc32c({body, len}));
  return static_cast<uint32_t>(len);
}

std::size_t EncodeEntry(const core::QueueEntry& entry, core::ShardId shard,
                        std::vector<std::byte>& out) {
  const std::size_t start = out.size();
  const std::size_t size = EntryFrameSize(entry);
  out.resize(start + size);
  const uint32_t len = EncodeEntryInto(entry, shard, std::span(out).subspan(start), size);
  // gen 0 until Commit: CloseBatch and Commit read len from here.
  StoreLE(out.data() + start, CommitWord(len, 0));
  return size;
}

std::size_t EntryFrameSize(const core::QueueEntry& entry) {
  return FrameSize(kHeaderBytes + entry_payload::EncodedSize(entry));
}

void CloseBatch(std::span<std::byte> frames) {
  std::size_t off = 0;
  while (off < frames.size()) {
    std::byte* frame = frames.data() + off;
    const uint32_t len = CommitLen(LoadLE<uint64_t>(frame));
    std::byte* body = frame + kBodyAt;
    StoreLE<uint64_t>(body + kBatchRestAt, frames.size() - off);
    StoreLE(frame + kCommitBytes, Crc32c({body, len}));
    off += FrameSize(len);
  }
}

uint32_t SealCrc(uint32_t body_crc, uint64_t salt, uint64_t word) noexcept {
  std::array<std::byte, 2 * sizeof(uint64_t)> bytes{};
  StoreLE(bytes.data(), salt);
  StoreLE(bytes.data() + sizeof(uint64_t), word);
  return Crc32cExtend(body_crc, bytes);
}

uint64_t EncodePadding(std::size_t span, uint32_t gen, uint64_t salt, std::span<std::byte> dst) {
  const std::span<std::byte> header = dst.subspan(kBodyAt, kHeaderBytes);
  std::ranges::fill(header, std::byte{0});
  header[0] = static_cast<std::byte>(Kind::kPadding);
  const uint64_t word = CommitWord(static_cast<uint32_t>(span - kBodyAt), gen);
  StoreLE(dst.data() + kCommitBytes, SealCrc(Crc32c(header), salt, word));
  return word;
}

View Inspect(uint64_t commit_word, std::span<const std::byte> bytes, uint32_t gen, uint64_t salt,
             bool verify_crc) {
  View view;
  const uint32_t len = CommitLen(commit_word);
  if (CommitGen(commit_word) != gen || len == 0) return view;
  view.state = State::kTorn;
  const std::size_t size = FrameSize(len);
  if (len < kHeaderBytes || size > bytes.size()) return view;

  const std::span<const std::byte> body = bytes.subspan(kBodyAt, len);
  const auto kind = static_cast<Kind>(body[0]);
  if (verify_crc &&
      !CrcHolds(kind, body, salt, commit_word, LoadLE<uint32_t>(bytes.data() + kCommitBytes))) {
    return view;
  }

  view.state = State::kFilled;
  view.header = Header{
      .kind = kind,
      .shard = LoadLE<uint16_t>(body.data() + kShardAt),
      .flags = LoadLE<uint32_t>(body.data() + kFlagsAt),
      .seq = LoadLE<uint64_t>(body.data() + kSeqAt),
      .batch_rest = LoadLE<uint64_t>(body.data() + kBatchRestAt),
      .appended_at_us = LoadLE<int64_t>(body.data() + kAppendedAt),
  };
  view.size = size;
  view.body = body;
  return view;
}

core::Result<core::QueueEntry> DecodeEntry(const View& view) {
  if (view.state != State::kFilled || view.header.kind != Kind::kEntry) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "WAL frame is not a filled entry"});
  }
  if ((view.header.flags & ~kReplacesState) != 0) {
    return std::unexpected(Corrupt("unknown flags " + std::to_string(view.header.flags)));
  }
  const auto type = static_cast<entry_payload::EntryType>(view.body[kTypeAt]);
  const auto appended_us = LoadLE<int64_t>(view.body.data() + kAppendedAt);
  std::span<const std::byte> cursor = view.body.subspan(kHeaderBytes);
  auto payload = entry_payload::Decode(type, cursor);
  if (!payload.has_value()) return std::unexpected(payload.error());
  if (!cursor.empty()) {
    return std::unexpected(Corrupt(std::to_string(cursor.size()) + " bytes after the payload"));
  }
  return core::QueueEntry{
      .seq = view.header.seq,
      .appended_at = core::WallTime(std::chrono::microseconds(appended_us)),
      .payload = std::move(*payload),
      .replaces_state = (view.header.flags & kReplacesState) != 0,
  };
}

}  // namespace abyss::queue::frame
