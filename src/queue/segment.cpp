#include "segment.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/platform/fs.h"
#include "abyss/queue/wal_entry.h"
#include "binary_io.h"

ABYSS_LOG_COMPONENT("abyss.queue.segment")

namespace abyss::queue {

namespace {

namespace pfs = abyss::platform::fs;

constexpr size_t kReadChunkSize = size_t{256} * 1024;

// ADP-009 frame: u32 body_len, body (u8 type, u64 seq, ...), u32 crc.
constexpr size_t kFrameLenBytes = sizeof(uint32_t);
constexpr size_t kFrameOverhead = kFrameLenBytes + sizeof(uint32_t);
constexpr size_t kFrameSeqOffset = kFrameLenBytes + sizeof(uint8_t);
constexpr size_t kFrameHeaderPeek = kFrameSeqOffset + sizeof(uint64_t);

// Grow-only read buffer, deliberately not value-initialised. The array
// form is what make_unique_for_overwrite needs to skip zeroing.
// NOLINTBEGIN(modernize-avoid-c-arrays)
class ReadScratch {
 public:
  std::byte* Reserve(size_t n) {
    if (n > capacity_) {
      data_ = std::make_unique_for_overwrite<std::byte[]>(n);
      capacity_ = n;
    }
    return data_.get();
  }

 private:
  std::unique_ptr<std::byte[]> data_;
  size_t capacity_ = 0;
};
// NOLINTEND(modernize-avoid-c-arrays)

uint32_t LoadU32LE(const std::byte* p) {
  uint32_t v = 0;
  std::memcpy(&v, p, sizeof(v));
  if constexpr (!binary::kNativeLittleEndian) v = std::byteswap(v);
  return v;
}

uint64_t LoadU64LE(const std::byte* p) {
  uint64_t v = 0;
  std::memcpy(&v, p, sizeof(v));
  if constexpr (!binary::kNativeLittleEndian) v = std::byteswap(v);
  return v;
}

core::Error FrameCorruption(const std::string& path, size_t offset, std::string_view what) {
  return {core::ErrorCode::kCorruption,
          "WAL frame at " + path + ":" + std::to_string(offset) + " " + std::string(what)};
}

core::Result<size_t> PreadExact(const pfs::File& file, std::byte* buf, size_t n, size_t offset,
                                const std::string& path) {
  auto nread = pfs::Pread(file, buf, n, offset);
  if (!nread.has_value()) return std::unexpected(nread.error());
  if (*nread < n) return std::unexpected(FrameCorruption(path, offset, "short read"));
  return *nread;
}

// Frames below `end` were CRC-checked at Open or written by this process,
// so the walk reads only each length and seq.
core::Result<size_t> SkipToSeq(const pfs::File& file, const std::string& path, size_t offset,
                               core::SequenceId offset_seq, core::SequenceId target, size_t end,
                               ReadScratch& scratch) {
  core::SequenceId expected = offset_seq;
  while (offset < end) {
    const size_t n = std::min(kReadChunkSize, end - offset);
    std::byte* buf = scratch.Reserve(n);
    if (auto r = PreadExact(file, buf, n, offset, path); !r.has_value()) {
      return std::unexpected(r.error());
    }
    size_t pos = 0;
    while (pos + kFrameHeaderPeek <= n) {
      const core::SequenceId seq = LoadU64LE(buf + pos + kFrameSeqOffset);
      if (seq != expected) {
        return std::unexpected(FrameCorruption(path, offset + pos, "has an out-of-sequence seq"));
      }
      if (seq == target) return offset + pos;
      pos += kFrameOverhead + LoadU32LE(buf + pos);
      ++expected;
    }
    if (pos == 0) return std::unexpected(FrameCorruption(path, offset, "header is truncated"));
    offset += pos;
  }
  return std::unexpected(FrameCorruption(path, offset, "walk passed the write offset"));
}

core::Result<Segment::ReadResult> DecodeFrames(const pfs::File& file, const std::string& path,
                                               uint8_t format_minor, size_t file_offset, size_t end,
                                               size_t max_count, ReadScratch& scratch) {
  Segment::ReadResult result;  // NOLINT(misc-const-correctness)
  size_t cursor = file_offset;
  size_t want = kReadChunkSize;
  while (result.entries.size() < max_count && cursor < end) {
    const size_t remaining = end - cursor;
    const size_t to_read = std::min(remaining, want);
    std::byte* buf = scratch.Reserve(to_read);
    if (auto r = PreadExact(file, buf, to_read, cursor, path); !r.has_value()) {
      return std::unexpected(r.error());
    }

    std::span<const std::byte> view(buf, to_read);
    size_t consumed = 0;
    while (!view.empty() && result.entries.size() < max_count) {
      auto decoded = DecodeWalEntry(view, format_minor);
      if (!decoded.has_value()) {
        // A frame that fits the view yet fails to decode is corrupt on disk;
        // one that does not fit just needs a larger read.
        if (view.size() >= kFrameLenBytes &&
            kFrameOverhead + LoadU32LE(view.data()) <= view.size()) {
          return std::unexpected(
              FrameCorruption(path, cursor + consumed, decoded.error().message()));
        }
        break;
      }
      result.entries.push_back(std::move(decoded->entry));
      view = view.subspan(decoded->bytes_consumed);
      consumed += decoded->bytes_consumed;
    }

    if (consumed == 0) {
      if (to_read < kFrameLenBytes) {
        return std::unexpected(FrameCorruption(path, cursor, "header is truncated"));
      }
      const size_t frame = kFrameOverhead + LoadU32LE(buf);
      if (frame > remaining) {
        return std::unexpected(FrameCorruption(path, cursor, "overruns the write offset"));
      }
      want = frame;
      continue;
    }
    cursor += consumed;
    want = kReadChunkSize;
  }
  result.next_offset = cursor;
  return result;
}

}  // namespace

SegmentIndex::SegmentIndex(size_t max_bytes) : points_((max_bytes / kIntervalBytes) + 1) {}

void SegmentIndex::MaybeRecord(core::SequenceId seq, size_t offset) {
  if (offset < last_offset_ + kIntervalBytes) return;
  const size_t count = count_.load(std::memory_order_relaxed);
  if (count == points_.size()) return;
  points_[count] = Point{.seq = seq, .offset = offset};
  count_.store(count + 1, std::memory_order_release);
  last_offset_ = offset;
}

void SegmentIndex::TrimTo(size_t end_offset) {
  size_t count = count_.load(std::memory_order_relaxed);
  while (count > 0 && points_[count - 1].offset >= end_offset) --count;
  count_.store(count, std::memory_order_release);
  last_offset_ = count > 0 ? points_[count - 1].offset : kSegmentHeaderSize;
}

std::optional<SegmentIndex::Point> SegmentIndex::Floor(core::SequenceId seq) const {
  const size_t count = count_.load(std::memory_order_acquire);
  const std::span<const Point> points(points_.data(), count);
  const auto it = std::ranges::upper_bound(points, seq, std::ranges::less{},
                                           [](const Point& p) { return p.seq; });
  if (it == points.begin()) return std::nullopt;
  return *std::prev(it);
}

Segment::Segment(std::string path, SegmentHeader header, size_t max_size, pfs::File file,
                 size_t write_offset, core::SequenceId next_seq, size_t entry_count,
                 std::unique_ptr<SegmentIndex> index)
    : path_(std::move(path)),
      header_(header),
      max_size_(max_size),
      file_(std::move(file)),
      write_offset_(write_offset),
      next_seq_(next_seq),
      entry_count_(entry_count),
      index_(std::move(index)) {}

Segment::Segment(Segment&& other) noexcept
    : path_(std::move(other.path_)),
      header_(other.header_),
      max_size_(other.max_size_),
      file_(std::move(other.file_)),
      write_offset_(other.write_offset_.load(std::memory_order_relaxed)),
      next_seq_(other.next_seq_.load(std::memory_order_relaxed)),
      entry_count_(other.entry_count_),
      sealed_(other.sealed_),
      index_(std::move(other.index_)) {}

Segment& Segment::operator=(Segment&& other) noexcept {
  if (this != &other) {
    path_ = std::move(other.path_);
    header_ = other.header_;
    max_size_ = other.max_size_;
    file_ = std::move(other.file_);
    write_offset_.store(other.write_offset_.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
    next_seq_.store(other.next_seq_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    entry_count_ = other.entry_count_;
    sealed_ = other.sealed_;
    index_ = std::move(other.index_);
  }
  return *this;
}

core::Result<Segment> Segment::Create(const std::string& path, SegmentHeader header,
                                      size_t max_size) {
  auto file = pfs::Open(
      std::filesystem::path(path),
      pfs::OpenOptions{
          .mode = pfs::OpenMode::kReadWrite, .create = true, .exclusive = true, .truncate = false});
  if (!file.has_value()) return std::unexpected(file.error());

  std::vector<std::byte> buf;
  buf.reserve(kSegmentHeaderSize);
  EncodeSegmentHeader(header, buf);

  auto wr = pfs::Pwrite(*file, buf.data(), buf.size(), 0);
  if (!wr.has_value()) {
    file->Close();
    (void)pfs::Unlink(std::filesystem::path(path));  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(wr.error());
  }

  // Persist the header bytes before the segment becomes appendable. Combined
  // with the caller's directory fsync, the header + name are durable before
  // the first entry's DurabilityFuture can resolve (QUEUE-1/NET-6).
  if (auto sync = pfs::Fsync(*file); !sync.has_value()) {
    file->Close();
    (void)pfs::Unlink(std::filesystem::path(path));  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(sync.error());
  }

  return Segment(path, header, max_size, std::move(*file), kSegmentHeaderSize, header.base_seq, 0,
                 std::make_unique<SegmentIndex>(max_size));
}

core::Result<Segment> Segment::Open(const std::string& path, size_t max_size) {
  auto file =
      pfs::Open(std::filesystem::path(path), pfs::OpenOptions{.mode = pfs::OpenMode::kReadWrite});
  if (!file.has_value()) return std::unexpected(file.error());

  std::vector<std::byte> hdr_buf(kSegmentHeaderSize);
  auto hdr_read = pfs::Pread(*file, hdr_buf.data(), kSegmentHeaderSize, 0);
  if (!hdr_read.has_value()) return std::unexpected(hdr_read.error());
  if (*hdr_read < kSegmentHeaderSize) {
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption, "segment too small for header"});
  }

  auto header = DecodeSegmentHeader(hdr_buf);
  if (!header.has_value()) return std::unexpected(header.error());

  auto file_size = pfs::FileSize(*file);
  if (!file_size.has_value()) return std::unexpected(file_size.error());

  const auto file_size_bytes = static_cast<size_t>(*file_size);
  const size_t data_size =
      file_size_bytes > kSegmentHeaderSize ? file_size_bytes - kSegmentHeaderSize : 0;
  size_t write_offset = kSegmentHeaderSize;
  core::SequenceId next_seq = header->base_seq;
  size_t entry_count = 0;
  // Sized to the file too, in case it predates a smaller segment_size_bytes.
  auto index = std::make_unique<SegmentIndex>(std::max(max_size, file_size_bytes));

  if (data_size > 0) {
    // Overwritten by the read below; zeroing a whole segment is wasted work.
    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    auto data = std::make_unique_for_overwrite<std::byte[]>(data_size);
    auto data_read = pfs::Pread(*file, data.get(), data_size, kSegmentHeaderSize);
    if (!data_read.has_value()) return std::unexpected(data_read.error());

    // Scan the body. Advance the "durable" watermark only at batch-closing.
    std::span<const std::byte> view(data.get(), *data_read);
    size_t cursor_offset = 0;
    size_t durable_offset = 0;
    core::SequenceId pending_next_seq = 0;
    size_t pending_entry_count = 0;

    while (!view.empty()) {
      WalDecodeFailure failure = WalDecodeFailure::kNone;
      auto decoded = DecodeWalEntry(view, header->format_minor, failure);
      if (!decoded.has_value()) {
        if (failure == WalDecodeFailure::kCorruptFrame) {
          // A CRC-valid frame whose structure could not be decoded is genuine
          // corruption of durably-acked data. Fail-stop: truncating here would
          // silently discard acked data (invariants 1/2). Decision 6.
          metrics::Registry::Instance()
              .Counter(metrics::names::kWalDecodeCorruptionTotal)
              .Increment();
          ABYSS_LOG_CRITICAL("WAL decode corruption", {"path", std::string_view{path}},
                             {"shard", static_cast<int64_t>(header->shard_id)},
                             {"base_seq", static_cast<uint64_t>(header->base_seq)},
                             {"offset", static_cast<uint64_t>(kSegmentHeaderSize + cursor_offset)},
                             {"err", std::string_view{decoded.error().message()}});
          return std::unexpected(
              core::Error{core::ErrorCode::kCorruption,
                          "WAL decode corruption in " + path + ": " + decoded.error().message()});
        }
        // Torn tail (incomplete / failed CRC): truncate as today.
        break;
      }
      index->MaybeRecord(decoded->entry.seq, kSegmentHeaderSize + cursor_offset);
      cursor_offset += decoded->bytes_consumed;
      pending_next_seq = decoded->entry.seq + 1;
      ++pending_entry_count;

      if (decoded->entry.seq == decoded->batch_last_seq) {
        durable_offset = cursor_offset;
        next_seq = pending_next_seq;
        entry_count = pending_entry_count;
      }
      view = view.subspan(decoded->bytes_consumed);
    }

    write_offset = kSegmentHeaderSize + durable_offset;
  }
  index->TrimTo(write_offset);

  if (write_offset < file_size_bytes) {
    ABYSS_LOG_WARN("torn tail truncated at recovery", {"path", std::string_view{path}},
                   {"shard", static_cast<int64_t>(header->shard_id)},
                   {"base_seq", static_cast<uint64_t>(header->base_seq)},
                   {"file_size", static_cast<uint64_t>(file_size_bytes)},
                   {"durable_offset", static_cast<uint64_t>(write_offset)},
                   {"bytes_discarded", static_cast<uint64_t>(file_size_bytes - write_offset)});
    if (auto r = pfs::Ftruncate(*file, write_offset); !r.has_value()) {
      return std::unexpected(r.error());
    }
  }

  return Segment(path, *header, max_size, std::move(*file), write_offset, next_seq, entry_count,
                 std::move(index));
}

// NOLINTNEXTLINE(readability-make-member-function-const)
core::Result<size_t> Segment::Append(const core::QueueEntry& entry,
                                     core::SequenceId batch_last_seq) {
  if (batch_last_seq < entry.seq) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "batch_last_seq < entry.seq"});
  }

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, batch_last_seq, buf);
  return AppendEncoded(buf, entry.seq);
}

// NOLINTNEXTLINE(readability-make-member-function-const)
core::Result<size_t> Segment::AppendEncoded(std::span<const std::byte> bytes,
                                            core::SequenceId entry_seq) {
  if (sealed_) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "append on sealed segment"});
  }
  const core::SequenceId current_next_seq = next_seq_.load(std::memory_order_relaxed);
  if (entry_seq != current_next_seq) {
    return std::unexpected(core::Error{
        core::ErrorCode::kInvalidArgument,
        "expected seq " + std::to_string(current_next_seq) + ", got " + std::to_string(entry_seq)});
  }
  const size_t current_offset = write_offset_.load(std::memory_order_relaxed);
  if (current_offset + bytes.size() > max_size_) {
    return std::unexpected(core::Error{core::ErrorCode::kResourceExhausted, "segment full"});
  }

  if (auto r = pfs::Pwrite(file_, bytes.data(), bytes.size(), current_offset); !r.has_value()) {
    return std::unexpected(r.error());
  }

  index_->MaybeRecord(entry_seq, current_offset);
  write_offset_.store(current_offset + bytes.size(), std::memory_order_release);
  next_seq_.store(entry_seq + 1, std::memory_order_release);
  entry_count_++;
  return bytes.size();
}

// NOLINTNEXTLINE(readability-make-member-function-const)
core::Result<size_t> Segment::AppendEncodedBatch(std::span<const std::byte> bytes,
                                                 std::span<const size_t> sizes,
                                                 core::SequenceId first_seq) {
  size_t sized = 0;
  for (const size_t size : sizes) sized += size;
  if (sizes.empty() || sized != bytes.size()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "batch sizes do not cover its bytes"});
  }
  if (sealed_) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "append on sealed segment"});
  }
  const core::SequenceId current_next_seq = next_seq_.load(std::memory_order_relaxed);
  if (first_seq != current_next_seq) {
    return std::unexpected(core::Error{
        core::ErrorCode::kInvalidArgument,
        "expected seq " + std::to_string(current_next_seq) + ", got " + std::to_string(first_seq)});
  }
  const size_t start = write_offset_.load(std::memory_order_relaxed);
  if (start + bytes.size() > max_size_) {
    return std::unexpected(core::Error{core::ErrorCode::kResourceExhausted, "segment full"});
  }

  if (auto r = pfs::Pwrite(file_, bytes.data(), bytes.size(), start); !r.has_value()) {
    return std::unexpected(r.error());
  }

  size_t offset = start;
  for (size_t i = 0; i < sizes.size(); ++i) {
    index_->MaybeRecord(first_seq + i, offset);
    offset += sizes[i];
  }
  write_offset_.store(start + bytes.size(), std::memory_order_release);
  next_seq_.store(first_seq + sizes.size(), std::memory_order_release);
  entry_count_ += sizes.size();
  return bytes.size();
}

core::Result<void> Segment::Fsync() const {
  if (!file_.valid()) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "fsync on closed segment"});
  }
  return pfs::Fsync(file_, pfs::SyncMode::kDurableData);
}

core::Result<void> Segment::Seal() {
  if (sealed_) return {};
  if (!file_.valid()) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "seal on closed segment"});
  }
  const size_t offset = write_offset_.load(std::memory_order_relaxed);
  if (auto r = pfs::Ftruncate(file_, offset); !r.has_value()) return std::unexpected(r.error());
  if (auto r = pfs::Fsync(file_, pfs::SyncMode::kDurableData); !r.has_value()) {
    return std::unexpected(r.error());
  }
  sealed_ = true;
  return {};
}

core::Result<Segment::ReadResult> Segment::ReadEntries(size_t file_offset, size_t max_count) const {
  const size_t end = write_offset_.load(std::memory_order_acquire);
  if (file_offset < kSegmentHeaderSize || file_offset > end) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "file_offset out of range"});
  }
  ReadScratch scratch;
  return DecodeFrames(file_, path_, header_.format_minor, file_offset, end, max_count, scratch);
}

core::Result<Segment::ReadResult> Segment::ReadEntriesFrom(core::SequenceId seq,
                                                           size_t max_count) const {
  const core::SequenceId end_seq = next_seq_.load(std::memory_order_acquire);
  const size_t end = write_offset_.load(std::memory_order_acquire);
  if (seq >= end_seq) {
    return ReadResult{.entries = {}, .next_offset = end};
  }

  ReadScratch scratch;
  size_t from = kSegmentHeaderSize;
  if (seq > header_.base_seq) {
    core::SequenceId from_seq = header_.base_seq;
    if (const auto point = index_->Floor(seq); point.has_value()) {
      from = point->offset;
      from_seq = point->seq;
    }
    auto found = SkipToSeq(file_, path_, from, from_seq, seq, end, scratch);
    if (!found.has_value()) return std::unexpected(found.error());
    from = *found;
  }
  return DecodeFrames(file_, path_, header_.format_minor, from, end, max_count, scratch);
}

}  // namespace abyss::queue
