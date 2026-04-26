#include "segment.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/log/log.h"
#include "abyss/platform/fs.h"
#include "abyss/queue/wal_entry.h"
#include "binary_io.h"

namespace abyss::queue {

namespace {

namespace pfs = abyss::platform::fs;

const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.queue.segment");
  return l;
}

constexpr size_t kReadChunkSize = size_t{256} * 1024;

}  // namespace

Segment::Segment(std::string path, SegmentHeader header, size_t max_size, pfs::File file,
                 size_t write_offset, core::SequenceId next_seq, size_t entry_count)
    : path_(std::move(path)),
      header_(header),
      max_size_(max_size),
      file_(std::move(file)),
      write_offset_(write_offset),
      next_seq_(next_seq),
      entry_count_(entry_count) {}

Segment::Segment(Segment&& other) noexcept
    : path_(std::move(other.path_)),
      header_(other.header_),
      max_size_(other.max_size_),
      file_(std::move(other.file_)),
      write_offset_(other.write_offset_.load(std::memory_order_relaxed)),
      next_seq_(other.next_seq_.load(std::memory_order_relaxed)),
      entry_count_(other.entry_count_),
      sealed_(other.sealed_) {}

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

  return Segment(path, header, max_size, std::move(*file), kSegmentHeaderSize, header.base_seq, 0);
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

  if (data_size > 0) {
    std::vector<std::byte> data(data_size);
    auto data_read = pfs::Pread(*file, data.data(), data_size, kSegmentHeaderSize);
    if (!data_read.has_value()) return std::unexpected(data_read.error());

    // Scan the body. Advance the "durable" watermark only at batch-closing.
    std::span<const std::byte> view(data.data(), *data_read);
    size_t cursor_offset = 0;
    size_t durable_offset = 0;
    core::SequenceId pending_next_seq = 0;
    size_t pending_entry_count = 0;

    while (!view.empty()) {
      auto decoded = DecodeWalEntry(view, header->format_minor);
      if (!decoded.has_value()) break;
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

  if (write_offset < file_size_bytes) {
    ABYSS_LOG_WARN(Log(), "torn tail truncated at recovery", {"path", std::string_view{path}},
                   {"shard", static_cast<int64_t>(header->shard_id)},
                   {"base_seq", static_cast<uint64_t>(header->base_seq)},
                   {"file_size", static_cast<uint64_t>(file_size_bytes)},
                   {"durable_offset", static_cast<uint64_t>(write_offset)},
                   {"bytes_discarded", static_cast<uint64_t>(file_size_bytes - write_offset)});
    if (auto r = pfs::Ftruncate(*file, write_offset); !r.has_value()) {
      return std::unexpected(r.error());
    }
  }

  return Segment(path, *header, max_size, std::move(*file), write_offset, next_seq, entry_count);
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

  write_offset_.store(current_offset + bytes.size(), std::memory_order_release);
  next_seq_.store(entry_seq + 1, std::memory_order_release);
  entry_count_++;
  return bytes.size();
}

core::Result<void> Segment::Fsync() const {
  if (!file_.valid()) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "fsync on closed segment"});
  }
  return pfs::Fsync(file_);
}

core::Result<void> Segment::Seal() {
  if (sealed_) return {};
  if (!file_.valid()) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "seal on closed segment"});
  }
  const size_t offset = write_offset_.load(std::memory_order_relaxed);
  if (auto r = pfs::Ftruncate(file_, offset); !r.has_value()) return std::unexpected(r.error());
  if (auto r = pfs::Fsync(file_); !r.has_value()) return std::unexpected(r.error());
  sealed_ = true;
  return {};
}

core::Result<Segment::ReadResult> Segment::ReadEntries(size_t file_offset, size_t max_count) const {
  const size_t end = write_offset_.load(std::memory_order_acquire);
  if (file_offset < kSegmentHeaderSize || file_offset > end) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "file_offset out of range"});
  }

  ReadResult result;  // NOLINT(misc-const-correctness)
  result.next_offset = file_offset;

  if (file_offset == end || max_count == 0) {
    return result;
  }

  size_t cursor = file_offset;

  while (result.entries.size() < max_count && cursor < end) {
    const size_t remaining = end - cursor;
    const size_t to_read = std::min(remaining, kReadChunkSize);

    std::vector<std::byte> buf(to_read);
    auto nread = pfs::Pread(file_, buf.data(), to_read, cursor);
    if (!nread.has_value()) return std::unexpected(nread.error());
    if (*nread == 0) break;
    buf.resize(*nread);

    std::span<const std::byte> view(buf);
    size_t chunk_consumed = 0;

    while (!view.empty() && result.entries.size() < max_count) {
      auto decoded = DecodeWalEntry(view, header_.format_minor);
      if (!decoded.has_value()) break;
      result.entries.push_back(std::move(decoded->entry));
      view = view.subspan(decoded->bytes_consumed);
      chunk_consumed += decoded->bytes_consumed;
    }

    if (chunk_consumed == 0 && to_read < remaining) {
      // Entry larger than chunk. Read its exact size.
      if (remaining < sizeof(uint32_t)) break;

      uint32_t body_len = 0;
      std::memcpy(&body_len, buf.data(), sizeof(body_len));
      if constexpr (!binary::kNativeLittleEndian) {
        body_len = std::byteswap(body_len);
      }

      const size_t entry_size = sizeof(uint32_t) + body_len + sizeof(uint32_t);
      if (entry_size > remaining) break;

      std::vector<std::byte> entry_buf(entry_size);
      auto entry_read = pfs::Pread(file_, entry_buf.data(), entry_size, cursor);
      if (!entry_read.has_value() || *entry_read < entry_size) break;

      auto decoded = DecodeWalEntry(std::span<const std::byte>(entry_buf), header_.format_minor);
      if (!decoded.has_value()) break;

      result.entries.push_back(std::move(decoded->entry));
      chunk_consumed = decoded->bytes_consumed;
    }

    cursor += chunk_consumed;
    if (chunk_consumed == 0) break;
  }

  result.next_offset = cursor;
  return result;
}

core::Result<Segment::ReadResult> Segment::ReadEntriesFrom(core::SequenceId seq,
                                                           size_t max_count) const {
  const core::SequenceId end_seq = next_seq_.load(std::memory_order_acquire);
  const size_t end_offset = write_offset_.load(std::memory_order_acquire);

  if (seq >= end_seq) {
    return ReadResult{.entries = {}, .next_offset = end_offset};
  }
  if (seq <= header_.base_seq) {
    return ReadEntries(kSegmentHeaderSize, max_count);
  }

  const size_t skip_count = seq - header_.base_seq;
  size_t skipped = 0;
  size_t offset = kSegmentHeaderSize;

  while (skipped < skip_count && offset < end_offset) {
    const size_t batch = std::min(skip_count - skipped, static_cast<size_t>(1024));
    auto read = ReadEntries(offset, batch);
    if (!read.has_value()) return std::unexpected(read.error());
    if (read->entries.empty()) break;
    skipped += read->entries.size();
    offset = read->next_offset;
  }

  return ReadEntries(offset, max_count);
}

}  // namespace abyss::queue
