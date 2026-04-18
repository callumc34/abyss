#include "segment.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "abyss/queue/wal_entry.h"
#include "binary_io.h"

namespace abyss::queue {

namespace {

constexpr size_t kReadChunkSize = size_t{256} * 1024;

core::Error IoError(const char* what) {
  return {core::ErrorCode::kInternal, std::string(what) + ": " + std::strerror(errno)};
}

core::Result<void> FullPwrite(int fd, const void* buf, size_t count, off_t offset) {
  const auto* p = static_cast<const uint8_t*>(buf);
  size_t remaining = count;
  while (remaining > 0) {  // NOLINT(bugprone-infinite-loop)
    auto n = ::pwrite(fd, p, remaining, offset);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(IoError("pwrite"));
    }
    p += n;
    offset += n;
    remaining -= static_cast<size_t>(n);
  }
  return {};
}

core::Result<size_t> FullPread(int fd, void* buf, size_t count, off_t offset) {
  auto* p = static_cast<uint8_t*>(buf);
  size_t total = 0;
  while (total < count) {
    auto n = ::pread(fd, p + total, count - total, offset + static_cast<off_t>(total));
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(IoError("pread"));
    }
    if (n == 0) break;
    total += static_cast<size_t>(n);
  }
  return total;
}

}  // namespace

Segment::Segment(std::string path, SegmentHeader header, size_t max_size, int fd,
                 size_t write_offset, core::SequenceId next_seq, size_t entry_count)
    : path_(std::move(path)),
      header_(header),
      max_size_(max_size),
      fd_(fd),
      write_offset_(write_offset),
      next_seq_(next_seq),
      entry_count_(entry_count) {}

Segment::~Segment() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

Segment::Segment(Segment&& other) noexcept
    : path_(std::move(other.path_)),
      header_(other.header_),
      max_size_(other.max_size_),
      fd_(other.fd_),
      write_offset_(other.write_offset_),
      next_seq_(other.next_seq_),
      entry_count_(other.entry_count_) {
  other.fd_ = -1;
}

Segment& Segment::operator=(Segment&& other) noexcept {
  if (this != &other) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    path_ = std::move(other.path_);
    header_ = other.header_;
    max_size_ = other.max_size_;
    fd_ = other.fd_;
    write_offset_ = other.write_offset_;
    next_seq_ = other.next_seq_;
    entry_count_ = other.entry_count_;
    other.fd_ = -1;
  }
  return *this;
}

core::Result<Segment> Segment::Create(const std::string& path, SegmentHeader header,
                                      size_t max_size) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
  if (fd < 0) {
    return std::unexpected(IoError("open(create)"));
  }

  std::vector<std::byte> buf;
  buf.reserve(kSegmentHeaderSize);
  EncodeSegmentHeader(header, buf);

  auto wr = FullPwrite(fd, buf.data(), buf.size(), 0);
  if (!wr.has_value()) {
    ::close(fd);
    ::unlink(path.c_str());
    return std::unexpected(wr.error());
  }

  return Segment(path, header, max_size, fd, kSegmentHeaderSize, header.base_seq, 0);
}

core::Result<Segment> Segment::Open(const std::string& path, size_t max_size) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  int fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) {
    return std::unexpected(IoError("open"));
  }

  std::vector<std::byte> hdr_buf(kSegmentHeaderSize);
  auto hdr_read = FullPread(fd, hdr_buf.data(), kSegmentHeaderSize, 0);
  if (!hdr_read.has_value()) {
    ::close(fd);
    return std::unexpected(hdr_read.error());
  }
  if (*hdr_read < kSegmentHeaderSize) {
    ::close(fd);
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption, "segment too small for header"});
  }

  auto header = DecodeSegmentHeader(hdr_buf);
  if (!header.has_value()) {
    ::close(fd);
    return std::unexpected(header.error());
  }

  struct stat st{};
  if (::fstat(fd, &st) < 0) {
    ::close(fd);
    return std::unexpected(IoError("fstat"));
  }
  const auto file_size = static_cast<size_t>(st.st_size);

  const size_t data_size = file_size > kSegmentHeaderSize ? file_size - kSegmentHeaderSize : 0;
  size_t write_offset = kSegmentHeaderSize;
  core::SequenceId next_seq = header->base_seq;
  size_t entry_count = 0;

  if (data_size > 0) {
    std::vector<std::byte> data(data_size);
    auto data_read = FullPread(fd, data.data(), data_size, static_cast<off_t>(kSegmentHeaderSize));
    if (!data_read.has_value()) {
      ::close(fd);
      return std::unexpected(data_read.error());
    }

    std::span<const std::byte> view(data.data(), *data_read);
    while (!view.empty()) {
      auto decoded = DecodeWalEntry(view);
      if (!decoded.has_value()) break;
      next_seq = decoded->entry.seq + 1;
      entry_count++;
      view = view.subspan(decoded->bytes_consumed);
    }
    write_offset = kSegmentHeaderSize + (*data_read - view.size());
  }

  if (write_offset < file_size) {
    if (::ftruncate(fd, static_cast<off_t>(write_offset)) < 0) {
      ::close(fd);
      return std::unexpected(IoError("ftruncate"));
    }
  }

  return Segment(path, *header, max_size, fd, write_offset, next_seq, entry_count);
}

core::Result<size_t> Segment::Append(const core::QueueEntry& entry) {
  if (entry.seq != next_seq_) {
    return std::unexpected(core::Error{
        core::ErrorCode::kInvalidArgument,
        "expected seq " + std::to_string(next_seq_) + ", got " + std::to_string(entry.seq)});
  }

  std::vector<std::byte> buf;
  size_t encoded_size = EncodeWalEntry(entry, buf);

  if (write_offset_ + encoded_size > max_size_) {
    return std::unexpected(core::Error{core::ErrorCode::kResourceExhausted, "segment full"});
  }

  auto wr = FullPwrite(fd_, buf.data(), buf.size(), static_cast<off_t>(write_offset_));
  if (!wr.has_value()) {
    return std::unexpected(wr.error());
  }

  write_offset_ += encoded_size;
  next_seq_ = entry.seq + 1;
  entry_count_++;
  return encoded_size;
}

core::Result<Segment::ReadResult> Segment::ReadEntries(size_t file_offset, size_t max_count) const {
  if (file_offset < kSegmentHeaderSize || file_offset > write_offset_) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "file_offset out of range"});
  }

  ReadResult result;  // NOLINT(misc-const-correctness)
  result.next_offset = file_offset;

  if (file_offset == write_offset_ || max_count == 0) {
    return result;
  }

  size_t cursor = file_offset;

  while (result.entries.size() < max_count && cursor < write_offset_) {
    const size_t remaining = write_offset_ - cursor;
    const size_t to_read = std::min(remaining, kReadChunkSize);

    std::vector<std::byte> buf(to_read);
    auto nread = FullPread(fd_, buf.data(), to_read, static_cast<off_t>(cursor));
    if (!nread.has_value()) return std::unexpected(nread.error());
    if (*nread == 0) break;
    buf.resize(*nread);

    std::span<const std::byte> view(buf);
    size_t chunk_consumed = 0;

    while (!view.empty() && result.entries.size() < max_count) {
      auto decoded = DecodeWalEntry(view);
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
      auto entry_read = FullPread(fd_, entry_buf.data(), entry_size, static_cast<off_t>(cursor));
      if (!entry_read.has_value() || *entry_read < entry_size) break;

      auto decoded = DecodeWalEntry(std::span<const std::byte>(entry_buf));
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
  if (seq >= next_seq_) {
    return ReadResult{.entries = {}, .next_offset = write_offset_};
  }
  if (seq <= header_.base_seq) {
    return ReadEntries(kSegmentHeaderSize, max_count);
  }

  const size_t skip_count = seq - header_.base_seq;
  size_t skipped = 0;
  size_t offset = kSegmentHeaderSize;

  while (skipped < skip_count && offset < write_offset_) {
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
