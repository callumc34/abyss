#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/segment_header.h"

namespace abyss::queue {

// Single WAL segment file on disk.
class Segment {
 public:
  static core::Result<Segment> Create(const std::string& path, SegmentHeader header,
                                      size_t max_size);
  static core::Result<Segment> Open(const std::string& path, size_t max_size);

  ~Segment();

  Segment(Segment&& other) noexcept;
  Segment& operator=(Segment&& other) noexcept;
  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;

  // Serialise and write an entry to the fd via pwrite. Does NOT fsync.
  // Returns the number of bytes written on success.
  core::Result<size_t> Append(const core::QueueEntry& entry);

  struct ReadResult {
    std::vector<core::QueueEntry> entries;
    size_t next_offset = 0;
  };

  core::Result<ReadResult> ReadEntries(size_t file_offset, size_t max_count) const;
  core::Result<ReadResult> ReadEntriesFrom(core::SequenceId seq, size_t max_count) const;

  const std::string& path() const { return path_; }
  const SegmentHeader& header() const { return header_; }
  core::SequenceId base_seq() const { return header_.base_seq; }
  core::SequenceId next_seq() const { return next_seq_; }
  size_t write_offset() const { return write_offset_; }
  size_t max_size() const { return max_size_; }
  size_t entry_count() const { return entry_count_; }
  size_t SpaceRemaining() const {
    return max_size_ > write_offset_ ? max_size_ - write_offset_ : 0;
  }
  int fd() const { return fd_; }

 private:
  Segment(std::string path, SegmentHeader header, size_t max_size, int fd, size_t write_offset,
          core::SequenceId next_seq, size_t entry_count);

  std::string path_;
  SegmentHeader header_;
  size_t max_size_;
  int fd_ = -1;
  size_t write_offset_ = 0;
  core::SequenceId next_seq_ = 0;
  size_t entry_count_ = 0;
};

}  // namespace abyss::queue
