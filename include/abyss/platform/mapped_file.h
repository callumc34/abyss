#pragma once

#include <cstddef>
#include <cstdint>

#include "abyss/core/result.h"
#include "abyss/platform/fs.h"

namespace abyss::platform::fs {

// A read-write shared mapping of the first `size()` bytes of a file.
// Stores through it land in the file's page cache, so they survive a
// process crash; WriteBack then Fsync makes them survive power loss.
// The file must already be that large: writing past its end is SIGBUS.
class MappedFile {
 public:
  static core::Result<MappedFile> Map(const File& file, std::size_t size);

  MappedFile() noexcept = default;
  ~MappedFile();

  MappedFile(const MappedFile&) = delete;
  MappedFile& operator=(const MappedFile&) = delete;
  MappedFile(MappedFile&& other) noexcept;
  MappedFile& operator=(MappedFile&& other) noexcept;

  std::byte* data() const noexcept { return data_; }
  std::size_t size() const noexcept { return size_; }
  bool valid() const noexcept { return data_ != nullptr; }

  // Hands dirty pages in [offset, offset + length) to the file, so the
  // next Fsync covers them. A no-op where the page cache is unified
  // with the mapping (Linux, macOS); FlushViewOfFile on Windows.
  core::Result<void> WriteBack(std::size_t offset, std::size_t length) const;

  void Unmap() noexcept;

 private:
  MappedFile(std::byte* data, std::size_t size, OsFd mapping) noexcept
      : data_(data), size_(size), mapping_(mapping) {}

  std::byte* data_ = nullptr;
  std::size_t size_ = 0;
  // The Windows file-mapping object; unused elsewhere.
  OsFd mapping_ = kInvalidOsFd;
};

// Writes zeros over [0, size) and sets the file's length to `size`, so
// every block is allocated and written before first use. Allocation
// alone is not enough: Linux unwritten extents and APFS preallocated
// blocks still cost metadata, or stall writers, on their first write.
core::Result<void> ZeroFill(const File& file, std::uint64_t size);

}  // namespace abyss::platform::fs
