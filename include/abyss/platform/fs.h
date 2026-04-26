#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "abyss/core/result.h"
#include "abyss/platform/types.h"

namespace abyss::platform::fs {

enum class OpenMode : std::uint8_t { kRead, kWrite, kReadWrite };

struct OpenOptions {
  OpenMode mode = OpenMode::kRead;
  bool create = false;
  bool exclusive = false;
  bool truncate = false;
};

// RAII wrapper for an open file handle.
class File {
 public:
  File() noexcept = default;
  explicit File(OsFd fd) noexcept : fd_(fd) {}
  ~File() noexcept;

  File(const File&) = delete;
  File& operator=(const File&) = delete;

  File(File&& other) noexcept;
  File& operator=(File&& other) noexcept;

  bool valid() const noexcept;
  OsFd get() const noexcept { return fd_; }
  OsFd Release() noexcept;
  void Close() noexcept;

 private:
  OsFd fd_ = kInvalidOsFd;
};

// Open a file at `path`. Wide-char on Windows; UTF-8 octet stream on POSIX.
core::Result<File> Open(const std::filesystem::path& path, OpenOptions opts);

// Positional write. Loops past partial writes and EINTR. Thread-safe across
// concurrent callers on the same handle: uses OVERLAPPED on Windows so the
// kernel file pointer is not consulted.
core::Result<void> Pwrite(const File& f, const void* buf, std::size_t count, std::uint64_t offset);

// Positional read. Returns the number of bytes actually read (0 at EOF).
// Loops past EINTR up to `count`. Same thread-safety property as Pwrite.
core::Result<std::size_t> Pread(const File& f, void* buf, std::size_t count, std::uint64_t offset);

// Sequential read at the file's current position. Single syscall.
core::Result<std::size_t> Read(const File& f, void* buf, std::size_t count);

// Sequential write loops past partial writes and EINTR.
core::Result<void> WriteAll(const File& f, const void* buf, std::size_t count);

core::Result<void> Ftruncate(const File& f, std::uint64_t size);
core::Result<void> Fsync(const File& f);
core::Result<std::uint64_t> FileSize(const File& f);

core::Result<void> Unlink(const std::filesystem::path& path);
core::Result<void> Rename(const std::filesystem::path& from, const std::filesystem::path& to);

// Best-effort directory fsync. Returns success on filesystems that don't
// support directory fsync.
core::Result<void> FsyncDir(const std::filesystem::path& dir);

// Current process id, widened to bridge pid_t (POSIX) and DWORD (Windows).
std::uint64_t ProcessId() noexcept;

}  // namespace abyss::platform::fs
