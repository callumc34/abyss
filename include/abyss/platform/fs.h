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

// Durability intent for Fsync. kDurable guarantees the bytes are on stable
// media after a successful return on every supported OS (macOS F_FULLFSYNC,
// Linux fsync, Windows FlushFileBuffers). kDurableData is as strong for the
// data and the metadata needed to read it back, but skips unrelated metadata
// such as mtime (Linux fdatasync; identical to kDurable elsewhere).
// kFlushOnly is the legacy drive-cache flush, for callers that explicitly do
// not need power-loss durability.
enum class SyncMode : std::uint8_t { kDurable, kDurableData, kFlushOnly };

// The stable-media barrier kDurable resolves to on this platform.
enum class FsyncBackend : std::uint8_t { kFullFsync, kFsync, kFlushFileBuffers };

// fsync, honouring `mode`. The default is kDurable so any caller that does not
// opt out gets stable-media semantics (fails safe, not fast).
core::Result<void> Fsync(const File& f, SyncMode mode = SyncMode::kDurable);

core::Result<std::uint64_t> FileSize(const File& f);

core::Result<void> Unlink(const std::filesystem::path& path);
core::Result<void> Rename(const std::filesystem::path& from, const std::filesystem::path& to);

// Outcome of FsyncDir: kSynced means the directory entry is on stable media;
// kUnsupported means the underlying volume cannot fsync directories (FAT/exFAT,
// some network shares). It is NOT an I/O error -- a genuine error still returns
// std::unexpected. Durability-critical callers MUST treat kUnsupported as a
// hard error (invariant 5: surface pressure, never silently degrade).
enum class DirSyncOutcome : std::uint8_t { kSynced, kUnsupported };

// Directory fsync, distinguishing "synced" from "the volume cannot do it".
core::Result<DirSyncOutcome> FsyncDir(const std::filesystem::path& dir);

// The durability posture of a data directory's volume.
struct DurabilityCapability {
  bool dir_sync_supported = false;
  FsyncBackend backend = FsyncBackend::kFsync;
};

// One-shot probe of `data_dir`'s durability capability, intended to run once at
// startup so operators can observe the real posture before a power loss. Checks
// directory-fsync support and reports the platform's kDurable fsync backend.
core::Result<DurabilityCapability> ProbeDurability(const std::filesystem::path& data_dir);

// Current process id, widened to bridge pid_t (POSIX) and DWORD (Windows).
std::uint64_t ProcessId() noexcept;

namespace testing {

// Number of times Fsync(kDurable) had to fall back to a weaker barrier because
// the strong one was unsupported on the volume (ENOTSUP/EOPNOTSUPP). Process
// wide; for tests and degraded-mode assertions.
std::uint64_t DurableFsyncFallbackCount() noexcept;
void ResetDurableFsyncFallbackCount() noexcept;

#ifndef _WIN32
// Test seam for the Apple F_FULLFSYNC syscall: returns 0 on success or -1 with
// errno set. Pass nullptr to restore the real implementation. Production always
// uses the real implementation -- this exists only so unit tests can observe
// that the strong barrier was selected and inject ENOTSUP/EIO. On non-Apple
// POSIX it is a no-op (kDurable uses plain fsync there).
using FullFsyncFn = int (*)(int fd);
void SetFullFsyncForTesting(FullFsyncFn fn) noexcept;
std::uint64_t FullFsyncCallCount() noexcept;
void ResetFullFsyncCallCount() noexcept;
#endif

}  // namespace testing

}  // namespace abyss::platform::fs
