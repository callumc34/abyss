#ifdef _WIN32

#include <io.h>
#include <process.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>

#include "abyss/platform/fs.h"

namespace abyss::platform::fs {

namespace {

core::Error MakeWin32Error(core::ErrorCode code, const char* what) {
  const DWORD err = ::GetLastError();
  std::string msg(what);
  msg.append(": ");
  msg.append(std::system_category().message(static_cast<int>(err)));
  return {code, std::move(msg)};
}

DWORD ToAccess(OpenMode mode) {
  switch (mode) {
    case OpenMode::kRead:
      return GENERIC_READ;
    case OpenMode::kWrite:
      return GENERIC_WRITE;
    case OpenMode::kReadWrite:
      return GENERIC_READ | GENERIC_WRITE;
  }
  return 0;
}

DWORD ToDisposition(OpenOptions opts) {
  if (!opts.create) return OPEN_EXISTING;
  if (opts.exclusive) return CREATE_NEW;
  if (opts.truncate) return CREATE_ALWAYS;
  return OPEN_ALWAYS;
}

}  // namespace

File::~File() noexcept { Close(); }

File::File(File&& other) noexcept : fd_(std::exchange(other.fd_, kInvalidOsFd)) {}

File& File::operator=(File&& other) noexcept {
  if (this != &other) {
    Close();
    fd_ = std::exchange(other.fd_, kInvalidOsFd);
  }
  return *this;
}

bool File::valid() const noexcept { return fd_ != kInvalidOsFd; }

OsFd File::Release() noexcept { return std::exchange(fd_, kInvalidOsFd); }

void File::Close() noexcept {
  if (fd_ != kInvalidOsFd) {
    ::CloseHandle(fd_);
    fd_ = kInvalidOsFd;
  }
}

core::Result<File> Open(const std::filesystem::path& path, OpenOptions opts) {
  const HANDLE h = ::CreateFileW(path.c_str(), ToAccess(opts.mode),
                                 FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                 ToDisposition(opts), FILE_ATTRIBUTE_NORMAL, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    const DWORD err = ::GetLastError();
    const auto code = (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND)
                          ? core::ErrorCode::kNotFound
                      : (err == ERROR_FILE_EXISTS || err == ERROR_ALREADY_EXISTS)
                          ? core::ErrorCode::kAlreadyExists
                          : core::ErrorCode::kInternal;
    ::SetLastError(err);
    return std::unexpected(MakeWin32Error(code, "CreateFile"));
  }
  return File{h};
}

namespace {

OVERLAPPED MakeOverlapped(std::uint64_t offset) noexcept {
  OVERLAPPED ov{};
  ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFU);
  ov.OffsetHigh = static_cast<DWORD>(offset >> 32U);
  return ov;
}

}  // namespace

core::Result<void> Pwrite(const File& f, const void* buf, std::size_t count, std::uint64_t offset) {
  const auto* p = static_cast<const std::uint8_t*>(buf);
  std::size_t remaining = count;
  std::uint64_t cursor = offset;
  while (remaining > 0) {
    OVERLAPPED ov = MakeOverlapped(cursor);
    const DWORD chunk = remaining > 0xFFFFFFFFU ? 0xFFFFFFFFU : static_cast<DWORD>(remaining);
    DWORD written = 0;
    if (::WriteFile(f.get(), p, chunk, &written, &ov) == 0) {
      return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "WriteFile"));
    }
    if (written == 0) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInternal, "WriteFile returned zero bytes"});
    }
    p += written;
    cursor += written;
    remaining -= written;
  }
  return {};
}

core::Result<std::size_t> Pread(const File& f, void* buf, std::size_t count, std::uint64_t offset) {
  auto* p = static_cast<std::uint8_t*>(buf);
  std::size_t total = 0;
  while (total < count) {
    OVERLAPPED ov = MakeOverlapped(offset + total);
    const DWORD chunk =
        (count - total) > 0xFFFFFFFFU ? 0xFFFFFFFFU : static_cast<DWORD>(count - total);
    DWORD read = 0;
    if (::ReadFile(f.get(), p + total, chunk, &read, &ov) == 0) {
      const DWORD err = ::GetLastError();
      if (err == ERROR_HANDLE_EOF) return total;
      ::SetLastError(err);
      return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "ReadFile"));
    }
    if (read == 0) break;  // EOF
    total += read;
  }
  return total;
}

core::Result<std::size_t> Read(const File& f, void* buf, std::size_t count) {
  const DWORD chunk = count > 0xFFFFFFFFU ? 0xFFFFFFFFU : static_cast<DWORD>(count);
  DWORD read = 0;
  if (::ReadFile(f.get(), buf, chunk, &read, nullptr) == 0) {
    const DWORD err = ::GetLastError();
    if (err == ERROR_HANDLE_EOF) return 0U;
    ::SetLastError(err);
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "ReadFile"));
  }
  return static_cast<std::size_t>(read);
}

core::Result<void> WriteAll(const File& f, const void* buf, std::size_t count) {
  const auto* p = static_cast<const std::uint8_t*>(buf);
  std::size_t remaining = count;
  while (remaining > 0) {
    const DWORD chunk = remaining > 0xFFFFFFFFU ? 0xFFFFFFFFU : static_cast<DWORD>(remaining);
    DWORD written = 0;
    if (::WriteFile(f.get(), p, chunk, &written, nullptr) == 0) {
      return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "WriteFile"));
    }
    if (written == 0) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInternal, "WriteFile returned zero bytes"});
    }
    p += written;
    remaining -= written;
  }
  return {};
}

core::Result<void> Ftruncate(const File& f, std::uint64_t size) {
  LARGE_INTEGER li;
  li.QuadPart = static_cast<LONGLONG>(size);
  if (::SetFilePointerEx(f.get(), li, nullptr, FILE_BEGIN) == 0) {
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "SetFilePointerEx"));
  }
  if (::SetEndOfFile(f.get()) == 0) {
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "SetEndOfFile"));
  }
  return {};
}

core::Result<void> Fsync(const File& f, SyncMode /*mode*/) {
  // FlushFileBuffers is the strongest durability barrier Windows exposes on a
  // file handle; it covers every SyncMode. Rename durability is handled
  // separately via MOVEFILE_WRITE_THROUGH in Rename.
  if (::FlushFileBuffers(f.get()) == 0) {
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "FlushFileBuffers"));
  }
  return {};
}

core::Result<std::uint64_t> FileSize(const File& f) {
  LARGE_INTEGER li{};
  if (::GetFileSizeEx(f.get(), &li) == 0) {
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "GetFileSizeEx"));
  }
  return static_cast<std::uint64_t>(li.QuadPart);
}

core::Result<void> Unlink(const std::filesystem::path& path) {
  if (::DeleteFileW(path.c_str()) == 0) {
    const DWORD err = ::GetLastError();
    if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) {
      return std::unexpected(core::Error{core::ErrorCode::kNotFound,
                                         std::string("unlink: no such file: ") + path.string()});
    }
    ::SetLastError(err);
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "DeleteFile"));
  }
  return {};
}

core::Result<void> Rename(const std::filesystem::path& from, const std::filesystem::path& to) {
  if (::MoveFileExW(from.c_str(), to.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) ==
      0) {
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "MoveFileEx"));
  }
  return {};
}

core::Result<DirSyncOutcome> FsyncDir(const std::filesystem::path& dir) {
  // FILE_FLAG_BACKUP_SEMANTICS opens a directory; FlushFileBuffers needs
  // GENERIC_WRITE on it. Access denied is a permissions or programming
  // error, never a missing filesystem capability.
  const HANDLE h = ::CreateFileW(dir.c_str(), GENERIC_READ | GENERIC_WRITE,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                 OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    const DWORD err = ::GetLastError();
    if (err == ERROR_NOT_SUPPORTED) return DirSyncOutcome::kUnsupported;
    ::SetLastError(err);
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "open dir for fsync"));
  }
  if (::FlushFileBuffers(h) == 0) {
    const DWORD err = ::GetLastError();
    ::CloseHandle(h);
    // FAT/exFAT and some network shares cannot flush a directory.
    if (err == ERROR_NOT_SUPPORTED || err == ERROR_INVALID_FUNCTION) {
      return DirSyncOutcome::kUnsupported;
    }
    ::SetLastError(err);
    return std::unexpected(MakeWin32Error(core::ErrorCode::kInternal, "FlushFileBuffers dir"));
  }
  ::CloseHandle(h);
  return DirSyncOutcome::kSynced;
}

core::Result<DurabilityCapability> ProbeDurability(const std::filesystem::path& data_dir) {
  DurabilityCapability cap;
  cap.backend = FsyncBackend::kFlushFileBuffers;
  auto dir = FsyncDir(data_dir);
  if (!dir.has_value()) return std::unexpected(dir.error());
  cap.dir_sync_supported = (*dir == DirSyncOutcome::kSynced);
  return cap;
}

std::uint64_t ProcessId() noexcept { return static_cast<std::uint64_t>(::GetCurrentProcessId()); }

namespace testing {

std::uint64_t DurableFsyncFallbackCount() noexcept { return 0; }

void ResetDurableFsyncFallbackCount() noexcept {}

}  // namespace testing

}  // namespace abyss::platform::fs

#endif  // _WIN32
