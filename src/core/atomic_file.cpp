#include "abyss/core/atomic_file.h"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <system_error>

#ifdef _WIN32
#include <io.h>
#include <process.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace abyss::core {
namespace {

Error IoError(std::string_view what, const std::filesystem::path& path) {
  std::string msg(what);
  msg.append(" '");
  msg.append(path.string());
  msg.append("': ");
  msg.append(std::strerror(errno));
  return {ErrorCode::kInternal, std::move(msg)};
}

Error FsError(std::string_view what, const std::filesystem::path& path, const std::error_code& ec) {
  std::string msg(what);
  msg.append(" '");
  msg.append(path.string());
  msg.append("': ");
  msg.append(ec.message());
  return {ErrorCode::kInternal, std::move(msg)};
}

std::filesystem::path TempSibling(const std::filesystem::path& path) {
  static std::atomic<uint64_t> counter{0};
  std::string suffix = ".tmp.";
#ifdef _WIN32
  suffix += std::to_string(::_getpid());
#else
  suffix += std::to_string(::getpid());
#endif
  suffix += '.';
  suffix += std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
  auto out = path;
  out += suffix;
  return out;
}

#ifndef _WIN32
Result<void> WriteFull(int fd, const void* data, size_t len) {
  const auto* p = static_cast<const uint8_t*>(data);
  size_t remaining = len;
  while (remaining > 0) {
    const auto n = ::write(fd, p, remaining);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(
          Error{ErrorCode::kInternal, std::string("write: ") + std::strerror(errno)});
    }
    p += n;
    remaining -= static_cast<size_t>(n);
  }
  return {};
}

Result<void> FsyncDir(const std::filesystem::path& dir) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    // Some filesystems reject directory fsync; treat as best-effort.
    if (errno == EPERM || errno == ENOTSUP || errno == EINVAL) return {};
    return std::unexpected(IoError("open dir for fsync", dir));
  }
  if (::fsync(fd) < 0 && errno != EINVAL && errno != ENOTSUP) {
    const int saved = errno;
    ::close(fd);
    errno = saved;
    return std::unexpected(IoError("fsync dir", dir));
  }
  ::close(fd);
  return {};
}
#endif  // !_WIN32

Result<void> EnsureParent(const std::filesystem::path& path) {
  const auto parent = path.parent_path();
  if (parent.empty()) return {};
  std::error_code ec;
  std::filesystem::create_directories(parent, ec);
  if (ec) return std::unexpected(FsError("create parent dir", parent, ec));
  return {};
}

}  // namespace

Result<void> WriteFileAtomic(const std::filesystem::path& path, std::span<const std::byte> bytes) {
  if (auto r = EnsureParent(path); !r.has_value()) return r;
  const auto tmp = TempSibling(path);

#ifdef _WIN32
  // FlushFileBuffers + MoveFileEx(WRITE_THROUGH) is the Windows analogue of
  // fsync + rename + dirsync.
  HANDLE h = CreateFileW(tmp.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
                         FILE_ATTRIBUTE_NORMAL, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    return std::unexpected(Error{ErrorCode::kInternal, "CreateFile tmp failed"});
  }
  DWORD written = 0;
  if (!WriteFile(h, bytes.data(), static_cast<DWORD>(bytes.size()), &written, nullptr) ||
      written != bytes.size()) {
    CloseHandle(h);
    DeleteFileW(tmp.c_str());
    return std::unexpected(Error{ErrorCode::kInternal, "WriteFile tmp failed"});
  }
  if (!FlushFileBuffers(h)) {
    CloseHandle(h);
    DeleteFileW(tmp.c_str());
    return std::unexpected(Error{ErrorCode::kInternal, "FlushFileBuffers failed"});
  }
  CloseHandle(h);
  if (!MoveFileExW(tmp.c_str(), path.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    DeleteFileW(tmp.c_str());
    return std::unexpected(Error{ErrorCode::kInternal, "MoveFileEx failed"});
  }
  return {};
#else
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return std::unexpected(IoError("open tmp", tmp));

  if (auto r = WriteFull(fd, bytes.data(), bytes.size()); !r.has_value()) {
    ::close(fd);
    ::unlink(tmp.c_str());
    return r;
  }

  if (::fsync(fd) < 0) {
    const int saved = errno;
    ::close(fd);
    ::unlink(tmp.c_str());
    errno = saved;
    return std::unexpected(IoError("fsync tmp", tmp));
  }
  ::close(fd);

  if (::rename(tmp.c_str(), path.c_str()) < 0) {
    const int saved = errno;
    ::unlink(tmp.c_str());
    errno = saved;
    return std::unexpected(IoError("rename", path));
  }

  return FsyncDir(path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path());
#endif
}

Result<void> WriteFileAtomic(const std::filesystem::path& path, std::string_view text) {
  return WriteFileAtomic(path, std::span<const std::byte>(
                                   reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

}  // namespace abyss::core
