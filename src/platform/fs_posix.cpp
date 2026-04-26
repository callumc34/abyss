#ifndef _WIN32

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>

#include "abyss/platform/fs.h"

namespace abyss::platform::fs {

namespace {

core::Error MakeErrno(core::ErrorCode code, const char* what) {
  std::string msg(what);
  msg.append(": ");
  msg.append(std::strerror(errno));
  return {code, std::move(msg)};
}

int ToOpenFlags(OpenOptions opts) {
  int flags = 0;
  switch (opts.mode) {
    case OpenMode::kRead:
      flags = O_RDONLY;
      break;
    case OpenMode::kWrite:
      flags = O_WRONLY;
      break;
    case OpenMode::kReadWrite:
      flags = O_RDWR;
      break;
  }
  if (opts.create) flags |= O_CREAT;
  if (opts.exclusive) flags |= O_EXCL;
  if (opts.truncate) flags |= O_TRUNC;
  return flags;
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
    while (::close(fd_) < 0 && errno == EINTR) {
    }
    fd_ = kInvalidOsFd;
  }
}

core::Result<File> Open(const std::filesystem::path& path, OpenOptions opts) {
  const int flags = ToOpenFlags(opts);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  const int fd = ::open(path.c_str(), flags, 0644);
  if (fd < 0) {
    const auto code = (errno == ENOENT) ? core::ErrorCode::kNotFound : core::ErrorCode::kInternal;
    return std::unexpected(MakeErrno(code, "open"));
  }
  return File{fd};
}

core::Result<void> Pwrite(const File& f, const void* buf, std::size_t count, std::uint64_t offset) {
  const auto* p = static_cast<const std::uint8_t*>(buf);
  std::size_t remaining = count;
  auto cursor = static_cast<off_t>(offset);
  while (remaining > 0) {
    const auto n = ::pwrite(f.get(), p, remaining, cursor);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "pwrite"));
    }
    p += n;
    cursor += n;
    remaining -= static_cast<std::size_t>(n);
  }
  return {};
}

core::Result<std::size_t> Pread(const File& f, void* buf, std::size_t count, std::uint64_t offset) {
  auto* p = static_cast<std::uint8_t*>(buf);
  std::size_t total = 0;
  while (total < count) {
    const auto n = ::pread(f.get(), p + total, count - total, static_cast<off_t>(offset + total));
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "pread"));
    }
    if (n == 0) break;
    total += static_cast<std::size_t>(n);
  }
  return total;
}

core::Result<std::size_t> Read(const File& f, void* buf, std::size_t count) {
  while (true) {
    const auto n = ::read(f.get(), buf, count);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "read"));
    }
    return static_cast<std::size_t>(n);
  }
}

core::Result<void> WriteAll(const File& f, const void* buf, std::size_t count) {
  const auto* p = static_cast<const std::uint8_t*>(buf);
  std::size_t remaining = count;
  while (remaining > 0) {
    const auto n = ::write(f.get(), p, remaining);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "write"));
    }
    p += n;
    remaining -= static_cast<std::size_t>(n);
  }
  return {};
}

core::Result<void> Ftruncate(const File& f, std::uint64_t size) {
  if (::ftruncate(f.get(), static_cast<off_t>(size)) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "ftruncate"));
  }
  return {};
}

core::Result<void> Fsync(const File& f) {
  if (::fsync(f.get()) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fsync"));
  }
  return {};
}

core::Result<std::uint64_t> FileSize(const File& f) {
  struct stat st{};
  if (::fstat(f.get(), &st) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fstat"));
  }
  return static_cast<std::uint64_t>(st.st_size);
}

core::Result<void> Unlink(const std::filesystem::path& path) {
  if (::unlink(path.c_str()) < 0) {
    if (errno == ENOENT) {
      return std::unexpected(core::Error{core::ErrorCode::kNotFound,
                                         std::string("unlink: no such file: ") + path.string()});
    }
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "unlink"));
  }
  return {};
}

core::Result<void> Rename(const std::filesystem::path& from, const std::filesystem::path& to) {
  if (::rename(from.c_str(), to.c_str()) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "rename"));
  }
  return {};
}

core::Result<void> FsyncDir(const std::filesystem::path& dir) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == EPERM || errno == EINVAL || errno == ENOTSUP) return {};
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "open dir for fsync"));
  }
  if (::fsync(fd) < 0 && errno != EINVAL && errno != ENOTSUP) {
    const int saved = errno;
    while (::close(fd) < 0 && errno == EINTR) {
    }
    errno = saved;
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fsync dir"));
  }
  while (::close(fd) < 0 && errno == EINTR) {
  }
  return {};
}

std::uint64_t ProcessId() noexcept { return static_cast<std::uint64_t>(::getpid()); }

}  // namespace abyss::platform::fs

#endif  // !_WIN32
