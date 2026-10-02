#ifndef _WIN32

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "abyss/platform/fs.h"
#include "abyss/platform/mapped_file.h"

namespace abyss::platform::fs {

namespace {

core::Error MakeErrno(core::ErrorCode code, const char* what) {
  std::string msg(what);
  msg.append(": ");
  msg.append(std::strerror(errno));
  return {code, std::move(msg)};
}

std::atomic<std::uint64_t> g_durable_fallbacks{0};

#ifdef __APPLE__
std::atomic<std::uint64_t> g_full_fsync_calls{0};

int RealFullFsync(int fd) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  return ::fcntl(fd, F_FULLFSYNC);
}

std::atomic<testing::FullFsyncFn> g_full_fsync_fn{&RealFullFsync};
#endif

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
    core::ErrorCode code = core::ErrorCode::kInternal;
    if (errno == ENOENT) code = core::ErrorCode::kNotFound;
    if (errno == EEXIST) code = core::ErrorCode::kAlreadyExists;
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

core::Result<void> Fsync(const File& f, SyncMode mode) {
#ifdef __APPLE__
  if (mode == SyncMode::kDurable || mode == SyncMode::kDurableData) {
    g_full_fsync_calls.fetch_add(1, std::memory_order_relaxed);
    const auto full_fsync = g_full_fsync_fn.load(std::memory_order_acquire);
    if (full_fsync(f.get()) == 0) return {};
    // F_FULLFSYNC is unsupported on some volumes (e.g. certain network/overlay
    // filesystems). Fall back to the strongest barrier that volume does
    // support; any other error (e.g. EIO) is a real durability failure.
    if (errno != ENOTSUP && errno != EOPNOTSUPP) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fcntl(F_FULLFSYNC)"));
    }
    g_durable_fallbacks.fetch_add(1, std::memory_order_relaxed);
  }
#else
  if (mode == SyncMode::kDurableData) {
    if (::fdatasync(f.get()) < 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fdatasync"));
    }
    return {};
  }
#endif
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

core::Result<DirSyncOutcome> FsyncDir(const std::filesystem::path& dir) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == EPERM || errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
      return DirSyncOutcome::kUnsupported;
    }
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "open dir for fsync"));
  }
  const int rc = ::fsync(fd);
  const int saved = errno;
  while (::close(fd) < 0 && errno == EINTR) {
  }
  if (rc < 0) {
    if (saved == EINVAL || saved == ENOTSUP || saved == EOPNOTSUPP) {
      return DirSyncOutcome::kUnsupported;
    }
    errno = saved;
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fsync dir"));
  }
  return DirSyncOutcome::kSynced;
}

core::Result<DurabilityCapability> ProbeDurability(const std::filesystem::path& data_dir) {
  DurabilityCapability cap;
#ifdef __APPLE__
  cap.backend = FsyncBackend::kFullFsync;
#else
  cap.backend = FsyncBackend::kFsync;
#endif
  auto dir = FsyncDir(data_dir);
  if (!dir.has_value()) return std::unexpected(dir.error());
  cap.dir_sync_supported = (*dir == DirSyncOutcome::kSynced);
  return cap;
}

std::uint64_t ProcessId() noexcept { return static_cast<std::uint64_t>(::getpid()); }

core::Result<MappedFile> MappedFile::Map(const File& file, std::size_t size) {
  if (!file.valid() || size == 0) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, "map: no file or size"});
  }
  auto file_size = FileSize(file);
  if (!file_size.has_value()) return std::unexpected(file_size.error());
  if (*file_size < size) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "map: file is smaller than the mapping"});
  }
  void* addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, file.get(), 0);
  if (addr == MAP_FAILED) return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "mmap"));
  return MappedFile(static_cast<std::byte*>(addr), size, kInvalidOsFd);
}

MappedFile::~MappedFile() { Unmap(); }

MappedFile::MappedFile(MappedFile&& other) noexcept
    : data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      mapping_(std::exchange(other.mapping_, kInvalidOsFd)) {}

MappedFile& MappedFile::operator=(MappedFile&& other) noexcept {
  if (this != &other) {
    Unmap();
    data_ = std::exchange(other.data_, nullptr);
    size_ = std::exchange(other.size_, 0);
    mapping_ = std::exchange(other.mapping_, kInvalidOsFd);
  }
  return *this;
}

core::Result<void> MappedFile::WriteBack(std::size_t offset, std::size_t length) const {
  if (data_ == nullptr || offset > size_ || length > size_ - offset) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "write back: range outside the mapping"});
  }
  return {};
}

void MappedFile::Unmap() noexcept {
  if (data_ == nullptr) return;
  ::munmap(data_, size_);
  data_ = nullptr;
  size_ = 0;
}

core::Result<void> ZeroFill(const File& file, std::uint64_t size) {
  constexpr std::size_t kChunk = std::size_t{1} << 20;
  static const std::vector<std::byte> zeros(kChunk);
  for (std::uint64_t offset = 0; offset < size; offset += kChunk) {
    const auto n = static_cast<std::size_t>(std::min<std::uint64_t>(kChunk, size - offset));
    if (auto w = Pwrite(file, zeros.data(), n, offset); !w.has_value()) return w;
  }
  return Ftruncate(file, size);
}

namespace testing {

std::uint64_t DurableFsyncFallbackCount() noexcept {
  return g_durable_fallbacks.load(std::memory_order_relaxed);
}

void ResetDurableFsyncFallbackCount() noexcept {
  g_durable_fallbacks.store(0, std::memory_order_relaxed);
}

#ifdef __APPLE__
void SetFullFsyncForTesting(FullFsyncFn fn) noexcept {
  g_full_fsync_fn.store(fn != nullptr ? fn : &RealFullFsync, std::memory_order_release);
}

std::uint64_t FullFsyncCallCount() noexcept {
  return g_full_fsync_calls.load(std::memory_order_relaxed);
}

void ResetFullFsyncCallCount() noexcept { g_full_fsync_calls.store(0, std::memory_order_relaxed); }
#else
void SetFullFsyncForTesting(FullFsyncFn /*fn*/) noexcept {}

std::uint64_t FullFsyncCallCount() noexcept { return 0; }

void ResetFullFsyncCallCount() noexcept {}
#endif

}  // namespace testing

}  // namespace abyss::platform::fs

#endif  // !_WIN32
