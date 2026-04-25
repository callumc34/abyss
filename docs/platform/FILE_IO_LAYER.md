# File I/O Layer Windows Implementation Plan

## Current State

### Problem Summary
- `src/queue/segment.cpp` uses POSIX file operations without platform guards
- `src/queue/file_offset_store.cpp` uses POSIX file operations without platform guards
- `src/core/atomic_file.cpp` already has Windows support and serves as a model
- No centralized platform abstraction for file I/O

### Files Affected

| File | POSIX Operations Used |
|------|----------------------|
| `src/queue/segment.cpp` | `open`, `close`, `pread`, `pwrite`, `fstat`, `fsync`, `ftruncate`, `unlink` |
| `src/queue/file_offset_store.cpp` | `open`, `close`, `read`, `write`, `fstat`, `fsync`, `unlink`, `rename`, `getpid` |
| `src/core/atomic_file.cpp` | Already has `#ifdef _WIN32` (model for this work) |

### Reference: atomic_file.cpp Pattern

`src/core/atomic_file.cpp` already implements Windows support:

```cpp
#ifdef _WIN32
#include <io.h>
#include <process.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

// Uses CreateFileW, WriteFile, FlushFileBuffers, MoveFileExW on Windows
// Uses open, write, fsync, rename on POSIX
```

## Architecture Overview

```
segment.cpp
  ├─ Uses open(O_RDWR|O_CREAT|O_EXCL, 0644) for segment creation
  ├─ Uses pread/pwrite for atomic read/write at offset
  ├─ Uses fsync for durability
  ├─ Uses ftruncate to seal
  └─ Uses unlink to remove

file_offset_store.cpp
  ├─ Uses open/read/write for offset persistence
  ├─ Uses fsync for durability
  ├─ Uses rename for atomic update
  └─ Uses getpid for temp file naming

┌─────────────────────────────────────────────┐
│          Platform Abstraction Layer         │
│              (NEW - fs.h + fs_*.cpp)         │
├─────────────────────────────────────────────┤
│  segment.cpp  │  file_offset_store.cpp       │
│              │  │  atomic_file.cpp          │
├──────────────┴──┴───────────────────────────┤
│     Windows (CreateFile/ReadFile/WriteFile) │
│     POSIX   (open/read/write/close)         │
└─────────────────────────────────────────────┘
```

## Implementation Steps

### Step 1: Create Platform File I/O Abstraction

**File**: `include/abyss/platform/fs.h` (NEW)

Purpose: Define cross-platform file operations

```cpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "abyss/core/result.h"
#include "abyss/platform/types.h"  // For os_fd_t

namespace abyss::platform::fs {

// ============================================================================
// Types
// ============================================================================

// File handle abstraction
struct FileHandle {
  os_fd_t fd;

  FileHandle() : fd(kInvalidOsFd) {}
  explicit FileHandle(os_fd_t fd_) : fd(fd_) {}

  bool valid() const { return fd != kInvalidOsFd; }

  // RAII close - call from destructor or explicitly
  void close() {
    if (valid()) {
      Close(fd);
      fd = kInvalidOsFd;
    }
  }

  // For compatibility with existing code that uses .Get()
  os_fd_t Get() const { return fd; }
};

// Stat result
struct Stat {
  uint64_t size = 0;
  bool is_directory = false;
  bool is_regular_file = false;
};

// ============================================================================
// Open flags
// ============================================================================

enum class OpenMode { ReadOnly, WriteOnly, ReadWrite };

struct OpenOptions {
  OpenMode mode = OpenMode::ReadOnly;
  bool create = false;
  bool exclusive = false;       // O_EXCL - fail if exists
  bool truncate = false;       // O_TRUNC - truncate to zero
  bool append = false;          // O_APPEND - seek to end on write
  uint32_t permissions = 0644;   // Unix permissions (ignored on Windows)
};

// ============================================================================
// Core file operations
// ============================================================================

// Open file with options
core::Result<FileHandle> open(const std::string& path, OpenOptions opts);

// Simple convenience overloads
core::Result<FileHandle> open_read(const std::string& path);
core::Result<FileHandle> open_write(const std::string& path, bool create = false,
                                     bool exclusive = false);

// Close file descriptor
void Close(os_fd_t fd);

// Read from current position (updates position)
core::Result<size_t> read(os_fd_t fd, void* buf, size_t count);

// Write at current position (updates position)
core::Result<size_t> write(os_fd_t fd, const void* buf, size_t count);

// Read from specific offset (does NOT update position)
core::Result<size_t> pread(os_fd_t fd, void* buf, size_t count, uint64_t offset);

// Write to specific offset (does NOT update position)
core::Result<size_t> pwrite(os_fd_t fd, const void* buf, size_t count, uint64_t offset);

// Sync file to disk (durability guarantee)
core::Result<void> fsync(os_fd_t fd);

// Truncate file to specified size
core::Result<void> ftruncate(os_fd_t fd, uint64_t size);

// Get file metadata
core::Result<Stat> stat(const std::string& path);

// Get file metadata by fd
core::Result<Stat> fstat(os_fd_t fd);

// ============================================================================
// File operations
// ============================================================================

// Delete file
core::Result<void> unlink(const std::string& path);

// Rename/move file (atomic on most filesystems)
core::Result<void> rename(const std::string& old_path, const std::string& new_path);

// Create directory and parents if needed
core::Result<void> create_directories(const std::string& path);

// Remove directory (must be empty)
core::Result<void> remove_dir(const std::string& path);

// Remove directory tree (recursive)
core::Result<void> remove_all(const std::string& path);

// ============================================================================
// Directory fsync (for rename durability)
// ============================================================================

// fsync directory containing a file (ensures rename is durable)
core::Result<void> fsync_dir(const std::string& dir_path);

// ============================================================================
// Temp file helpers
// ============================================================================

// Get system temp directory
std::string temp_directory_path();

// Create unique temp directory (like mkdtemp)
core::Result<std::string> make_temp_directory(const std::string& prefix);

// Create temp file path (not created, just path)
std::string temp_file_path(const std::string& dir, const std::string& prefix);

// ============================================================================
// Process info
// ============================================================================

// Get current process ID (for temp file naming)
int get_process_id();

// ============================================================================
// Helper: Write all data (loop until complete)
// ============================================================================

core::Result<void> write_all(os_fd_t fd, const void* buf, size_t count);

}  // namespace abyss::platform::fs
```

### Step 2: Create Platform Types Header

**File**: `include/abyss/platform/types.h` (NEW)

Purpose: Define platform-specific types (needed before fs.h can compile)

```cpp
#pragma once

#ifdef _WIN32
#include <windows.h>

// Windows: file handles are HANDLE, sockets are SOCKET
using os_fd_t = HANDLE;
using socket_fd_t = SOCKET;

constexpr os_fd_t kInvalidOsFd = INVALID_HANDLE_VALUE;
constexpr socket_fd_t kInvalidSocket = INVALID_SOCKET;

#else
// POSIX: file descriptors and sockets are both int
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

using os_fd_t = int;
using socket_fd_t = int;

constexpr os_fd_t kInvalidOsFd = -1;
constexpr socket_fd_t kInvalidSocket = -1;
#endif
```

### Step 3: Create Windows Implementation

**File**: `src/platform/fs_windows.cpp` (NEW)

Purpose: Implement file operations using Windows API

```cpp
#include "abyss/platform/fs.h"
#include <windows.h>
#include <fileapi.h>
#include <handleapi.h>

namespace abyss::platform::fs {

// ============================================================================
// Error handling
// ============================================================================

core::Error MakeWinError(core::ErrorCode code, std::string_view what) {
  DWORD err = GetLastError();
  char* msg = nullptr;
  FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
                 nullptr, err, 0, reinterpret_cast<char*>(&msg), 0, nullptr);
  std::string result = what.empty() ? "" : std::string(what) + ": ";
  if (msg) {
    result += msg;
    LocalFree(msg);
  } else {
    result += "unknown error " + std::to_string(err);
  }
  return {code, result};
}

// ============================================================================
// open
// ============================================================================

core::Result<FileHandle> open(const std::string& path, OpenOptions opts) {
  DWORD access = 0;
  switch (opts.mode) {
    case OpenMode::ReadOnly:  access = GENERIC_READ; break;
    case OpenMode::WriteOnly: access = GENERIC_WRITE; break;
    case OpenMode::ReadWrite: access = GENERIC_READ | GENERIC_WRITE; break;
  }

  DWORD create_mode = OPEN_EXISTING;
  if (opts.create) {
    if (opts.exclusive) create_mode = CREATE_NEW;
    else if (opts.truncate) create_mode = CREATE_ALWAYS;
    else create_mode = OPEN_ALWAYS;
  }

  HANDLE h = CreateFileA(
    path.c_str(),
    access,
    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,  // Share mode
    nullptr,  // Security
    create_mode,
    FILE_ATTRIBUTE_NORMAL,
    nullptr
  );

  if (h == INVALID_HANDLE_VALUE) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "CreateFile"));
  }

  if (opts.truncate && opts.create) {
    SetFilePointer(h, 0, nullptr, FILE_BEGIN);
    SetEndOfFile(h);
  }

  return FileHandle(h);
}

// ============================================================================
// Close
// ============================================================================

void Close(os_fd_t fd) {
  CloseHandle(fd);
}

// ============================================================================
// read/write
// ============================================================================

core::Result<size_t> read(os_fd_t fd, void* buf, size_t count) {
  DWORD bytes_read = 0;
  if (!ReadFile(fd, buf, static_cast<DWORD>(count), &bytes_read, nullptr)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "ReadFile"));
  }
  return static_cast<size_t>(bytes_read);
}

core::Result<size_t> write(os_fd_t fd, const void* buf, size_t count) {
  DWORD bytes_written = 0;
  if (!WriteFile(fd, buf, static_cast<DWORD>(count), &bytes_written, nullptr)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "WriteFile"));
  }
  return static_cast<size_t>(bytes_written);
}

// ============================================================================
// pread/pwrite - lseek + read/write
// ============================================================================

core::Result<size_t> pread(os_fd_t fd, void* buf, size_t count, uint64_t offset) {
  LARGE_INTEGER off, new_off;
  off.QuadPart = static_cast<LONGLONG>(offset);

  if (!SetFilePointerEx(fd, off, &new_off, FILE_BEGIN)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "SetFilePointerEx"));
  }

  DWORD bytes_read = 0;
  if (!ReadFile(fd, buf, static_cast<DWORD>(count), &bytes_read, nullptr)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "ReadFile"));
  }
  return static_cast<size_t>(bytes_read);
}

core::Result<size_t> pwrite(os_fd_t fd, const void* buf, size_t count, uint64_t offset) {
  LARGE_INTEGER off, new_off;
  off.QuadPart = static_cast<LONGLONG>(offset);

  if (!SetFilePointerEx(fd, off, &new_off, FILE_BEGIN)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "SetFilePointerEx"));
  }

  DWORD bytes_written = 0;
  if (!WriteFile(fd, buf, static_cast<DWORD>(count), &bytes_written, nullptr)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "WriteFile"));
  }
  return static_cast<size_t>(bytes_written);
}

// ============================================================================
// fsync
// ============================================================================

core::Result<void> fsync(os_fd_t fd) {
  if (!FlushFileBuffers(fd)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "FlushFileBuffers"));
  }
  return {};
}

// ============================================================================
// ftruncate
// ============================================================================

core::Result<void> ftruncate(os_fd_t fd, uint64_t size) {
  LARGE_INTEGER sz;
  sz.QuadPart = static_cast<LONGLONG>(size);

  if (!SetFilePointerEx(fd, sz, nullptr, FILE_BEGIN)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "SetFilePointerEx"));
  }

  if (!SetEndOfFile(fd)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "SetEndOfFile"));
  }
  return {};
}

// ============================================================================
// stat/fstat
// ============================================================================

core::Result<Stat> stat(const std::string& path) {
  WIN32_FILE_ATTRIBUTE_DATA data;
  if (!GetFileAttributesExA(path.c_str(), GetFileExInfoStandard, &data)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kNotFound, "GetFileAttributesEx"));
  }

  Stat st;
  st.size = (static_cast<uint64_t>(data.nFileSizeHigh) << 32) | data.nFileSizeLow;
  st.is_directory = (data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
  st.is_regular_file = !st.is_directory;
  return st;
}

core::Result<Stat> fstat(os_fd_t fd) {
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(fd, &info)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "GetFileInformationByHandle"));
  }

  Stat st;
  st.size = (static_cast<uint64_t>(info.nFileSizeHigh) << 32) | info.nFileSizeLow;
  st.is_directory = (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
  st.is_regular_file = !st.is_directory;
  return st;
}

// ============================================================================
// File operations
// ============================================================================

core::Result<void> unlink(const std::string& path) {
  if (!DeleteFileA(path.c_str())) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "DeleteFile"));
  }
  return {};
}

core::Result<void> rename(const std::string& old_path, const std::string& new_path) {
  if (!MoveFileExA(old_path.c_str(), new_path.c_str(), MOVEFILE_REPLACE_EXISTING)) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "MoveFileEx"));
  }
  return {};
}

// ============================================================================
// Directory operations
// ============================================================================

core::Result<void> create_directories(const std::string& path) {
  if (!CreateDirectoryA(path.c_str(), nullptr)) {
    DWORD err = GetLastError();
    if (err != ERROR_ALREADY_EXISTS) {
      return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "CreateDirectory"));
    }
  }
  return {};
}

core::Result<void> remove_dir(const std::string& path) {
  if (!RemoveDirectoryA(path.c_str())) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "RemoveDirectory"));
  }
  return {};
}

core::Result<void> remove_all(const std::string& path) {
  // Windows: Use SHFileOperation or recursive approach
  // For simplicity, implement recursive deletion
  WIN32_FIND_DATAA find_data;
  HANDLE h = FindFirstFileA((path + "\\*").c_str(), &find_data);

  if (h == INVALID_HANDLE_VALUE) {
    // Might be a file, try to delete it
    if (DeleteFileA(path.c_str())) return {};
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "remove_all"));
  }

  do {
    std::string name = find_data.cFileName;
    if (name == "." || name == "..") continue;

    std::string full = path + "\\" + name;
    if (find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
      auto r = remove_all(full);
      if (!r) { FindClose(h); return r; }
    } else {
      if (!DeleteFileA(full.c_str())) {
        FindClose(h);
        return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "DeleteFile"));
      }
    }
  } while (FindNextFileA(h, &find_data));

  FindClose(h);

  if (!RemoveDirectoryA(path.c_str())) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "RemoveDirectory"));
  }
  return {};
}

// ============================================================================
// fsync_dir - Open directory and fsync it
// ============================================================================

core::Result<void> fsync_dir(const std::string& dir_path) {
  HANDLE h = CreateFileA(
    dir_path.c_str(),
    GENERIC_READ,
    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
    nullptr,
    OPEN_EXISTING,
    FILE_FLAG_BACKUP_SEMANTICS,  // Required for directories!
    nullptr
  );

  if (h == INVALID_HANDLE_VALUE) {
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "fsync_dir open"));
  }

  if (!FlushFileBuffers(h)) {
    DWORD err = GetLastError();
    CloseHandle(h);
    // ENOTSUP/EPERM is acceptable - some filesystems don't support dir fsync
    if (err == ERROR_NOT_SUPPORTED || err == ERROR_ACCESS_DENIED) {
      return {};
    }
    SetLastError(err);
    return std::unexpected(MakeWinError(core::ErrorCode::kInternal, "FlushFileBuffers"));
  }

  CloseHandle(h);
  return {};
}

// ============================================================================
// Temp file helpers
// ============================================================================

std::string temp_directory_path() {
  char path[MAX_PATH];
  DWORD len = GetTempPathA(MAX_PATH, path);
  if (len == 0) return ".";
  return std::string(path, len);
}

core::Result<std::string> make_temp_directory(const std::string& prefix) {
  std::string path = temp_directory_path() + "\\" + prefix + "XXXXXX";
  // CreateDirectory with unique name is complex on Windows
  // For now, use _mkdir and hope for uniqueness
  if (_mkdir(path.c_str()) != 0 && errno != EEXIST) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "mkdtemp failed"});
  }
  return path;
}

std::string temp_file_path(const std::string& dir, const std::string& prefix) {
  return dir + "\\" + prefix + ".tmp";
}

// ============================================================================
// Process ID
// ============================================================================

int get_process_id() {
  return _getpid();
}

// ============================================================================
// write_all helper
// ============================================================================

core::Result<void> write_all(os_fd_t fd, const void* buf, size_t count) {
  const char* p = static_cast<const char*>(buf);
  size_t remaining = count;
  while (remaining > 0) {
    auto r = write(fd, p, remaining);
    if (!r) return std::unexpected(r.error());
    p += *r;
    remaining -= *r;
  }
  return {};
}

}  // namespace abyss::platform::fs
```

### Step 4: Create POSIX Implementation

**File**: `src/platform/fs_posix.cpp` (NEW)

Purpose: Implement file operations using POSIX API (wrapper around existing calls)

```cpp
#include "abyss/platform/fs.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace abyss::platform::fs {

// ============================================================================
// Error handling
// ============================================================================

core::Error MakeErrno(core::ErrorCode code, std::string_view what) {
  return {code, std::string(what) + ": " + std::strerror(errno)};
}

// ============================================================================
// open
// ============================================================================

core::Result<FileHandle> open(const std::string& path, OpenOptions opts) {
  int flags = 0;
  switch (opts.mode) {
    case OpenMode::ReadOnly:  flags = O_RDONLY; break;
    case OpenMode::WriteOnly: flags = O_WRONLY; break;
    case OpenMode::ReadWrite: flags = O_RDWR; break;
  }

  if (opts.create) flags |= O_CREAT;
  if (opts.exclusive) flags |= O_EXCL;
  if (opts.truncate) flags |= O_TRUNC;
  if (opts.append) flags |= O_APPEND;

  int fd = ::open(path.c_str(), flags, opts.permissions);
  if (fd < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "open"));
  }
  return FileHandle(fd);
}

// ============================================================================
// Close
// ============================================================================

void Close(os_fd_t fd) {
  while (::close(fd) < 0 && errno == EINTR) {}
}

// ============================================================================
// read/write
// ============================================================================

core::Result<size_t> read(os_fd_t fd, void* buf, size_t count) {
  ssize_t n = ::read(fd, buf, count);
  if (n < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "read"));
  }
  return static_cast<size_t>(n);
}

core::Result<size_t> write(os_fd_t fd, const void* buf, size_t count) {
  ssize_t n = ::write(fd, buf, count);
  if (n < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "write"));
  }
  return static_cast<size_t>(n);
}

// ============================================================================
// pread/pwrite
// ============================================================================

core::Result<size_t> pread(os_fd_t fd, void* buf, size_t count, uint64_t offset) {
  ssize_t n = ::pread(fd, buf, count, static_cast<off_t>(offset));
  if (n < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "pread"));
  }
  return static_cast<size_t>(n);
}

core::Result<size_t> pwrite(os_fd_t fd, const void* buf, size_t count, uint64_t offset) {
  ssize_t n = ::pwrite(fd, buf, count, static_cast<off_t>(offset));
  if (n < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "pwrite"));
  }
  return static_cast<size_t>(n);
}

// ============================================================================
// fsync
// ============================================================================

core::Result<void> fsync(os_fd_t fd) {
  if (::fsync(fd) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fsync"));
  }
  return {};
}

// ============================================================================
// ftruncate
// ============================================================================

core::Result<void> ftruncate(os_fd_t fd, uint64_t size) {
  if (::ftruncate(fd, static_cast<off_t>(size)) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "ftruncate"));
  }
  return {};
}

// ============================================================================
// stat/fstat
// ============================================================================

core::Result<Stat> stat(const std::string& path) {
  struct stat st;
  if (::stat(path.c_str(), &st) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kNotFound, "stat"));
  }
  return Stat{
    .size = static_cast<uint64_t>(st.st_size),
    .is_directory = S_ISDIR(st.st_mode),
    .is_regular_file = S_ISREG(st.st_mode)
  };
}

core::Result<Stat> fstat(os_fd_t fd) {
  struct stat st;
  if (::fstat(fd, &st) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fstat"));
  }
  return Stat{
    .size = static_cast<uint64_t>(st.st_size),
    .is_directory = S_ISDIR(st.st_mode),
    .is_regular_file = S_ISREG(st.st_mode)
  };
}

// ============================================================================
// File operations
// ============================================================================

core::Result<void> unlink(const std::string& path) {
  if (::unlink(path.c_str()) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "unlink"));
  }
  return {};
}

core::Result<void> rename(const std::string& old_path, const std::string& new_path) {
  if (::rename(old_path.c_str(), new_path.c_str()) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "rename"));
  }
  return {};
}

// ============================================================================
// Directory operations
// ============================================================================

core::Result<void> create_directories(const std::string& path) {
  if (::mkdir(path.c_str(), 0755) < 0 && errno != EEXIST) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "mkdir"));
  }
  return {};
}

core::Result<void> remove_dir(const std::string& path) {
  if (::rmdir(path.c_str()) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "rmdir"));
  }
  return {};
}

core::Result<void> remove_all(const std::string& path) {
  // Simple recursive implementation using std::filesystem
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, ec.message()});
  }
  return {};
}

// ============================================================================
// fsync_dir
// ============================================================================

core::Result<void> fsync_dir(const std::string& dir_path) {
  int fd = ::open(dir_path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == EPERM || errno == EINVAL || errno == ENOTSUP) {
      return {};  // Best-effort
    }
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "open dir for fsync"));
  }
  if (::fsync(fd) < 0 && errno != EINVAL && errno != ENOTSUP) {
    int saved = errno;
    ::close(fd);
    errno = saved;
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fsync dir"));
  }
  ::close(fd);
  return {};
}

// ============================================================================
// Temp file helpers
// ============================================================================

std::string temp_directory_path() {
  const char* tmp = std::getenv("TMPDIR");
  if (!tmp) tmp = "/tmp";
  return tmp;
}

core::Result<std::string> make_temp_directory(const std::string& prefix) {
  std::string path = temp_directory_path() + "/" + prefix + "XXXXXX";
  char* result = ::mkdtemp(path.data());
  if (!result) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "mkdtemp"));
  }
  return path;
}

std::string temp_file_path(const std::string& dir, const std::string& prefix) {
  return dir + "/" + prefix + ".tmp";
}

// ============================================================================
// Process ID
// ============================================================================

int get_process_id() {
  return ::getpid();
}

// ============================================================================
// write_all helper
// ============================================================================

core::Result<void> write_all(os_fd_t fd, const void* buf, size_t count) {
  const char* p = static_cast<const char*>(buf);
  size_t remaining = count;
  while (remaining > 0) {
    ssize_t n = ::write(fd, p, remaining);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "write"));
    }
    p += n;
    remaining -= static_cast<size_t>(n);
  }
  return {};
}

}  // namespace abyss::platform::fs
```

### Step 5: Create Platform CMakeLists.txt

**File**: `src/platform/CMakeLists.txt` (NEW)

```cmake
set(ABYSS_PLATFORM_SOURCES
  fs_posix.cpp
)

if(WIN32)
  list(APPEND ABYSS_PLATFORM_SOURCES fs_windows.cpp)
endif()

add_library(abyss_platform ${ABYSS_PLATFORM_SOURCES})
add_library(abyss::platform ALIAS abyss_platform)

target_include_directories(abyss_platform PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include
)

target_compile_options(abyss_platform PRIVATE
  $<$<CXX_COMPILER_ID:MSVC>:/W4>
  $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall -Wextra>
)

# Platform-specific defines
if(WIN32)
  target_compile_definitions(abyss_platform PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
```

### Step 6: Update segment.cpp

**File**: `src/queue/segment.cpp` (MODIFY)

Replace POSIX includes and operations:

```cpp
// OLD:
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

// NEW:
#include "abyss/platform/fs.h"

// Also need to update the segment.h fd type
// Change: int fd_ = -1;
// To: os_fd_t fd_ = kInvalidOsFd;
```

Replace operations:

| Original | Replacement |
|----------|-------------|
| `::open(path, O_RDWR\|O_CREAT\|O_EXCL, 0644)` | `fs::open(path, {.mode=OpenMode::ReadWrite, .create=true, .exclusive=true})` |
| `::close(fd_)` | `fs::Close(fd_)` |
| `::pread(fd_, ...)` | `fs::pread(fd_, ...)` |
| `::pwrite(fd_, ...)` | `fs::pwrite(fd_, ...)` |
| `::fstat(fd_, &st)` | `fs::fstat(fd_)` |
| `::fsync(fd_)` | `fs::fsync(fd_)` |
| `::ftruncate(fd_, ...)` | `fs::ftruncate(fd_, ...)` |
| `::unlink(path)` | `fs::unlink(path)` |

**Update segment.h** - Change fd type:
```cpp
// In segment.h, add include and change type
#include "abyss/platform/types.h"

// Change fd_ from int to os_fd_t
os_fd_t fd_ = kInvalidOsFd;
```

### Step 7: Update file_offset_store.cpp

**File**: `src/queue/file_offset_store.cpp` (MODIFY)

Similar changes to segment.cpp:

```cpp
// OLD:
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

// NEW:
#include "abyss/platform/fs.h"
```

Replace operations:

| Original | Replacement |
|----------|------------|
| `::open(path, O_RDONLY)` | `fs::open_read(path)` |
| `::open(path, O_WRONLY\|O_CREAT\|O_TRUNC, 0644)` | `fs::open_write(path, true)` |
| `::close(fd)` | `fs::Close(fd)` |
| `::read(fd, ...)` | `fs::read(fd, ...)` |
| `::write(fd, ...)` | `fs::write(fd, ...)` |
| `::fstat(fd, &st)` | `fs::fstat(fd)` |
| `::fsync(fd)` | `fs::fsync(fd)` |
| `::unlink(path)` | `fs::unlink(path)` |
| `::getpid()` | `fs::get_process_id()` |

### Step 8: Update Dependencies

Update the main CMakeLists.txt to include platform library:

```cmake
add_subdirectory(src/platform)
```

Update dependent libraries to link to platform:

```cmake
# In src/queue/CMakeLists.txt
target_link_libraries(abyss_queue PRIVATE abyss::platform)

# In src/core/CMakeLists.txt
target_link_libraries(abyss_core PRIVATE abyss::platform)
```

### Step 9: Update atomic_file.cpp

Update to use new abstraction (or keep as-is for consistency with existing Windows implementation):

```cpp
// Option: Refactor atomic_file.cpp to use fs:: abstraction
// Or keep current implementation and add fs:: wrapper later
```

## Verification Checklist

- [ ] WAL segment creation works on Windows
- [ ] WAL segment opening and recovery works on Windows
- [ ] WAL append operations work on Windows
- [ ] WAL segment sealing works on Windows
- [ ] File offset store persists consumer positions on Windows
- [ ] File offset store loads positions on Windows restart
- [ ] Atomic file writes work on both platforms

## Key Differences Summary

| Feature | POSIX | Windows |
|---------|-------|---------|
| FD type | `int` | `HANDLE` |
| Open flags | O_RDONLY, O_CREAT, etc. | GENERIC_READ, CREATE_ALWAYS, etc. |
| Seek | `lseek()` | `SetFilePointerEx()` |
| Read/Write | `read()`, `write()` | `ReadFile()`, `WriteFile()` |
| Sync | `fsync()` | `FlushFileBuffers()` |
| Delete | `unlink()` | `DeleteFile()` |
| Rename | `rename()` | `MoveFileEx()` |
| Permissions | `0644` mode | Ignored, uses defaults |
| Temp dir | `/tmp` | `GetTempPath()` |

## Dependencies

- Windows SDK (included with Visual Studio)
- No additional libraries needed
