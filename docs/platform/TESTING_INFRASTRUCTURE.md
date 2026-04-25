# Testing Infrastructure Windows Implementation Plan

## Current State

### Problem Summary
- `tests/system/framework/server_fixture.cpp` already has Windows implementation
- `tests/component/wal_queue_crash_test.cpp` uses `fork()` - Unix only
- `tests/unit/net/poller_test.cpp` uses `pipe()` - needs platform abstraction
- `tests/component/net/sync_redis_client.cpp` uses POSIX headers without guards

### Files Affected

| File | Issue |
|------|-------|
| `tests/unit/net/poller_test.cpp` | Uses `pipe()`, `read()`, `write()`, `close()` |
| `tests/component/wal_queue_crash_test.cpp` | Uses `fork()`, `waitpid()`, `mkdtemp()` |
| `tests/component/net/sync_redis_client.cpp` | Uses POSIX headers |
| `tests/system/framework/redis_client.cpp` | Uses `poll()` |
| `tests/system/framework/server_fixture.cpp` | Already has Windows support |

### Working Components

The test framework already has Windows support for:
- `tests/system/framework/platform_compat.h` - Type abstractions (SOCKET vs int)
- `tests/system/framework/server_fixture.cpp` - Full Windows process spawning
- `tests/support/temp_dir.h` - Windows `_getpid()` handling

## Architecture Overview

```
tests/
├── support/                      (already has some Windows support)
│   ├── temp_dir.h               ✓ Has _WIN32 guards
│   └── integration_harness.h    (may need updates)
│
├── unit/                         (most need updates)
│   ├── net/
│   │   └── poller_test.cpp      ✗ Needs pipe() abstraction
│   └── queue/
│       ├── segment_test.cpp     (uses POSIX headers)
│       └── offset_store_test.cpp (uses POSIX headers)
│
├── component/                    (most need updates)
│   ├── net/
│   │   └── sync_redis_client.cpp ✗ Needs POSIX guards
│   └── wal_queue_crash_test.cpp  ✗ Uses fork() - skip on Windows
│
└── system/                       (mostly working)
    └── framework/
        ├── server_fixture.cpp   ✓ Has Windows support
        ├── platform_compat.h    ✓ Has Windows types
        └── redis_client.cpp     (uses poll() - needs guards)
```

## Implementation Steps

### Step 1: Create Platform Pipe Abstraction

**File**: `tests/system/framework/pipe.h` (NEW)

Purpose: Cross-platform pipe creation for tests

```cpp
#pragma once

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include <unistd.h>
#include <fcntl.h>
#endif

#include <array>
#include <cstdint>

namespace abyss::test {

// Cross-platform non-blocking pipe
struct Pipe {
  std::array<os_fd_t, 2> fds{{kInvalidFd, kInvalidFd}};

  bool valid() const { return fds[0] != kInvalidFd && fds[1] != kInvalidFd; }

  void close_both() {
    close_read();
    close_write();
  }

  void close_read() {
#ifdef _WIN32
    if (fds[0] != kInvalidFd) { CloseHandle(fds[0]); fds[0] = kInvalidFd; }
#else
    if (fds[0] >= 0) { close(fds[0]); fds[0] = -1; }
#endif
  }

  void close_write() {
#ifdef _WIN32
    if (fds[1] != kInvalidFd) { CloseHandle(fds[1]); fds[1] = kInvalidFd; }
#else
    if (fds[1] >= 0) { close(fds[1]); fds[1] = -1; }
#endif
  }

  os_fd_t read_end() const { return fds[0]; }
  os_fd_t write_end() const { return fds[1]; }
};

// Create a non-blocking pipe
// Returns -1 on failure, 0 on success
int create_nonblocking_pipe(Pipe* out);

// Cross-platform poll() wrapper
int platform_poll(int fd, int events, int timeout_ms);

// Cross-platform read
ssize_t platform_read(os_fd_t fd, void* buf, size_t count);

// Cross-platform write
ssize_t platform_write(os_fd_t fd, const void* buf, size_t count);

// Cross-platform close
void platform_close(os_fd_t fd);

// Get process ID
int get_current_pid();

}  // namespace abyss::test
```

**Implementation**: `tests/system/framework/pipe.cpp`

```cpp
#include "pipe.h"
#include <errno>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>
#endif

namespace abyss::test {

// Platform-specific constants
#ifdef _WIN32
constexpr os_fd_t kInvalidFd = INVALID_HANDLE_VALUE;
#else
constexpr os_fd_t kInvalidFd = -1;
#endif

int create_nonblocking_pipe(Pipe* out) {
#ifdef _WIN32
  SECURITY_ATTRIBUTES sa{};
  sa.nLength = sizeof(sa);
  sa.bInheritHandle = TRUE;
  sa.lpSecurityDescriptor = nullptr;

  if (!CreatePipe(&out->fds[0], &out->fds[1], &sa, 0)) {
    return -1;
  }

  // Set read end to non-inheritable
  if (!SetHandleInformation(out->fds[0], HANDLE_FLAG_INHERIT, 0)) {
    CloseHandle(out->fds[0]);
    CloseHandle(out->fds[1]);
    return -1;
  }

  return 0;
#else
  int fds[2];
  if (pipe(fds) < 0) return -1;
  out->fds[0] = fds[0];
  out->fds[1] = fds[1];

  // Set non-blocking
  for (int i = 0; i < 2; i++) {
    int flags = fcntl(fds[i], F_GETFL, 0);
    if (flags < 0) return -1;
    if (fcntl(fds[i], F_SETFL, flags | O_NONBLOCK) < 0) return -1;
  }

  return 0;
#endif
}

int platform_poll(int fd, int events, int timeout_ms) {
#ifdef _WIN32
  // Windows: Use select() since poll() has limitations
  fd_set read_fds, write_fds, except_fds;
  FD_ZERO(&read_fds);
  FD_ZERO(&write_fds);
  FD_ZERO(&except_fds);

  SOCKET s = static_cast<SOCKET>(fd);

  if (events & POLLIN) FD_SET(s, &read_fds);
  if (events & POLLOUT) FD_SET(s, &write_fds);
  if (events & POLLERR) FD_SET(s, &except_fds);

  TIMEVAL tv{};
  tv.tv_usec = timeout_ms * 1000;

  int n = select(0, &read_fds, &write_fds, &except_fds, &tv);

  if (n <= 0) return n;

  int revents = 0;
  if (FD_ISSET(s, &read_fds)) revents |= POLLIN;
  if (FD_ISSET(s, &write_fds)) revents |= POLLOUT;
  if (FD_ISSET(s, &except_fds)) revents |= POLLERR;

  return revents;
#else
  struct pollfd pfd{fd, static_cast<short>(events), 0};
  return poll(&pfd, 1, timeout_ms);
#endif
}

ssize_t platform_read(os_fd_t fd, void* buf, size_t count) {
#ifdef _WIN32
  DWORD bytes_read = 0;
  if (ReadFile(fd, buf, static_cast<DWORD>(count), &bytes_read, nullptr)) {
    return bytes_read;
  }
  return -1;
#else
  return read(fd, buf, count);
#endif
}

ssize_t platform_write(os_fd_t fd, const void* buf, size_t count) {
#ifdef _WIN32
  DWORD bytes_written = 0;
  if (WriteFile(fd, buf, static_cast<DWORD>(count), &bytes_written, nullptr)) {
    return bytes_written;
  }
  return -1;
#else
  return write(fd, buf, count);
#endif
}

void platform_close(os_fd_t fd) {
#ifdef _WIN32
  CloseHandle(fd);
#else
  close(fd);
#endif
}

int get_current_pid() {
#ifdef _WIN32
  return _getpid();
#else
  return getpid();
#endif
}

}  // namespace abyss::test
```

### Step 2: Update poller_test.cpp

**File**: `tests/unit/net/poller_test.cpp` (MODIFY)

Replace pipe() with platform abstraction:

```cpp
// OLD:
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

int MakeNonblockingPipe(int fds[2]) {
  if (::pipe(fds) < 0) return -1;
  // ...
}

// NEW:
#include <gtest/gtest.h>
#include "abyss/net/poller.h"
#include "abyss/test/pipe.h"  // NEW

int MakeNonblockingPipe(int fds[2]) {
  Pipe p;
  if (create_nonblocking_pipe(&p) < 0) return -1;
  fds[0] = p.read_end();
  fds[1] = p.write_end();
  return 0;
}
```

Replace write/close:
```cpp
// OLD:
::write(fds[1], "x", 1);
::close(fds[0]);
::close(fds[1]);

// NEW:
platform_write(fds[1], "x", 1);
platform_close(fds[0]);
platform_close(fds[1]);
```

### Step 3: Handle wal_queue_crash_test.cpp

**File**: `tests/component/wal_queue_crash_test.cpp` (MODIFY)

This test fundamentally requires `fork()` for crash simulation. Options:

#### Option A: Skip on Windows (Recommended for Initial Support)

```cpp
#include <gtest/gtest.h>

#ifdef _WIN32
// Test requires fork() for crash simulation - not available on Windows
TEST_F(WalCrashTest, DISABLED_AckedWritesSurviveKillNineAndReopen) {
  GTEST_SKIP() << "Crash simulation via fork() not available on Windows. "
                  "This test validates crash recovery semantics and requires "
                  "Unix fork() for proper testing. The WAL guarantees are "
                  "still validated by other tests on Windows.";
}
#else
// Original test
#endif
```

#### Option B: Rewrite for Windows (Future Work)

Would require using Windows Job Objects with `GenerateConsoleCtrlEvent` or similar mechanisms. Significantly more complex.

**Recommendation**: Use Option A for initial Windows support, track Option B as a future enhancement.

### Step 4: Update sync_redis_client.cpp

**File**: `tests/component/net/sync_redis_client.cpp` (MODIFY)

Add platform guards:

```cpp
// OLD:
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

// NEW:
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
```

Replace `::close()`:
```cpp
// OLD:
::close(sock);

// NEW:
#ifdef _WIN32
closesocket(sock);
#else
::close(sock);
#endif
```

Replace `fcntl()`:
```cpp
// OLD:
const int flags = ::fcntl(sock, F_GETFL, 0);
::fcntl(sock, F_SETFL, flags | O_NONBLOCK);

// NEW:
#ifdef _WIN32
unsigned long mode = 1;
ioctlsocket(sock, FIONBIO, &mode);
#else
const int flags = ::fcntl(sock, F_GETFL, 0);
::fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#endif
```

### Step 5: Update redis_client.cpp

**File**: `tests/system/framework/redis_client.cpp` (MODIFY)

Similar changes to sync_redis_client.cpp for headers and socket operations.

### Step 6: Update Other Test Files

Check and update any remaining files with POSIX headers:

```bash
grep -r "unistd.h\|sys/socket.h\|sys/wait.h\|poll.h" tests/
```

Files to check:
- `tests/unit/queue/segment_test.cpp`
- `tests/unit/queue/offset_store_test.cpp`
- `tests/unit/cold/backends/*_test.cpp`

### Step 7: Update server_fixture.cpp (Verify)

The existing Windows implementation should be verified:

```cpp
// Already has: #ifdef _WIN32 blocks for Windows implementation
// Verify it compiles and works correctly after platform changes
```

### Step 8: Update CMakeLists.txt Files

**File**: `tests/unit/CMakeLists.txt` (Add new test sources)

Add platform test files if needed.

## Verification Checklist

- [ ] Unit tests compile on Windows
- [ ] Poller unit tests work on Windows
- [ ] WAL tests work (except crash test)
- [ ] Component tests work on Windows
- [ ] System tests work on Windows
- [ ] Crash test is properly skipped on Windows with clear message

## Key Differences Summary

| Feature | POSIX | Windows |
|---------|-------|---------|
| Pipe | `pipe()` | `CreatePipe()` |
| Close pipe | `close()` | `CloseHandle()` |
| Non-blocking | `fcntl(O_NONBLOCK)` | Inherit from creation |
| Poll | `poll()` | `select()` or `WSAPoll()` |
| Process ID | `getpid()` | `_getpid()` |
| Fork | Available | Not available |

## Test Categories and Windows Support

| Test Type | Windows Support | Notes |
|-----------|----------------|-------|
| Unit Tests | ✓ Full Support | Most work with pipe abstraction |
| Component Tests | ✓ Mostly Supported | Crash test needs skip |
| System Tests | ✓ Full Support | Framework already works |
| Integration Tests | ✓ Full Support | Should work |
| Benchmarks | ✓ Full Support | Should work |
| Fuzz Tests | ⚠ Needs Review | May need platform work |

## Dependencies

- No additional libraries needed
- Uses existing platform abstractions
