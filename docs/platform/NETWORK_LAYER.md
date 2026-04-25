# Network Layer Windows Implementation Plan

## Current State

### Problem Summary
- `src/net/poller_windows.cpp` is a stub with `#error` directive
- `src/net/socket_ops.cpp` uses POSIX headers (`<arpa/inet.h>`, `<sys/socket.h>`, `<unistd.h>`) without platform guards
- `src/net/connection.cpp` uses POSIX socket functions (`::recv()`, `::send()`, `shutdown()`) without guards
- `src/net/CMakeLists.txt` calls `message(FATAL_ERROR)` for Windows

### Files Affected

| File | Lines | Issue |
|------|-------|-------|
| `src/net/poller_windows.cpp` | 1 | Stub with `#error` |
| `src/net/socket_ops.cpp` | 3-8 | POSIX includes |
| `src/net/socket_ops.cpp` | 66-67 | `::close()` with EINTR |
| `src/net/socket_ops.cpp` | 135-136 | `fcntl()` for SOCK_CLOEXEC |
| `src/net/connection.cpp` | 3-4 | POSIX includes |
| `src/net/connection.cpp` | 134 | `::shutdown()` |
| `src/net/connection.cpp` | 158 | `::recv()` |
| `src/net/connection.cpp` | 222-227 | `::send()` with MSG_NOSIGNAL |
| `src/net/CMakeLists.txt` | 13-15 | FATAL_ERROR on Windows |

## Architecture Overview

```
TcpServer
  └─ owns std::vector<Reactor>
       └─ Reactor owns Poller& (polymorphic)
            ├─ EpollPoller (Linux)
            ├─ KqueuePoller (macOS/BSD)
            └─ IocpPoller (Windows) [NEW - needs implementation]

Connection
  └─ owns Fd (int on POSIX, SOCKET on Windows)
  └─ uses Poller& for event registration

socket_ops.cpp
  └─ CreateListenSocket() → Fd + bound_port
  └─ AcceptNonBlocking() → Fd + remote info
  └─ SetTcpNoDelay(), SetNoSigPipe(), CloseFd()
```

## Implementation Steps

### Step 1: Create Platform Types Header

**File**: `include/abyss/platform/net_types.h` (NEW)

Purpose: Define platform-specific socket types and error handling

```cpp
#pragma once

#ifdef _WIN32
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")

// Windows: SOCKET is unsigned; INVALID_SOCKET is ~0
using socket_fd_t = SOCKET;
constexpr socket_fd_t kInvalidSocket = INVALID_SOCKET;

// Convert between socket and int for API compatibility
inline int SocketToInt(socket_fd_t s) { return static_cast<int>(s); }
inline socket_fd_t IntToSocket(int i) { return static_cast<socket_fd_t>(i); }

// Error handling
inline int GetSocketError() { return WSAGetLastError(); }
inline void SetSocketError(int e) { WSASetLastError(e); }

// Shutdown modes
constexpr int kShutdownRdWr = SD_BOTH;
constexpr int kShutdownRead = SD_RECEIVE;
constexpr int kShutdownWrite = SD_SEND;

#else
// POSIX
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

using socket_fd_t = int;
constexpr socket_fd_t kInvalidSocket = -1;

inline int SocketToInt(socket_fd_t s) { return s; }
inline socket_fd_t IntToSocket(int i) { return i; }

inline int GetSocketError() { return errno; }
inline void SetSocketError(int e) { errno = e; }

constexpr int kShutdownRdWr = SHUT_RDWR;
constexpr int kShutdownRead = SHUT_RD;
constexpr int kShutdownWrite = SHUT_WR;
#endif
```

### Step 2: Refactor socket_ops.h

**File**: `include/abyss/net/socket_ops.h` (MODIFY)

Changes:
1. Include platform types header at top
2. Change `kInvalidFd` to use platform type
3. Update `Fd` class to work with both int and SOCKET

```cpp
#pragma once

#include "abyss/core/result.h"
#include "abyss/platform/net_types.h"  // NEW

namespace abyss::net {

// Update Fd to be platform-agnostic
class Fd {
 public:
  Fd() = default;
  explicit Fd(socket_fd_t fd) noexcept : fd_(fd) {}
  ~Fd() noexcept { Reset(); }

  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;

  Fd(Fd&& other) noexcept : fd_(std::exchange(other.fd_, kInvalidSocket)) {}
  Fd& operator=(Fd&& other) noexcept {
    if (this != &other) { Reset(); fd_ = std::exchange(other.fd_, kInvalidSocket); }
    return *this;
  }

  socket_fd_t Get() const noexcept { return fd_; }
  socket_fd_t Release() noexcept { return std::exchange(fd_, kInvalidSocket); }
  void Reset() noexcept;
  bool Valid() const noexcept { return fd_ != kInvalidSocket; }

 private:
  socket_fd_t fd_ = kInvalidSocket;
};

// Rest of declarations remain the same...
```

### Step 3: Update socket_ops.cpp

**File**: `src/net/socket_ops.cpp` (MODIFY)

Changes: Add platform guards for headers and implement Windows path

```cpp
// OLD HEADERS (remove):
// #include <arpa/inet.h>
// #include <fcntl.h>
// #include <netinet/in.h>
// #include <netinet/tcp.h>
// #include <sys/socket.h>
// #include <unistd.h>

// NEW HEADERS:
#ifdef _WIN32
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
```

Key differences to handle:

| POSIX | Windows Equivalent |
|-------|-------------------|
| `close(fd)` | `closesocket(s)` for sockets |
| `errno` | `WSAGetLastError()` |
| `EAGAIN` | `WSAEWOULDBLOCK` (same value: 10035) |
| `EWOULDBLOCK` | `WSAEWOULDBLOCK` (same value) |
| `fcntl(F_GETFL)` | `ioctlsocket(s, FIONBIO, ...)` |
| `SOCK_NONBLOCK` | Non-blocking set via `ioctlsocket` |
| `SOCK_CLOEXEC` | No direct equivalent, set after creation |
| `SHUT_RDWR` | `SD_BOTH` |
| `MSG_NOSIGNAL` | Not needed on Windows |

Critical implementation changes:

```cpp
// CloseFd - needs platform-specific implementation
void CloseFd(int fd) noexcept {
  if (fd < 0) return;
#ifdef _WIN32
  closesocket(static_cast<SOCKET>(fd));
#else
  while (::close(fd) < 0 && errno == EINTR) {}
#endif
}

// SetNonblockCloexec - different on Windows
core::Result<void> SetNonblockCloexec(int fd) {
  if constexpr (ABYSS_NET_ATOMIC_FD_FLAGS == 0) {
#ifdef _WIN32
    // Windows: Use ioctlsocket for non-blocking
    unsigned long mode = 1;
    if (ioctlsocket(fd, FIONBIO, &mode) != 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "ioctlsocket FIONBIO"));
    }
    // CLOEXEC not applicable on Windows (different handle model)
#else
    // POSIX: Use fcntl
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return std::unexpected(MakeErrno(...));
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) { ... }
    // Set CLOEXEC...
#endif
  }
  return {};
}
```

### Step 4: Implement IOCP Poller

**File**: `src/net/poller_windows.cpp` (COMPLETE REWRITE)

This is the most complex part. IOCP uses a fundamentally different model than epoll/kqueue.

#### IOCP Concepts

1. **Completion Port**: `CreateIoCompletionPort()` creates a port
2. **Association**: Associate sockets with port via `CreateIoCompletionPort(hFile, Port, Key, 0)`
3. **Wait**: `GetQueuedCompletionStatus()` retrieves events
4. **Wake Event**: Separate manual-reset event for cross-thread wake

#### Architecture

```cpp
class IocpPoller : public Poller {
 public:
  static core::Result<std::unique_ptr<IocpPoller>> Create() {
    // 1. Create completion port
    HANDLE port = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
    if (!port) return unexpected(MakeWin32Error(...));

    // 2. Create wake event (auto-reset for Wake())
    HANDLE wake = CreateEvent(nullptr, FALSE, FALSE, nullptr);
    if (!wake) { CloseHandle(port); return unexpected(...); }

    // 3. Associate wake event with port
    // Note: Can't associate anonymous events, use a dummy socket pair instead

    return std::unique_ptr<IocpPoller>(new IocpPoller(port, wake));
  }

  // I/O operations
  core::Result<void> Add(int fd, EventKind interest, void* user_data) override {
    // 1. Associate socket with completion port
    HANDLE h = CreateIoCompletionPort(
      reinterpret_cast<HANDLE>(IntToSocket(fd)),
      port_,
      reinterpret_cast<ULONG_PTR>(user_data),
      0
    );
    if (!h) return unexpected(MakeWin32Error("CreateIoCompletionPort"));

    // 2. Store interest for later modification
    interests_[fd] = interest;

    // 3. Post initial zero-byte read for edge-triggered behavior
    // (or use overlapped I/O)

    return {};
  }

  core::Result<void> Modify(int fd, EventKind interest, void* user_data) override {
    interests_[fd] = interest;
    // Post new I/O as needed...
    return {};
  }

  core::Result<void> Remove(int fd) override {
    interests_.erase(fd);
    return {};
  }

  core::Result<std::span<const Event>> Wait(std::chrono::milliseconds timeout) override {
    DWORD num_events = 0;
    ULONG_PTR key = 0;
    OVERLAPPED* olap = nullptr;

    int timeout_ms = static_cast<int>(timeout.count());

    BOOL ok = GetQueuedCompletionStatus(
      port_,
      &num_events,
      &key,
      &olap,
      timeout_ms
    );

    if (!ok && olap == nullptr) {
      // Timeout
      return std::span<const Event>{};
    }

    if (olap == wake_overlapped_) {
      // Wake event
      return std::span<const Event>{};
    }

    // Determine event type from associated socket's stored interest
    // and completion status

    // ...
  }

  core::Result<void> Wake() override {
    SetEvent(wake_event_);
    return {};
  }

 private:
  HANDLE port_;
  HANDLE wake_event_;
  OVERLAPPED* wake_overlapped_;  // Associated with wake event
  std::unordered_map<int, EventKind> interests_;  // Track per-fd interests
  std::vector<OVERLAPPED_ENTRY> raw_events_;
  std::vector<Event> user_events_;
};
```

#### Key Implementation Details

1. **Association**: Every socket must be associated with the completion port. Association happens in `Add()`.

2. **Event Tracking**: IOCP doesn't store per-socket state like epoll. Need `std::unordered_map<int, EventKind>` to track interests.

3. **Edge Triggering**: IOCP is level-triggered by default. For edge-triggered behavior (matching epoll/kqueue), need to post new I/O after handling each event.

4. **Socket Modes**: Default Windows sockets are blocking. For non-blocking I/O with IOCP, either:
   - Use `WSASocket()` with `WSA_FLAG_OVERLAPPED`
   - Or use `ioctlsocket()` to set non-blocking mode

5. **Completion Key**: The `ULONG_PTR` key passed to `CreateIoCompletionPort` is used to identify the socket/user_data in `GetQueuedCompletionStatus`.

6. **Zero-Byte Operations**: For read notification, post a zero-byte `WSARecv()` or use `AcceptEx()` pattern.

### Step 5: Update connection.cpp

**File**: `src/net/connection.cpp` (MODIFY)

Changes: Add platform guards, use platform types

```cpp
// OLD:
#include <sys/socket.h>
#include <unistd.h>

// NEW:
#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif
```

Replace `shutdown()`:
```cpp
// OLD:
::shutdown(fd_.Get(), SHUT_RDWR);

// NEW:
#ifdef _WIN32
::shutdown(static_cast<SOCKET>(fd_.Get()), SD_BOTH);
#else
::shutdown(fd_.Get(), SHUT_RDWR);
#endif
```

Replace `recv()` error handling:
```cpp
if (n == SOCKET_ERROR) {
#ifdef _WIN32
  if (WSAGetLastError() == WSAEINTR) continue;
  if (WSAGetLastError() == WSAEWOULDBLOCK) break;
#else
  if (errno == EINTR) continue;
  if (errno == EAGAIN || errno == EWOULDBLOCK) break;
#endif
}
```

Replace `send()` with MSG_NOSIGNAL:
```cpp
#ifdef _WIN32
// Windows doesn't need MSG_NOSIGNAL - no SIGPIPE
const int flags = 0;
#else
const int flags =
#ifdef MSG_NOSIGNAL
  MSG_NOSIGNAL
#else
  0
#endif
;
#endif
```

### Step 6: Update CMakeLists.txt

**File**: `src/net/CMakeLists.txt` (MODIFY)

```cmake
# OLD:
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND ABYSS_NET_SOURCES poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME MATCHES "BSD")
  list(APPEND ABYSS_NET_SOURCES poller_kqueue.cpp)
else()
  message(FATAL_ERROR
    "abyss::net has no Poller backend for ${CMAKE_SYSTEM_NAME}. "
    "Windows IOCP is tracked in a follow-up issue.")
endif()

# NEW:
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND ABYSS_NET_SOURCES poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME MATCHES "BSD")
  list(APPEND ABYSS_NET_SOURCES poller_kqueue.cpp)
elseif(WIN32)
  list(APPEND ABYSS_NET_SOURCES poller_windows.cpp)
endif()
```

Also add Winsock initialization. The best place is in the server binary:

**File**: `apps/abyss-server/main.cpp` (or entry point)

```cpp
#ifdef _WIN32
#include <winsock2.h>
int main() {
  WSADATA wsa_data;
  int err = WSAStartup(MAKEWORD(2, 2), &wsa_data);
  if (err != 0) {
    // Handle error
    return 1;
  }
  // ... rest of main
}
#endif
```

## Verification Checklist

- [ ] `cmake --preset default` configures on Windows (no FATAL_ERROR)
- [ ] `CreateListenSocket()` creates and binds socket
- [ ] `AcceptNonBlocking()` accepts connections
- [ ] `Connection::OnReadable()` handles incoming data
- [ ] `Connection::OnWritable()` handles outgoing data
- [ ] `Connection::Close()` properly closes socket
- [ ] Server accepts Redis PING command and responds with PONG
- [ ] Multiple concurrent connections work

## Key Differences Summary

| Feature | POSIX | Windows |
|---------|-------|---------|
| FD type | `int` | `SOCKET` (unsigned) |
| Invalid value | `-1` | `INVALID_SOCKET` (~0) |
| Close socket | `close()` | `closesocket()` |
| Error code | `errno` | `WSAGetLastError()` |
| Non-blocking | `fcntl(O_NONBLOCK)` | `ioctlsocket(FIONBIO)` |
| Shutdown | `SHUT_RDWR` | `SD_BOTH` |
| No SIGPIPE | `MSG_NOSIGNAL` | Not needed |
| Event model | Edge-triggered | Completion-based |

## Dependencies

- Windows SDK (included with Visual Studio)
- Winsock2 (ws2_32.lib)
