# Windows Compatibility Report for Abyss

## Executive Summary

The project has multiple critical cross-platform issues preventing build and testing on Windows. Issues stem from direct POSIX API usage without conditional compilation guards.

---

## Section 1: Current Issues Identified

### 1.1 Network Layer - CRITICAL (Build Blocker)

| File | Lines | Issue |
|------|-------|-------|
| `src/net/poller_windows.cpp` | 1 | Stub file contains only `#error "Windows IOCP transport not yet implemented"` |
| `src/net/socket_ops.cpp` | 3-8 | Unguarded includes of `<arpa/inet.h>`, `<fcntl.h>`, `<netinet/in.h>`, `<netinet/tcp.h>`, `<sys/socket.h>`, `<unistd.h>` |
| `src/net/socket_ops.cpp` | 66-67 | Uses `::close()` with `EINTR` handling |
| `src/net/socket_ops.cpp` | 135-136 | Uses `fcntl()` for SOCK_CLOEXEC fallback |
| `src/net/connection.cpp` | 3-4 | Includes `<sys/socket.h>`, `<unistd.h>` |
| `src/net/connection.cpp` | 134 | Uses `::shutdown()` with POSIX constants |
| `src/net/connection.cpp` | 158 | Uses `::recv()` and `errno` |
| `src/net/connection.cpp` | 222-227 | Uses `::send()` with `MSG_NOSIGNAL` |
| `src/net/poller_epoll.cpp` | 1-3 | Has `#if !defined(__linux__)` guard but stub exists for Windows |
| `src/net/poller_kqueue.cpp` | 6-8 | Includes `<sys/event.h>`, `<unistd.h>` - FreeBSD only |

### 1.2 POSIX-Only I/O Layer - CRITICAL (Build Blocker)

| File | Lines | Issue |
|------|-------|-------|
| `src/queue/segment.cpp` | 3-5 | Includes `<fcntl.h>`, `<sys/stat.h>`, `<unistd.h>` |
| `src/queue/segment.cpp` | 37 | Uses `::pwrite()` - POSIX only |
| `src/queue/segment.cpp` | 53 | Uses `::pread()` - POSIX only |
| `src/queue/segment.cpp` | 78,97,127,145,149,156,213,279,283 | Multiple `::close()`, `::unlink()`, `::ftruncate()`, `::fsync()` calls |
| `src/queue/segment.cpp` | 116 | Uses `::open()` with POSIX flags (O_RDWR \| O_CREAT \| O_EXCL, mode 0644) |
| `src/queue/file_offset_store.cpp` | 3-5 | Includes `<fcntl.h>`, `<sys/stat.h>`, `<unistd.h>` |
| `src/queue/file_offset_store.cpp` | 49,62,78,91 | Uses `::write()`, `::read()`, `::open()` |
| `src/queue/file_offset_store.cpp` | 70 | Uses `::fstat()` |
| `src/queue/file_offset_store.cpp` | 99,305 | Uses `::fsync()` |
| `src/queue/file_offset_store.cpp` | 177 | Uses `::getpid()` |
| `src/queue/file_offset_store.cpp` | 295 | Uses `::open()` with POSIX flags |
| `src/queue/file_offset_store.cpp` | 301,307,314 | Uses `::unlink()`, `::rename()` |

### 1.3 Testing Framework Issues

| File | Lines | Issue |
|------|-------|-------|
| `tests/support/temp_dir.h` | 12-18 | Has Windows `_getpid()` fallback - partially aware |
| `tests/system/framework/server_fixture.cpp` | 3-10 | Has `#ifdef _WIN32` guards with Windows implementation |
| `tests/system/framework/server_fixture.cpp` | 170,183,248 | Uses `::fork()`, `::execl()`, `::waitpid()` - no Windows equivalent |
| `tests/component/wal_queue_crash_test.cpp` | 5-6 | Includes `<sys/wait.h>`, `<unistd.h>` |
| `tests/component/wal_queue_crash_test.cpp` | 66,94 | Uses `::fork()`, `::waitpid()` |
| `tests/unit/net/poller_test.cpp` | 3-6 | Includes `<sys/socket.h>`, `<unistd.h>` |
| `tests/unit/net/poller_test.cpp` | 16,39,47,60,66 | Uses `::pipe()`, `::write()`, `::close()` |
| `tests/component/net/sync_redis_client.cpp` | 3-9 | Includes `<arpa/inet.h>`, `<poll.h>`, `<sys/socket.h>`, `<unistd.h>` |
| `tests/component/net/sync_redis_client.cpp` | 53 | Uses `::close()` |
| `tests/system/framework/redis_client.cpp` | 164 | Uses `::poll()` |
| `tests/system/net/tcp_server_smoke_test.cpp` | - | Likely uses socket APIs |
| `tests/unit/queue/segment_test.cpp` | 5 | Includes `<unistd.h>` |
| `tests/unit/queue/offset_store_test.cpp` | 5 | Includes `<unistd.h>` |
| `tests/unit/cold/backends/*_test.cpp` | various | Multiple files include `<unistd.h>` |

### 1.4 Application Code

| File | Lines | Issue |
|------|-------|-------|
| `apps/abyss-server/server.cpp` | 13-17 | Has `#ifdef _WIN32` with partial Windows handling |
| `apps/abyss-server/server.cpp` | 261-280 | Has platform-specific `NotifyReady()` - properly guarded |
| `apps/abyss-server/providers.cpp` | 10 | Includes `<unistd.h>` |

### 1.5 CMake Configuration Issues

| File | Issue |
|------|-------|
| `CMakePresets.json` | All presets assume vcpkg toolchain with Unix paths |
| `CMakePresets.json:60-73` | `container` preset uses condition `hostSystemName == Linux` |
| `CMakeLists.txt:33-40` | `ccache` only searches for Unix tool `ccache` |

---

## Section 2: Feasibility Study

### 2.1 Effort Assessment by Category

| Category | Files Affected | Complexity | Effort |
|----------|---------------|------------|--------|
| **Network I/O Abstraction** | 3 | High | 2-3 weeks |
| **File I/O Abstraction Layer** | 2 | High | 2-3 weeks |
| **Poller Implementation (IOCP)** | 1 | Very High | 2-4 weeks |
| **Testing Infrastructure** | 8 | Medium | 1-2 weeks |
| **CMake Configuration** | 2 | Low | 2-3 days |

### 2.2 Technical Feasibility

| Component | Feasibility | Notes |
|-----------|-------------|-------|
| **Socket API** | Achievable | Winsock 2.0 provides equivalent functionality |
| **File I/O** | Achievable | Windows `CreateFile`, `ReadFile`, `WriteFile` provide equivalent features |
| **Process Management** | Limited | `fork()/exec()` pattern has no direct Windows equivalent |
| **IPC (Pipes)** | Achievable | Windows named pipes or anonymous pipes can replace POSIX pipes |
| **Poller (IOCP)** | Achievable but complex | IOCP uses completion-based model vs notification-based epoll/kqueue |
| **POSIX Signals** | Not feasible | Signal semantics differ fundamentally |

### 2.3 Key Technical Challenges

1. **IOCP vs epoll/kqueue**: IOCP uses completion-based model vs notification-based epoll/kqueue
2. **Process forking**: Windows tests using `fork()` would need complete rewrite
3. **Unix file descriptors vs HANDLEs**: Different types need abstraction
4. **errno vs GetLastError()**: Error handling patterns differ
5. **POSIX signal semantics**: No direct equivalent on Windows
6. **mkdtemp()**: No direct Windows equivalent - need implementation

---

## Section 3: Remediation Plan

### Phase 1: Foundation Layer (1-2 weeks)

**Goal**: Create abstraction layer for platform differences

#### 1.1 Create Platform Abstraction Headers

Create `include/abyss/platform/fs.h`:
- Abstracts: `close`, `read`, `write`, `pread`, `pwrite`, `fstat`, `fsync`, `open`
- Provides: `fs::open()`, `fs::close()`, `fs::read()`, `fs::write()`, `fs::pread()`, `fs::pwrite()`, `fs::fstat()`, `fs::fsync()`, `fs::unlink()`, `fs::rename()`

Create `include/abyss/platform/net.h`:
- Abstracts: `socket`, `bind`, `listen`, `accept`, `connect`, `send`, `recv`, `shutdown`
- Provides: `net::socket()`, `net::close()`, `net::send()`, `net::recv()`, etc.

Create `include/abyss/platform/types.h`:
- Platform-specific `fd_t` (int vs HANDLE), `socket_t` (SOCKET vs int)
- Error translation functions

#### 1.2 Implement Windows Backend

- `src/platform/fs_windows.cpp` - Windows file operations using CreateFile/ReadFile/WriteFile
- `src/platform/net_windows.cpp` - Windows socket operations using Winsock

#### 1.3 Implement Linux Backend

- `src/platform/fs_posix.cpp` - POSIX implementation wrapping existing calls
- `src/platform/net_posix.cpp` - POSIX implementation wrapping existing calls

---

### Phase 2: Network Layer Fixes (2-3 weeks)

**Goal**: Enable building on Windows

#### 2.1 Implement IOCP Poller

Replace stub in `src/net/poller_windows.cpp` with full IOCP implementation:

```cpp
// Key functions needed:
// - CreateIoCompletionPort() for port creation
// - GetQueuedCompletionStatus() for event loop
// - Associate device with port via CreateIoCompletionPort()
// - Translate IOCP events to Poller interface (EventKind::kReadable/kWritable)
```

#### 2.2 Update socket_ops.cpp

Add `#ifdef _WIN32` guards:
- Include `<winsock2.h>` instead of BSD headers on Windows
- Use Winsock equivalents for all socket operations
- Handle `WSAGetLastError()` instead of `errno`
- Map Windows errors to equivalent codes

#### 2.3 Update connection.cpp

- Replace POSIX `recv()`/`send()` with platform abstractions
- Handle `EAGAIN`/`EWOULDBLOCK` on Windows (same constant)
- Use `SD_RECEIVE`/`SD_SEND`/`SD_BOTH` instead of `SHUT_RDWR`

---

### Phase 3: File I/O Layer Fixes (2-3 weeks)

**Goal**: Enable file operations on Windows

#### 3.1 Update segment.cpp

Add platform conditionals:
- Use `fs::open()` abstraction
- Use `fs::pread()`/`fs::pwrite()` for file operations
- Use `fs::fsync()` for durability
- Handle file permissions appropriately on Windows (FILE_ATTRIBUTE_NORMAL)

#### 3.2 Update file_offset_store.cpp

Similar changes to segment.cpp:
- Use platform abstractions for all file operations
- Replace `::getpid()` with platform abstraction

---

### Phase 4: Testing Infrastructure (1-2 weeks)

**Goal**: Enable test framework on Windows

#### 4.1 Fork-Dependent Test Handling

For tests using `fork()`:

**Option A**: Mark tests as `GTEST_SKIP()` on Windows with explanation
- Use `#ifdef _WIN32` to skip tests that require forking
- Document that crash recovery tests need Unix for full verification

**Option B**: Rewrite using Windows `CreateProcess()` semantics
- More work but provides equivalent test coverage

**Recommendation**: Option A for initial support; Option B for complete coverage

#### 4.2 Update poller_test.cpp

Replace `pipe()` with socketpair or platform-specific implementation:
```cpp
#ifdef _WIN32
// Use socketpair or CreatePipe
#else
// Use pipe()
#endif
```

#### 4.3 Update wal_queue_crash_test.cpp

Skip or rewrite crash test:
- This test fundamentally requires `fork()` for crash simulation
- Mark as skipped on Windows with clear documentation

---

### Phase 5: CMake Configuration (2-3 days)

**Goal**: Support Windows build presets

#### 5.1 Add Windows Preset

```json
{
  "name": "windows-default",
  "inherits": "default",
  "condition": {
    "type": "equals",
    "lhs": "${hostSystemName}",
    "rhs": "Windows"
  },
  "cacheVariables": {
    "VCPKG_TARGET_TRIPLET": "x64-windows",
    "CMAKE_EXE_LINKER_FLAGS": "/WHOLEARCHIVE:$(VCPKG_INSTALLATION_ROOT)/installed/x64-windows/lib/*.lib"
  }
}
```

#### 5.2 Fix ccache Detection

Add Windows-specific ccache detection or disable on Windows

#### 5.3 Update All Presets

Ensure presets work cross-platform or add Windows-specific variants

---

## Section 4: Priority Recommendations

### Immediate (Must Fix for Build)

1. **Implement IOCP poller** - Currently blocks any Windows build
2. **Add Windows socket_ops.cpp** - Required for networking
3. **Create file I/O abstraction** - Required for WAL functionality
4. **Update segment.cpp** - Complete file I/O support
5. **Update file_offset_store.cpp** - Complete file I/O support

### Short Term (1-2 sprints)

6. **CMake Windows presets** - Enable easy Windows builds
7. **Testing framework updates** - Fork-dependent tests handling

### Medium Term (Ongoing)

8. **Poller tests Windows support** - Complete test coverage
9. **CI/CD Windows support** - Automated Windows builds
10. **Performance optimization** - Windows-specific tuning

---

## Section 5: Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| IOCP implementation complexity | High | High | Use existing IOCP patterns, extensive testing |
| Breaking Linux builds | Medium | High | Test on both platforms, use CI matrix |
| Performance regression on Windows | Low | Medium | Benchmark before/after, optimize hot paths |
| Dependencies not available on Windows vcpkg | Medium | High | Verify all deps have Windows triplets |
| Testing coverage gaps | Medium | Medium | Add Windows-specific integration tests |
| Scope creep | High | Medium | Stick to phase plan, defer nice-to-haves |

---

## Section 6: File-by-File Fix Summary

| File | Required Action | Priority |
|------|----------------|----------|
| `src/net/poller_windows.cpp` | Implement full IOCP poller | P0 |
| `src/net/socket_ops.cpp` | Add platform conditionals, Winsock support | P0 |
| `src/net/connection.cpp` | Add platform conditionals | P0 |
| `src/queue/segment.cpp` | Replace POSIX with platform abstractions | P0 |
| `src/queue/file_offset_store.cpp` | Replace POSIX with platform abstractions | P0 |
| `apps/abyss-server/providers.cpp` | Add platform conditionals | P1 |
| `CMakePresets.json` | Add Windows presets | P1 |
| `tests/unit/net/poller_test.cpp` | Replace pipe() with platform abstraction | P2 |
| `tests/component/wal_queue_crash_test.cpp` | Skip on Windows | P2 |
| `tests/system/framework/server_fixture.cpp` | Already has Windows path | P2 |
| `tests/component/net/sync_redis_client.cpp` | Add platform conditionals | P2 |

---

## Section 7: Summary

**Total estimated effort**: 6-10 weeks for complete Windows support

**Critical blockers**: 3 (poller stub, unguarded socket_ops, unguarded file I/O)

**Files requiring changes**: ~15 source files, 2 CMake files

**Testing impact**: Some tests (fork-dependent) may not be fully portable

**Recommendation**: Start with foundation abstractions, then network layer, then file I/O. Add Windows CI after Phase 2 to catch issues early.
