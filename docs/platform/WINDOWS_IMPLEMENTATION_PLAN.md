# Windows Compatibility - Implementation Plan

This directory contains the detailed plans for enabling Windows build and testing support for the Abyss project.

## Overview

The remediation requires changes across **5 main areas**:
1. [Network Layer](NETWORK_LAYER.md) - IOCP Poller + Socket Abstraction
2. [File I/O Layer](FILE_IO_LAYER.md) - Platform File Abstraction
3. [Testing Infrastructure](TESTING_INFRASTRUCTURE.md) - Fork-dependent tests
4. [CMake Configuration](CMAKE_CONFIGURATION.md) - Platform presets + triplets
5. [CI/CD](CI_CD.md) - Windows build pipeline

**Estimated Total Effort**: 8-12 weeks for full Windows support

## Quick Reference

### Critical Files Requiring Changes

| Area | Priority | Files |
|------|----------|-------|
| Network | P0 | `src/net/poller_windows.cpp`, `src/net/socket_ops.cpp`, `src/net/connection.cpp`, `src/net/CMakeLists.txt` |
| File I/O | P0 | `src/queue/segment.cpp`, `src/queue/file_offset_store.cpp`, NEW: `include/abyss/platform/fs.h`, `src/platform/fs_*.cpp` |
| Testing | P2 | `tests/unit/net/poller_test.cpp`, `tests/component/wal_queue_crash_test.cpp`, `tests/component/net/sync_redis_client.cpp` |
| CMake | P1 | `CMakePresets.json`, `cmake/triplets/x64-windows-static-release.cmake` |
| CI/CD | P1 | `.github/workflows/_build-windows.yml`, `.github/actions/setup-build-windows/` |

### Implementation Order

```
Phase 1: Foundation (Week 1-2)
  └─ Platform abstraction layer (types + file I/O)

Phase 2: File I/O (Week 2-3)
  └─ Update segment.cpp, file_offset_store.cpp

Phase 3: Network (Week 3-5)
  └─ IOCP poller, socket_ops updates, connection updates

Phase 4: Testing (Week 5-6)
  └─ Test framework updates

Phase 5: CMake/CI (Week 6-7)
  └─ Presets, triplets, CI pipeline

Phase 6: Integration (Week 7-8)
  └─ Full verification
```

## Success Criteria

- [ ] `cmake --preset default` configures on Windows without errors
- [ ] Server accepts connections on Windows
- [ ] Unit tests pass on Windows
- [ ] CI builds and tests on Windows
- [ ] No regressions on Linux

## Critical Decisions

See [DECISIONS.md](DECISIONS.md) for documented architectural decisions.
