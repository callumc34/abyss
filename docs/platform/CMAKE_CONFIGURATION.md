# CMake Configuration Windows Implementation Plan

## Current State

### Problem Summary
- All CMake presets assume Linux/Unix environment
- No Windows vcpkg triplets exist
- `src/net/CMakeLists.txt` has `FATAL_ERROR` for Windows
- `src/platform/` directory doesn't exist

### Files Affected

| File | Issue |
|------|-------|
| `src/net/CMakeLists.txt` | FATAL_ERROR for Windows, no Windows source |
| `CMakePresets.json` | No Windows presets, Linux-only `container` preset |
| `CMakeLists.txt` | No platform directory, ccache Unix-only |
| `cmake/triplets/` | Only has `x64-linux-static-release.cmake` |

### Current Platform Detection

The only platform detection is in `src/net/CMakeLists.txt`:

```cmake
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND ABYSS_NET_SOURCES poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME MATCHES "BSD")
  list(APPEND ABYSS_NET_SOURCES poller_kqueue.cpp)
else()
  message(FATAL_ERROR
    "abyss::net has no Poller backend for ${CMAKE_SYSTEM_NAME}. "
    "Windows IOCP is tracked in a follow-up issue.")
endif()
```

## Architecture Overview

```
CMake Configuration Structure:

CMakeLists.txt
├── add_subdirectory(src/platform)        (NEW)
├── add_subdirectory(src/net)             (UPDATE)
├── add_subdirectory(src/queue)           (UPDATE - link platform)
├── add_subdirectory(src/core)            (UPDATE - link platform)
└── ...

CMakePresets.json
├── default              (unchanged - works on all platforms)
├── release             (unchanged - works on all platforms)
├── asan                (unchanged - Linux sanitizers)
├── tsan                (unchanged - Linux sanitizers)
├── bench               (unchanged - benchmark build)
├── cloud-only          (unchanged - no RocksDB)
├── container           (unchanged - Linux only)
└── windows-default     (NEW)
└── windows-release     (NEW)

cmake/triplets/
├── x64-linux-static-release.cmake  (exists)
└── x64-windows-static-release.cmake (NEW)
```

## Implementation Steps

### Step 1: Create Windows vcpkg Triplet

**File**: `cmake/triplets/x64-windows-static-release.cmake` (NEW)

```cmake
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Windows)
set(VCPKG_BUILD_TYPE release)

# Windows-specific settings
set(VCPKG_APPLINK_LOCAL_DATA OFF)
```

### Step 2: Update src/net/CMakeLists.txt

**File**: `src/net/CMakeLists.txt` (MODIFY)

Remove the FATAL_ERROR and add Windows poller:

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

### Step 3: Create src/platform/CMakeLists.txt

**File**: `src/platform/CMakeLists.txt` (NEW)

```cmake
cmake_minimum_required(VERSION 3.25)

set(ABYSS_PLATFORM_SOURCES
  fs_posix.cpp
)

# Platform-specific sources
if(WIN32)
  list(APPEND ABYSS_PLATFORM_SOURCES fs_windows.cpp)
endif()

add_library(abyss_platform ${ABYSS_PLATFORM_SOURCES})
add_library(abyss::platform ALIAS abyss_platform)

target_include_directories(abyss_platform PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
  $<INSTALL_INTERFACE:include>
)

target_compile_options(abyss_platform PRIVATE
  $<$<CXX_COMPILER_ID:MSVC>:/W4 /WX-${ABYSS_WERROR})
  $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall -Wextra $<$<BOOL:${ABYSS_WERROR}>:-Werror>>
)

# Platform-specific defines and includes
if(WIN32)
  target_compile_definitions(abyss_platform PRIVATE
    _CRT_SECURE_NO_WARNINGS
    _WIN32_WINNT=0x0601  # Windows 7 minimum
    WIN32_LEAN_AND_MEAN
  )
  target_link_libraries(abyss_platform PRIVATE
    # Windows system libs are linked automatically by CMake
  )
else()
  target_compile_definitions(abyss_platform PRIVATE
    _POSIX_C_SOURCE=200809L
  )
endif()
```

### Step 4: Update Main CMakeLists.txt

**File**: `CMakeLists.txt` (MODIFY)

Add platform directory and update dependencies:

```cmake
# OLD:
add_subdirectory(src/core)
add_subdirectory(src/log)
# ... etc

# NEW:
add_subdirectory(src/platform)  # ADD THIS FIRST - others depend on it
add_subdirectory(src/core)
add_subdirectory(src/log)
# ... etc
```

Update queue CMakeLists.txt to link platform:
```cmake
# In src/queue/CMakeLists.txt
target_link_libraries(abyss_queue PUBLIC abyss::platform)
```

Update core CMakeLists.txt to link platform:
```cmake
# In src/core/CMakeLists.txt
target_link_libraries(abyss_core PUBLIC abyss::platform)
```

### Step 5: Fix ccache Detection

**File**: `CMakeLists.txt` (MODIFY)

Make ccache conditional:

```cmake
# OLD:
if(ABYSS_USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(CCACHE_PROGRAM ccache)
  if(CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    message(STATUS "ccache enabled: ${CCACHE_PROGRAM}")
  endif()
endif()

# NEW:
if(ABYSS_USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  if(WIN32)
    # On Windows, ccache is less common - skip with a note
    # Users can set CMAKE_CXX_COMPILER_LAUNCHER manually
  else()
    find_program(CCACHE_PROGRAM ccache)
    if(CCACHE_PROGRAM)
      set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
      set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
      message(STATUS "ccache enabled: ${CCACHE_PROGRAM}")
    endif()
  endif()
endif()
```

### Step 6: Add Windows CMake Presets

**File**: `CMakePresets.json` (ADD NEW PRESETS)

Add these presets after the existing ones:

```json
{
  "name": "windows-default",
  "inherits": "default",
  "condition": {
    "type": "equals",
    "lhs": "${hostSystemName}",
    "rhs": "Windows"
  },
  "binaryDir": "${sourceDir}/build/${presetName}",
  "toolchainFile": "$env{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake",
  "cacheVariables": {
    "CMAKE_BUILD_TYPE": "Debug",
    "ABYSS_BUILD_TESTS": "ON",
    "ABYSS_WITH_ROCKSDB": "ON",
    "VCPKG_MANIFEST_FEATURES": "builtin-cold",
    "VCPKG_TARGET_TRIPLET": "x64-windows-static-release",
    "VCPKG_OVERLAY_TRIPLETS": "${sourceDir}/cmake/triplets"
  }
},
{
  "name": "windows-release",
  "inherits": "release",
  "condition": {
    "type": "equals",
    "lhs": "${hostSystemName}",
    "rhs": "Windows"
  },
  "binaryDir": "${sourceDir}/build/${presetName}",
  "toolchainFile": "$env{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake",
  "cacheVariables": {
    "CMAKE_BUILD_TYPE": "Release",
    "ABYSS_BUILD_TESTS": "OFF",
    "ABYSS_WITH_ROCKSDB": "ON",
    "VCPKG_MANIFEST_FEATURES": "builtin-cold",
    "VCPKG_TARGET_TRIPLET": "x64-windows-static-release",
    "VCPKG_OVERLAY_TRIPLETS": "${sourceDir}/cmake/triplets"
  }
},
{
  "name": "windows-asan",
  "inherits": "windows-default",
  "cacheVariables": {
    "CMAKE_BUILD_TYPE": "Debug",
    "ABYSS_WITH_ASAN": "ON"
  }
},
{
  "name": "windows-cloud-only",
  "inherits": "windows-default",
  "cacheVariables": {
    "ABYSS_WITH_ROCKSDB": "OFF"
  }
}
```

Also add corresponding build and test presets:

```json
"buildPresets": [
  // ... existing presets ...
  {
    "name": "windows-default",
    "configurePreset": "windows-default"
  },
  {
    "name": "windows-release",
    "configurePreset": "windows-release"
  }
],
"testPresets": [
  // ... existing presets ...
  {
    "name": "windows-default",
    "configurePreset": "windows-default",
    "output": {
      "outputOnFailure": true
    },
    "execution": {
      "jobs": 4,
      "scheduleRandom": true,
      "noTestsAction": "error",
      "timeout": 60
    }
  }
]
```

### Step 7: Update Container Preset

The `container` preset has a condition that prevents it from running on Windows. Consider adding:

```json
{
  "name": "container",
  "inherits": "release",
  "condition": {
    "type": "equals",
    "lhs": "${hostSystemName}",
    "rhs": "Linux"
  },
  // ... existing config ...
}
```

This already exists, so no change needed. The preset will be skipped on Windows.

## Verification Checklist

- [ ] `cmake --preset windows-default` configures on Windows
- [ ] `cmake --preset windows-release` configures on Windows
- [ ] Build uses correct vcpkg triplet
- [ ] All source files compile without errors
- [ ] Tests can be built and run
- [ ] Existing Linux presets still work

## Build Commands Reference

### Windows Build Commands

```bash
# Configure with preset
cmake --preset windows-default

# Build all targets
cmake --build build/windows-default

# Build specific target
cmake --build build/windows-default --target abyss-server

# Run tests
ctest --preset windows-default --parallel

# Run specific test
ctest --preset windows-default -R poller_test --output-on-failure
```

### Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `VCPKG_ROOT not set` | vcpkg not installed | Install vcpkg and set VCPKG_ROOT |
| `Unknown triplet` | Triplet not found | Create triplet in cmake/triplets/ |
| `Winsock not initialized` | Missing WSAStartup | Add to server entry point |
| `HANDLE type mismatch` | Wrong fd type | Use os_fd_t from platform/types.h |

## Dependencies

### vcpkg Dependencies for Windows

All existing dependencies should have Windows support:
- [x] cli11
- [x] crc32c
- [x] prometheus-cpp
- [x] spdlog
- [x] xxhash
- [x] yaml-cpp
- [x] rocksdb (via builtin-cold feature)

### Windows SDK

- Included with Visual Studio
- Minimum Windows 7 API set (0x0601)

## Advanced Configuration Options

### Debug Build with Debug Symbols

```json
{
  "name": "windows-debug",
  "inherits": "windows-default",
  "cacheVariables": {
    "CMAKE_BUILD_TYPE": "Debug",
    "CMAKE_CXX_FLAGS": "/Zi /Od"
  }
}
```

### Link-Time Optimization

```json
{
  "name": "windows-release-lto",
  "inherits": "windows-release",
  "cacheVariables": {
    "CMAKE_INTERPROCEDURAL_OPTIMIZATION": "ON"
  }
}
```

### Address Sanitizer (MSVC)

```json
{
  "name": "windows-asan",
  "inherits": "windows-default",
  "cacheVariables": {
    "CMAKE_BUILD_TYPE": "Debug",
    "MSVC_USE_STATIC_CRT": "OFF"
  }
}
```

Note: MSVC ASAN requires `/MDd` (dynamic CRT) and has different setup than GCC/Clang ASAN.
