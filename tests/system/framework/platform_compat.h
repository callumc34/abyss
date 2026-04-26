#pragma once

// OS-handle aliases for the test server framework. Socket types live in
// abyss::platform — use those directly rather than re-aliasing here.

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
using proc_handle_t = HANDLE;
using pipe_handle_t = HANDLE;
inline const proc_handle_t kInvalidProcHandle = nullptr;
inline const pipe_handle_t kInvalidPipeHandle = INVALID_HANDLE_VALUE;
#else
#include <sys/types.h>
using proc_handle_t = pid_t;
using pipe_handle_t = int;
inline constexpr proc_handle_t kInvalidProcHandle = -1;
inline constexpr pipe_handle_t kInvalidPipeHandle = -1;
#endif
