#pragma once

#include "abyss/platform/net.h"
#include "abyss/platform/types.h"

// Backwards-compat aliases for tests that pre-date abyss::platform.
using socket_t = abyss::platform::Socket;
inline constexpr socket_t kInvalidSocket = abyss::platform::kInvalidSocket;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CLOSE_SOCKET(s) ::abyss::platform::net::CloseSocket(s)

inline int GetSocketError() { return ::abyss::platform::net::LastError(); }

#ifdef _WIN32
using proc_handle_t = HANDLE;
using pipe_handle_t = HANDLE;
inline const proc_handle_t kInvalidProcHandle = nullptr;
inline const pipe_handle_t kInvalidPipeHandle = INVALID_HANDLE_VALUE;
#else
#include <sys/types.h>
#include <unistd.h>
using proc_handle_t = pid_t;
using pipe_handle_t = int;
inline constexpr proc_handle_t kInvalidProcHandle = -1;
inline constexpr pipe_handle_t kInvalidPipeHandle = -1;
#endif
