#pragma once

// Kernel handle types per platform. File handles and socket handles are the
// same int on POSIX but distinct types on Windows, so we keep them separate
// even on POSIX to avoid casts at the platform boundary.

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
// winsock2.h must precede windows.h.
#include <windows.h>
#include <winsock2.h>
#endif

namespace abyss::platform {

#ifdef _WIN32
using OsFd = HANDLE;
using Socket = SOCKET;
inline const OsFd kInvalidOsFd = INVALID_HANDLE_VALUE;  // cast, not constexpr
inline constexpr Socket kInvalidSocket = INVALID_SOCKET;
#else
using OsFd = int;
using Socket = int;
inline constexpr OsFd kInvalidOsFd = -1;
inline constexpr Socket kInvalidSocket = -1;
#endif

}  // namespace abyss::platform
