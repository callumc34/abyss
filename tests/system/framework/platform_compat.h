#pragma once

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")

using socket_t = SOCKET;
inline constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#define CLOSE_SOCKET(s) closesocket(s)
inline int GetSocketError() { return WSAGetLastError(); }

using proc_handle_t = HANDLE;
using pipe_handle_t = HANDLE;
inline const proc_handle_t kInvalidProcHandle = nullptr;
inline const pipe_handle_t kInvalidPipeHandle = INVALID_HANDLE_VALUE;
#else
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using socket_t = int;
inline constexpr socket_t kInvalidSocket = -1;
#define CLOSE_SOCKET(s) close(s)
inline int GetSocketError() { return errno; }

using proc_handle_t = pid_t;
using pipe_handle_t = int;
inline constexpr proc_handle_t kInvalidProcHandle = -1;
inline constexpr pipe_handle_t kInvalidPipeHandle = -1;
#endif
