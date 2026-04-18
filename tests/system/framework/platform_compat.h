#pragma once

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")

using socket_t = SOCKET;
inline constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#define CLOSE_SOCKET(s) closesocket(s)
inline int GetSocketError() { return WSAGetLastError(); }
#else
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

using socket_t = int;
inline constexpr socket_t kInvalidSocket = -1;
#define CLOSE_SOCKET(s) close(s)
inline int GetSocketError() { return errno; }
#endif
