#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "abyss/core/result.h"
#include "abyss/platform/types.h"

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <sys/types.h>
#endif

namespace abyss::platform::net {

// Process-wide refcounted init. POSIX: no-op. Windows: WSAStartup/WSACleanup.
core::Result<void> Init();
void Shutdown() noexcept;

// RAII pair for Init/Shutdown.
class Scope {
 public:
  Scope() noexcept;
  // POSIX impl is empty; Windows impl calls Shutdown(). Declared here so both
  // translation units share the same visible interface.
  ~Scope() noexcept;  // NOLINT(performance-trivially-destructible)
  Scope(const Scope&) = delete;
  Scope& operator=(const Scope&) = delete;
  Scope(Scope&&) = delete;
  Scope& operator=(Scope&&) = delete;
  bool ok() const noexcept { return ok_; }

 private:
  bool ok_ = false;
};

int LastError() noexcept;
std::string ErrorString(int err);
inline std::string LastErrorString() { return ErrorString(LastError()); }

bool IsWouldBlock(int err) noexcept;
bool IsInterrupted(int err) noexcept;
bool IsConnReset(int err) noexcept;
bool IsBrokenPipe(int err) noexcept;

// Close a socket. Idempotent on kInvalidSocket. Retries past EINTR on POSIX.
void CloseSocket(Socket s) noexcept;

core::Result<void> SetNonBlocking(Socket s);
void SetCloseOnExec(Socket s) noexcept;
void IgnoreSigPipe() noexcept;
void SetNoSigPipePerSocket(Socket s) noexcept;

// MSG_NOSIGNAL on Linux, 0 elsewhere. Use as the flags arg to send().
int SendFlagsNoSigPipe() noexcept;

// Thin wrappers that hide Windows char*/int casts and return a signed byte
// count (negative = error, recv == 0 = EOF). On error, query LastError().
inline std::int64_t Send(Socket s, const void* buf, std::size_t len, int flags) noexcept {
#ifdef _WIN32
  return ::send(s, static_cast<const char*>(buf), static_cast<int>(len), flags);
#else
  return ::send(s, buf, len, flags);
#endif
}

inline std::int64_t Recv(Socket s, void* buf, std::size_t len, int flags) noexcept {
#ifdef _WIN32
  return ::recv(s, static_cast<char*>(buf), static_cast<int>(len), flags);
#else
  return ::recv(s, buf, len, flags);
#endif
}

inline int ShutdownBoth(Socket s) noexcept {
#ifdef _WIN32
  return ::shutdown(s, SD_BOTH);
#else
  return ::shutdown(s, SHUT_RDWR);
#endif
}

}  // namespace abyss::platform::net
