#ifdef _WIN32

#include <ws2tcpip.h>

#include <mutex>
#include <string>
#include <system_error>

#include "abyss/platform/net.h"

namespace abyss::platform::net {

namespace {

// Both lock and counter are touched only inside the same critical section,
// so a plain int suffices.
std::mutex& InitMutex() {
  static std::mutex m;
  return m;
}

int& InitCount() {
  static int c = 0;
  return c;
}

}  // namespace

core::Result<void> Init() {
  const std::scoped_lock lock(InitMutex());
  if (InitCount() > 0) {
    ++InitCount();
    return {};
  }
  WSADATA data{};
  const int rc = ::WSAStartup(MAKEWORD(2, 2), &data);
  if (rc != 0) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal,
                    std::string("WSAStartup: ") + std::system_category().message(rc)});
  }
  InitCount() = 1;
  return {};
}

void Shutdown() noexcept {
  const std::scoped_lock lock(InitMutex());
  if (InitCount() <= 0) {
    InitCount() = 0;
    return;
  }
  if (--InitCount() == 0) {
    ::WSACleanup();
  }
}

Scope::Scope() noexcept : ok_(Init().has_value()) {}

Scope::~Scope() noexcept {
  if (ok_) Shutdown();
}

int LastError() noexcept { return ::WSAGetLastError(); }

std::string ErrorString(int err) { return std::system_category().message(err); }

bool IsWouldBlock(int err) noexcept { return err == WSAEWOULDBLOCK; }

bool IsInterrupted(int err) noexcept { return err == WSAEINTR; }

bool IsConnReset(int err) noexcept {
  return err == WSAECONNRESET || err == WSAENETRESET || err == WSAECONNABORTED ||
         err == WSAENOTCONN;
}

bool IsBrokenPipe(int err) noexcept { return err == WSAESHUTDOWN || err == WSAECONNABORTED; }

void CloseSocket(Socket s) noexcept {
  if (s == kInvalidSocket) return;
  ::closesocket(s);
}

core::Result<void> SetNonBlocking(Socket s) {
  u_long mode = 1;
  if (::ioctlsocket(s, FIONBIO, &mode) != 0) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("ioctlsocket FIONBIO: ") + LastErrorString()});
  }
  return {};
}

void SetCloseOnExec(Socket /*s*/) noexcept {}

void IgnoreSigPipe() noexcept {}

void SetNoSigPipePerSocket(Socket /*s*/) noexcept {}

int SendFlagsNoSigPipe() noexcept { return 0; }

}  // namespace abyss::platform::net

#endif  // _WIN32
