#ifdef _WIN32

#include <ws2tcpip.h>

#include <atomic>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>

#include "abyss/platform/net.h"

namespace abyss::platform::net {

namespace {

std::mutex& InitMutex() {
  static std::mutex m;
  return m;
}

std::atomic<int>& InitCount() {
  static std::atomic<int> c{0};
  return c;
}

}  // namespace

core::Result<void> Init() {
  const std::scoped_lock lock(InitMutex());
  if (InitCount().load(std::memory_order_relaxed) > 0) {
    InitCount().fetch_add(1, std::memory_order_relaxed);
    return {};
  }
  WSADATA data{};
  const int rc = ::WSAStartup(MAKEWORD(2, 2), &data);
  if (rc != 0) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal,
                    std::string("WSAStartup: ") + std::system_category().message(rc)});
  }
  InitCount().store(1, std::memory_order_relaxed);
  return {};
}

void Shutdown() noexcept {
  const std::scoped_lock lock(InitMutex());
  const int prev = InitCount().fetch_sub(1, std::memory_order_relaxed);
  if (prev == 1) {
    ::WSACleanup();
  } else if (prev <= 0) {
    InitCount().store(0, std::memory_order_relaxed);
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
