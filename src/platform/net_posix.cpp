#ifndef _WIN32

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <string>

#include "abyss/platform/net.h"

namespace abyss::platform::net {

core::Result<void> Init() { return {}; }

void Shutdown() noexcept {}

Scope::Scope() noexcept : ok_(true) {}

Scope::~Scope() noexcept = default;  // NOLINT(performance-trivially-destructible)

int LastError() noexcept { return errno; }

std::string ErrorString(int err) { return std::strerror(err); }

bool IsWouldBlock(int err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

bool IsInterrupted(int err) noexcept { return err == EINTR; }

bool IsConnReset(int err) noexcept {
  return err == ECONNRESET || err == ENOTCONN || err == ENETRESET;
}

bool IsBrokenPipe(int err) noexcept { return err == EPIPE; }

void CloseSocket(Socket s) noexcept {
  if (s == kInvalidSocket) return;
  while (::close(s) < 0 && errno == EINTR) {
  }
}

core::Result<void> SetNonBlocking(Socket s) {
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int flags = ::fcntl(s, F_GETFL, 0);
  if (flags < 0) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("fcntl F_GETFL: ") + std::strerror(errno)});
  }
  if (::fcntl(s, F_SETFL, flags | O_NONBLOCK) < 0) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal,
                    std::string("fcntl F_SETFL O_NONBLOCK: ") + std::strerror(errno)});
  }
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)
  return {};
}

void SetCloseOnExec(Socket s) noexcept {
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int fd_flags = ::fcntl(s, F_GETFD, 0);
  if (fd_flags >= 0) (void)::fcntl(s, F_SETFD, fd_flags | FD_CLOEXEC);
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)
}

void IgnoreSigPipe() noexcept {
  static std::atomic<bool> done{false};
  if (done.exchange(true, std::memory_order_acq_rel)) return;
  std::signal(SIGPIPE, SIG_IGN);  // NOLINT(cert-err33-c)
}

void SetNoSigPipePerSocket(Socket s) noexcept {
#ifdef SO_NOSIGPIPE
  const int on = 1;
  (void)::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#else
  (void)s;
#endif
}

int SendFlagsNoSigPipe() noexcept {
#ifdef MSG_NOSIGNAL
  return MSG_NOSIGNAL;
#else
  return 0;
#endif
}

}  // namespace abyss::platform::net

#endif  // !_WIN32
