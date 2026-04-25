#include "abyss/net/socket_ops.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <string>
#include <utility>

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
#define ABYSS_NET_ATOMIC_FD_FLAGS 1
#define ABYSS_NET_SOCK_FLAGS (SOCK_NONBLOCK | SOCK_CLOEXEC)
#else
#define ABYSS_NET_ATOMIC_FD_FLAGS 0
#define ABYSS_NET_SOCK_FLAGS 0
#endif
// NOLINTEND(cppcoreguidelines-macro-usage)

namespace abyss::net {

namespace {

core::Error MakeErrno(core::ErrorCode code, std::string_view what) {
  std::string msg(what);
  msg += ": ";
  msg += std::strerror(errno);
  return {code, std::move(msg)};
}

core::Result<void> SetNonblockCloexec(int fd) {
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fcntl F_GETFL"));
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fcntl F_SETFL O_NONBLOCK"));
  }
  const int fd_flags = ::fcntl(fd, F_GETFD, 0);
  if (fd_flags < 0) return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fcntl F_GETFD"));
  if (::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "fcntl F_SETFD FD_CLOEXEC"));
  }
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)
  return {};
}

}  // namespace

void Fd::Reset() noexcept {
  if (fd_ != kInvalidFd) {
    CloseFd(fd_);
    fd_ = kInvalidFd;
  }
}

void CloseFd(int fd) noexcept {
  if (fd < 0) return;
  while (::close(fd) < 0 && errno == EINTR) {
  }
}

core::Result<ListenResult> CreateListenSocket(const ListenOptions& opts) {
  const int raw = ::socket(AF_INET, SOCK_STREAM | ABYSS_NET_SOCK_FLAGS, 0);
  if (raw < 0) return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "socket"));
  Fd listen_fd(raw);

  if constexpr (ABYSS_NET_ATOMIC_FD_FLAGS == 0) {
    if (auto r = SetNonblockCloexec(listen_fd.Get()); !r) return std::unexpected(r.error());
  }

  if (opts.reuse_addr) {
    const int on = 1;
    if (::setsockopt(listen_fd.Get(), SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "setsockopt SO_REUSEADDR"));
    }
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(opts.port);
  if (opts.bind_addr.empty() || opts.bind_addr == "0.0.0.0") {
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  } else if (::inet_pton(AF_INET, opts.bind_addr.c_str(), &addr.sin_addr) != 1) {
    std::string msg = "invalid bind address: ";
    msg += opts.bind_addr;
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, std::move(msg)});
  }

  if (::bind(listen_fd.Get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kUnavailable, "bind"));
  }

  if (::listen(listen_fd.Get(), opts.backlog) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kUnavailable, "listen"));
  }

  sockaddr_in bound{};
  socklen_t bound_len = sizeof(bound);
  if (::getsockname(listen_fd.Get(), reinterpret_cast<sockaddr*>(&bound), &bound_len) < 0) {
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "getsockname"));
  }

  return ListenResult{.fd = std::move(listen_fd), .bound_port = ntohs(bound.sin_port)};
}

core::Result<AcceptResult> AcceptNonBlocking(int listen_fd) {
  sockaddr_in addr{};
  socklen_t addr_len = sizeof(addr);
  int raw = kInvalidFd;

#ifdef __linux__
  raw = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len,
                  SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
  raw = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len);
#endif

  if (raw < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "accept would block"});
    }
    return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "accept"));
  }

  Fd accepted(raw);

#ifndef __linux__
  if (auto r = SetNonblockCloexec(accepted.Get()); !r) return std::unexpected(r.error());
#endif

  return AcceptResult{
      .fd = std::move(accepted),
      .remote_ipv4 = addr.sin_addr.s_addr,
      .remote_port = addr.sin_port,
  };
}

void SetTcpNoDelay(int fd) noexcept {
  int on = 1;
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
}

void SetNoSigPipe(int fd) noexcept {
#ifdef SO_NOSIGPIPE
  int on = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#else
  (void)fd;
#endif
}

void IgnoreSigPipeProcessWide() noexcept {
  static std::atomic<bool> done{false};
  if (done.exchange(true, std::memory_order_acq_rel)) return;
  std::signal(SIGPIPE, SIG_IGN);  // NOLINT(cert-err33-c)
}

std::string FormatIpv4(uint32_t addr_net) {
  std::array<char, INET_ADDRSTRLEN> buf{};
  in_addr in{};
  in.s_addr = addr_net;
  if (::inet_ntop(AF_INET, &in, buf.data(), buf.size()) == nullptr) return {};
  return {buf.data()};
}

}  // namespace abyss::net
