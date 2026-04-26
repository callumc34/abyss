#include "abyss/net/socket_ops.h"

#include "abyss/platform/net.h"
#include "abyss/platform/types.h"

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

#include <array>
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

namespace pnet = abyss::platform::net;

namespace {

core::Error MakeSocketError(core::ErrorCode code, std::string_view what) {
  std::string msg(what);
  msg += ": ";
  msg += pnet::LastErrorString();
  return {code, std::move(msg)};
}

}  // namespace

void Fd::Reset() noexcept {
  if (fd_ != kInvalidSocket) {
    pnet::CloseSocket(fd_);
    fd_ = kInvalidSocket;
  }
}

core::Result<ListenResult> CreateListenSocket(const ListenOptions& opts) {
  const Socket raw = ::socket(AF_INET, SOCK_STREAM | ABYSS_NET_SOCK_FLAGS, 0);
  if (raw == kInvalidSocket) {
    return std::unexpected(MakeSocketError(core::ErrorCode::kInternal, "socket"));
  }
  Fd listen_fd(raw);

  if constexpr (ABYSS_NET_ATOMIC_FD_FLAGS == 0) {
    if (auto r = pnet::SetNonBlocking(listen_fd.Get()); !r) return std::unexpected(r.error());
    pnet::SetCloseOnExec(listen_fd.Get());
  }

  if (opts.reuse_addr) {
    const int on = 1;
#ifdef _WIN32
    // POSIX-equivalent semantics: bind fails when the address is already in
    // use. Windows SO_REUSEADDR otherwise allows multiple concurrent binds to
    // the same port — surprising and dangerous for a server listener.
    if (::setsockopt(listen_fd.Get(), SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
                     reinterpret_cast<const char*>(&on), sizeof(on)) < 0) {
      return std::unexpected(
          MakeSocketError(core::ErrorCode::kInternal, "setsockopt SO_EXCLUSIVEADDRUSE"));
    }
#else
    // Allow rebinding ports stuck in TIME_WAIT after a clean restart.
    if (::setsockopt(listen_fd.Get(), SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&on),
                     sizeof(on)) < 0) {
      return std::unexpected(
          MakeSocketError(core::ErrorCode::kInternal, "setsockopt SO_REUSEADDR"));
    }
#endif
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
    return std::unexpected(MakeSocketError(core::ErrorCode::kUnavailable, "bind"));
  }

  if (::listen(listen_fd.Get(), opts.backlog) < 0) {
    return std::unexpected(MakeSocketError(core::ErrorCode::kUnavailable, "listen"));
  }

  sockaddr_in bound{};
#ifdef _WIN32
  int bound_len = sizeof(bound);
#else
  socklen_t bound_len = sizeof(bound);
#endif
  if (::getsockname(listen_fd.Get(), reinterpret_cast<sockaddr*>(&bound), &bound_len) < 0) {
    return std::unexpected(MakeSocketError(core::ErrorCode::kInternal, "getsockname"));
  }

  return ListenResult{.fd = std::move(listen_fd), .bound_port = ntohs(bound.sin_port)};
}

core::Result<AcceptResult> AcceptNonBlocking(Socket listen_fd) {
  sockaddr_in addr{};
#ifdef _WIN32
  int addr_len = sizeof(addr);
#else
  socklen_t addr_len = sizeof(addr);
#endif
  Socket raw = kInvalidSocket;

#ifdef __linux__
  raw = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len,
                  SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
  raw = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len);
#endif

  if (raw == kInvalidSocket) {
    if (pnet::IsWouldBlock(pnet::LastError())) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "accept would block"});
    }
    return std::unexpected(MakeSocketError(core::ErrorCode::kInternal, "accept"));
  }

  Fd accepted(raw);

#ifndef __linux__
  if (auto r = pnet::SetNonBlocking(accepted.Get()); !r) return std::unexpected(r.error());
  pnet::SetCloseOnExec(accepted.Get());
#endif

  return AcceptResult{
      .fd = std::move(accepted),
      .remote_ipv4 = addr.sin_addr.s_addr,
      .remote_port = addr.sin_port,
  };
}

void SetTcpNoDelay(Socket s) noexcept {
  int on = 1;
  (void)::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&on), sizeof(on));
}

std::string FormatIpv4(uint32_t addr_net) {
  std::array<char, INET_ADDRSTRLEN> buf{};
  in_addr in{};
  in.s_addr = addr_net;
  if (::inet_ntop(AF_INET, &in, buf.data(), buf.size()) == nullptr) return {};
  return {buf.data()};
}

}  // namespace abyss::net
