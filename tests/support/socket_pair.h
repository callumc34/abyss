#pragma once

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#endif

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "abyss/core/result.h"
#include "abyss/platform/net.h"
#include "abyss/platform/types.h"

namespace abyss::testing {

// Two non-blocking, mutually-connected sockets.
class SocketPair {
 public:
  SocketPair() = default;
  ~SocketPair() {
    if (read_end_ != platform::kInvalidSocket) platform::net::CloseSocket(read_end_);
    if (write_end_ != platform::kInvalidSocket) platform::net::CloseSocket(write_end_);
  }
  SocketPair(const SocketPair&) = delete;
  SocketPair& operator=(const SocketPair&) = delete;
  SocketPair(SocketPair&& other) noexcept
      : read_end_(std::exchange(other.read_end_, platform::kInvalidSocket)),
        write_end_(std::exchange(other.write_end_, platform::kInvalidSocket)) {}
  SocketPair& operator=(SocketPair&& other) noexcept {
    if (this != &other) {
      if (read_end_ != platform::kInvalidSocket) platform::net::CloseSocket(read_end_);
      if (write_end_ != platform::kInvalidSocket) platform::net::CloseSocket(write_end_);
      read_end_ = std::exchange(other.read_end_, platform::kInvalidSocket);
      write_end_ = std::exchange(other.write_end_, platform::kInvalidSocket);
    }
    return *this;
  }

  platform::Socket Read() const noexcept { return read_end_; }
  platform::Socket Write() const noexcept { return write_end_; }
  platform::Socket ReleaseRead() noexcept {
    return std::exchange(read_end_, platform::kInvalidSocket);
  }

  // small_buffers makes the kernel surface flow control on small payloads so
  // backpressure tests behave the same on POSIX socketpair and Windows TCP.
  static core::Result<SocketPair> Make(bool small_buffers = false);

 private:
  SocketPair(platform::Socket read, platform::Socket write) noexcept
      : read_end_(read), write_end_(write) {}

  platform::Socket read_end_ = platform::kInvalidSocket;
  platform::Socket write_end_ = platform::kInvalidSocket;
};

inline core::Result<SocketPair> SocketPair::Make(bool small_buffers) {
#ifdef _WIN32
  using platform::net::CloseSocket;
  using platform::net::LastErrorString;

  const platform::Socket listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == platform::kInvalidSocket) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("socket(listener): ") + LastErrorString()});
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
    CloseSocket(listener);
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, std::string("bind: ") + LastErrorString()});
  }
  int addr_len = sizeof(addr);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &addr_len) == SOCKET_ERROR) {
    CloseSocket(listener);
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, std::string("getsockname: ") + LastErrorString()});
  }
  if (::listen(listener, 1) == SOCKET_ERROR) {
    CloseSocket(listener);
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, std::string("listen: ") + LastErrorString()});
  }

  const platform::Socket writer = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (writer == platform::kInvalidSocket) {
    CloseSocket(listener);
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("socket(writer): ") + LastErrorString()});
  }
  if (::connect(writer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
    CloseSocket(listener);
    CloseSocket(writer);
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, std::string("connect: ") + LastErrorString()});
  }
  const platform::Socket reader = ::accept(listener, nullptr, nullptr);
  CloseSocket(listener);
  if (reader == platform::kInvalidSocket) {
    CloseSocket(writer);
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, std::string("accept: ") + LastErrorString()});
  }
#else
  int sv[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("socketpair: ") + std::strerror(errno)});
  }
  const platform::Socket reader = sv[0];
  const platform::Socket writer = sv[1];
#endif

  if (auto r = platform::net::SetNonBlocking(reader); !r) {
    platform::net::CloseSocket(reader);
    platform::net::CloseSocket(writer);
    return std::unexpected(r.error());
  }
  if (auto r = platform::net::SetNonBlocking(writer); !r) {
    platform::net::CloseSocket(reader);
    platform::net::CloseSocket(writer);
    return std::unexpected(r.error());
  }

  if (small_buffers) {
    // Set SO_SNDBUF and SO_RCVBUF on both ends so flow control triggers
    // regardless of which side the connection-under-test owns. POSIX socketpair
    // honours SO_SNDBUF on AF_UNIX; Windows TCP loopback honours both. Setting
    // them on both sides keeps the helper portable.
    const int buf = 4096;
    for (auto s : {reader, writer}) {
      (void)::setsockopt(s, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&buf),
                         sizeof(buf));
      (void)::setsockopt(s, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&buf),
                         sizeof(buf));
    }
  }

  return SocketPair{reader, writer};
}

}  // namespace abyss::testing
