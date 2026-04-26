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
//
// On POSIX this is an AF_UNIX socketpair. On Windows it's a TCP loopback pair.
// Both backends honour `small_buffers=true` to surface kernel flow control on
// payloads as small as a few KiB, which is what backpressure tests rely on.
//
// On Windows, SO_SNDBUF / SO_RCVBUF are reliably honoured ONLY when set BEFORE
// connect(). Listener inheritance to accepted sockets is unreliable: the
// kernel autotunes the accepted socket's send buffer to multiple MiB. So
// Read() returns the *connecting* socket (Connection's side, where Send() runs)
// and Write() returns the accepted socket (test peer). This way Connection's
// kernel send buffer is a known small size when small_buffers=true.
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

  static core::Result<SocketPair> Make(bool small_buffers = false);

 private:
  SocketPair(platform::Socket read, platform::Socket write) noexcept
      : read_end_(read), write_end_(write) {}

#ifdef _WIN32
  static void SetBuffers(platform::Socket s, int bytes) noexcept {
    (void)::setsockopt(s, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&bytes),
                       sizeof(bytes));
    (void)::setsockopt(s, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&bytes),
                       sizeof(bytes));
  }
#else
  static void SetBuffers(platform::Socket s, int bytes) noexcept {
    (void)::setsockopt(s, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes));
    (void)::setsockopt(s, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes));
  }
#endif

  platform::Socket read_end_ = platform::kInvalidSocket;
  platform::Socket write_end_ = platform::kInvalidSocket;
};

inline core::Result<SocketPair> SocketPair::Make(bool small_buffers) {
  // 8 KiB matches Windows' minimum effective SO_RCVBUF (smaller values are
  // silently rounded up). Setting it explicitly disables Windows TCP
  // autotuning so the buffer stays at this fixed size.
  constexpr int kSmallBuf = 8192;

#ifdef _WIN32
  using platform::net::CloseSocket;
  using platform::net::LastErrorString;

  const platform::Socket listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == platform::kInvalidSocket) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("socket(listener): ") + LastErrorString()});
  }

  // Listener buffers are set pre-bind; SO_RCVBUF *is* inherited by accepted
  // sockets reliably, but SO_SNDBUF inheritance is not. So the *reader* below
  // is the connecting socket (where pre-connect setsockopt is honoured), and
  // the writer is the accepted socket.
  if (small_buffers) SetBuffers(listener, kSmallBuf);

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

  const platform::Socket reader = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (reader == platform::kInvalidSocket) {
    CloseSocket(listener);
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       std::string("socket(reader): ") + LastErrorString()});
  }

  // Reader is the connecting socket: setsockopt BEFORE connect() is the only
  // way to pin SO_SNDBUF on Windows. This is what makes Connection.send()
  // hit EWOULDBLOCK after kSmallBuf bytes instead of having the kernel quietly
  // absorb multi-MiB payloads via autotuning.
  if (small_buffers) SetBuffers(reader, kSmallBuf);

  if (::connect(reader, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
    CloseSocket(listener);
    CloseSocket(reader);
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, std::string("connect: ") + LastErrorString()});
  }
  const platform::Socket writer = ::accept(listener, nullptr, nullptr);
  CloseSocket(listener);
  if (writer == platform::kInvalidSocket) {
    CloseSocket(reader);
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

  // AF_UNIX honours setsockopt post-creation; order doesn't matter.
  if (small_buffers) {
    SetBuffers(reader, kSmallBuf);
    SetBuffers(writer, kSmallBuf);
  }
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

  return SocketPair{reader, writer};
}

}  // namespace abyss::testing
