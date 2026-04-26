#include "sync_redis_client.h"

#include "abyss/platform/net.h"
#include "abyss/platform/types.h"

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#endif

#include <chrono>
#include <cstring>
#include <string>

namespace abyss::component_test {

namespace pnet = abyss::platform::net;
using ::abyss::platform::kInvalidSocket;
using ::abyss::platform::Socket;

namespace {

bool WaitFor(Socket s, short events, std::chrono::milliseconds timeout) {
#ifdef _WIN32
  WSAPOLLFD p{s, events, 0};
  const int n = ::WSAPoll(&p, 1, static_cast<int>(timeout.count()));
#else
  pollfd p{.fd = s, .events = events, .revents = 0};
  const int n = ::poll(&p, 1, static_cast<int>(timeout.count()));
#endif
  return n > 0 && (p.revents & events) != 0;
}

bool WaitWritable(Socket s, std::chrono::milliseconds timeout) {
  return WaitFor(s, POLLOUT, timeout);
}
bool WaitReadable(Socket s, std::chrono::milliseconds timeout) {
  return WaitFor(s, POLLIN, timeout);
}

}  // namespace

SyncRedisClient::~SyncRedisClient() { Close(); }

bool SyncRedisClient::Connect(uint16_t port, std::chrono::milliseconds timeout) {
  return Connect(port, ConnectOptions{.timeout = timeout});
}

bool SyncRedisClient::Connect(uint16_t port, const ConnectOptions& opts) {
  Close();
  Socket sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock == kInvalidSocket) return false;

  // Apply socket options BEFORE connect: Windows TCP ignores SO_RCVBUF /
  // SO_SNDBUF set on a connected socket, so callers (notably backpressure
  // tests) need this to take effect at all.
  if (opts.recv_buffer_bytes > 0) {
    const int sz = opts.recv_buffer_bytes;
    (void)::setsockopt(sock, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&sz), sizeof(sz));
  }

  if (auto r = pnet::SetNonBlocking(sock); !r) {
    pnet::CloseSocket(sock);
    return false;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  const int rc = ::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (rc < 0) {
    const int err = pnet::LastError();
    if (!pnet::IsWouldBlock(err)
#ifndef _WIN32
        && err != EINPROGRESS
#endif
    ) {
      pnet::CloseSocket(sock);
      return false;
    }
  }

  if (!WaitWritable(sock, opts.timeout)) {
    pnet::CloseSocket(sock);
    return false;
  }

  int err = 0;
#ifdef _WIN32
  int err_len = sizeof(err);
  if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &err_len) != 0 ||
      err != 0) {
#else
  socklen_t err_len = sizeof(err);
  if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &err_len) != 0 || err != 0) {
#endif
    pnet::CloseSocket(sock);
    return false;
  }

  // Returning to blocking mode keeps the rest of the test simple.
#ifdef _WIN32
  u_long mode = 0;
  if (::ioctlsocket(sock, FIONBIO, &mode) != 0) {
    pnet::CloseSocket(sock);
    return false;
  }
#else
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int flags = ::fcntl(sock, F_GETFL, 0);
  if (flags < 0 || ::fcntl(sock, F_SETFL, flags & ~O_NONBLOCK) < 0) {
    pnet::CloseSocket(sock);
    return false;
  }
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)
#endif

  fd_ = sock;
  return true;
}

void SyncRedisClient::Close() {
  if (fd_ != kInvalidSocket) {
    pnet::CloseSocket(fd_);
    fd_ = kInvalidSocket;
  }
}

std::string SyncRedisClient::Encode(const std::vector<std::string>& args) {
  std::string out = "*";
  out += std::to_string(args.size());
  out += "\r\n";
  for (const auto& a : args) {
    out += "$";
    out += std::to_string(a.size());
    out += "\r\n";
    out += a;
    out += "\r\n";
  }
  return out;
}

bool SyncRedisClient::SendRaw(const std::string& bytes) const {
  if (fd_ == kInvalidSocket) return false;
  size_t off = 0;
  while (off < bytes.size()) {
    const auto n = pnet::Send(fd_, bytes.data() + off, bytes.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && pnet::IsInterrupted(pnet::LastError())) continue;
    return false;
  }
  return true;
}

std::string SyncRedisClient::Command(std::initializer_list<std::string> args) const {
  if (!SendRaw(Encode(std::vector<std::string>(args)))) return {};
  return ReadSome(4096);
}

std::string SyncRedisClient::ReadSome(size_t n, std::chrono::milliseconds timeout) const {
  if (fd_ == kInvalidSocket) return {};
  if (!WaitReadable(fd_, timeout)) return {};
  std::string buf(n, '\0');
  const auto r = pnet::Recv(fd_, buf.data(), buf.size(), 0);
  if (r <= 0) return {};
  buf.resize(static_cast<size_t>(r));
  return buf;
}

}  // namespace abyss::component_test
