#include "sync_redis_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>

namespace abyss::component_test {

namespace {

bool WaitWritable(int fd, std::chrono::milliseconds timeout) {
  pollfd p{.fd = fd, .events = POLLOUT, .revents = 0};
  const int n = ::poll(&p, 1, static_cast<int>(timeout.count()));
  return n > 0 && (p.revents & POLLOUT) != 0;
}

bool WaitReadable(int fd, std::chrono::milliseconds timeout) {
  pollfd p{.fd = fd, .events = POLLIN, .revents = 0};
  const int n = ::poll(&p, 1, static_cast<int>(timeout.count()));
  return n > 0 && (p.revents & POLLIN) != 0;
}

}  // namespace

SyncRedisClient::~SyncRedisClient() { Close(); }

bool SyncRedisClient::Connect(uint16_t port, std::chrono::milliseconds timeout) {
  Close();
  const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return false;

  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int flags = ::fcntl(sock, F_GETFL, 0);
  ::fcntl(sock, F_SETFL, flags | O_NONBLOCK);
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  const int rc = ::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (rc < 0 && errno != EINPROGRESS) {
    ::close(sock);
    return false;
  }
  if (!WaitWritable(sock, timeout)) {
    ::close(sock);
    return false;
  }

  int err = 0;
  socklen_t err_len = sizeof(err);
  if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, &err, &err_len) != 0 || err != 0) {
    ::close(sock);
    return false;
  }

  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int f2 = ::fcntl(sock, F_GETFL, 0);
  ::fcntl(sock, F_SETFL, f2 & ~O_NONBLOCK);
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)

  fd_ = sock;
  return true;
}

void SyncRedisClient::Close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
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

bool SyncRedisClient::SendRaw(const std::string& bytes) {
  if (fd_ < 0) return false;
  size_t off = 0;
  while (off < bytes.size()) {
    const ssize_t n = ::send(fd_, bytes.data() + off, bytes.size() - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

std::string SyncRedisClient::Command(std::initializer_list<std::string> args) {
  if (!SendRaw(Encode(std::vector<std::string>(args)))) return {};
  return ReadSome(4096);
}

std::string SyncRedisClient::ReadSome(size_t n, std::chrono::milliseconds timeout) {
  if (fd_ < 0) return {};
  if (!WaitReadable(fd_, timeout)) return {};
  std::string buf(n, '\0');
  const ssize_t r = ::recv(fd_, buf.data(), buf.size(), 0);
  if (r <= 0) return {};
  buf.resize(static_cast<size_t>(r));
  return buf;
}

}  // namespace abyss::component_test
