#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "abyss/core/result.h"
#include "abyss/platform/types.h"

namespace abyss::net {

using platform::kInvalidSocket;
using platform::Socket;

class Fd {
 public:
  Fd() = default;
  explicit Fd(Socket fd) noexcept : fd_(fd) {}
  ~Fd() noexcept { Reset(); }

  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;

  Fd(Fd&& other) noexcept : fd_(std::exchange(other.fd_, kInvalidSocket)) {}
  Fd& operator=(Fd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, kInvalidSocket);
    }
    return *this;
  }

  Socket Get() const noexcept { return fd_; }
  Socket Release() noexcept { return std::exchange(fd_, kInvalidSocket); }
  void Reset() noexcept;
  bool Valid() const noexcept { return fd_ != kInvalidSocket; }

 private:
  Socket fd_ = kInvalidSocket;
};

struct ListenOptions {
  std::string bind_addr;
  uint16_t port = 0;  // 0 = OS-assigned ephemeral.
  int backlog = 128;
  bool reuse_addr = true;
};

struct ListenResult {
  Fd fd;
  uint16_t bound_port = 0;
};

core::Result<ListenResult> CreateListenSocket(const ListenOptions& opts);

struct AcceptResult {
  Fd fd;
  uint32_t remote_ipv4 = 0;  // network byte order
  uint16_t remote_port = 0;  // network byte order
};

// kUnavailable on EAGAIN/EWOULDBLOCK.
core::Result<AcceptResult> AcceptNonBlocking(Socket listen_fd);

void SetTcpNoDelay(Socket s) noexcept;

std::string FormatIpv4(uint32_t addr_net);

}  // namespace abyss::net
