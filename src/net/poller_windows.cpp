#ifndef _WIN32
#error "poller_windows.cpp built on a non-Windows platform"
#endif

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/net.h"
#include "abyss/platform/types.h"

namespace abyss::net {

namespace {

namespace pnet = abyss::platform::net;

core::Error MakeWsaError(core::ErrorCode code, std::string_view what) {
  std::string msg(what);
  msg += ": ";
  msg += pnet::LastErrorString();
  return {code, std::move(msg)};
}

// Cross-thread wake uses a self-connected loopback socket pair: WSAPoll only
// polls sockets, not events. The pair is created once at poller construction.
core::Result<std::pair<Socket, Socket>> MakeLoopbackPair() {
  Socket listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == kInvalidSocket) {
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "socket(listener)"));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;

  if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
    pnet::CloseSocket(listener);
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "bind(loopback)"));
  }
  if (::listen(listener, 1) == SOCKET_ERROR) {
    pnet::CloseSocket(listener);
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "listen(loopback)"));
  }

  int addr_len = sizeof(addr);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &addr_len) == SOCKET_ERROR) {
    pnet::CloseSocket(listener);
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "getsockname"));
  }

  Socket client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (client == kInvalidSocket) {
    pnet::CloseSocket(listener);
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "socket(client)"));
  }
  if (::connect(client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
    pnet::CloseSocket(listener);
    pnet::CloseSocket(client);
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "connect(loopback)"));
  }

  Socket server = ::accept(listener, nullptr, nullptr);
  pnet::CloseSocket(listener);
  if (server == kInvalidSocket) {
    pnet::CloseSocket(client);
    return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "accept(loopback)"));
  }

  if (auto r = pnet::SetNonBlocking(server); !r) {
    pnet::CloseSocket(client);
    pnet::CloseSocket(server);
    return std::unexpected(r.error());
  }
  if (auto r = pnet::SetNonBlocking(client); !r) {
    pnet::CloseSocket(client);
    pnet::CloseSocket(server);
    return std::unexpected(r.error());
  }

  return std::pair{server, client};
}

short ToPollEvents(EventKind interest) noexcept {
  short ev = 0;
  if (Has(interest, EventKind::kReadable)) ev |= POLLRDNORM;
  if (Has(interest, EventKind::kWritable)) ev |= POLLWRNORM;
  return ev;
}

EventKind FromPollRevents(short revents) noexcept {
  EventKind kinds = EventKind::kNone;
  // POLLERR/POLLHUP surface as readable so the connection sees the EOF/RST.
  if ((revents & (POLLRDNORM | POLLIN | POLLERR | POLLHUP)) != 0) {
    kinds |= EventKind::kReadable;
  }
  if ((revents & (POLLWRNORM | POLLOUT)) != 0) {
    kinds |= EventKind::kWritable;
  }
  return kinds;
}

class WsaPollPoller final : public Poller {
 public:
  static core::Result<std::unique_ptr<WsaPollPoller>> Create() {
    if (auto r = pnet::Init(); !r) return std::unexpected(r.error());
    auto pair = MakeLoopbackPair();
    if (!pair) {
      pnet::Shutdown();
      return std::unexpected(pair.error());
    }
    return std::unique_ptr<WsaPollPoller>(new WsaPollPoller(pair->first, pair->second));
  }

  ~WsaPollPoller() override {
    if (wake_read_ != kInvalidSocket) pnet::CloseSocket(wake_read_);
    if (wake_write_ != kInvalidSocket) pnet::CloseSocket(wake_write_);
    pnet::Shutdown();
  }

  WsaPollPoller(const WsaPollPoller&) = delete;
  WsaPollPoller& operator=(const WsaPollPoller&) = delete;
  WsaPollPoller(WsaPollPoller&&) = delete;
  WsaPollPoller& operator=(WsaPollPoller&&) = delete;

  core::Result<void> Add(Socket fd, EventKind interest, void* user_data) override {
    if (registered_.contains(fd)) {
      return std::unexpected(
          core::Error{core::ErrorCode::kAlreadyExists, "Poller::Add: fd already registered"});
    }
    registered_[fd] = Entry{interest, user_data};
    return {};
  }

  core::Result<void> Modify(Socket fd, EventKind interest, void* user_data) override {
    auto it = registered_.find(fd);
    if (it == registered_.end()) {
      return std::unexpected(
          core::Error{core::ErrorCode::kNotFound, "Poller::Modify: fd not registered"});
    }
    it->second = Entry{interest, user_data};
    return {};
  }

  core::Result<void> Remove(Socket fd) override {
    auto it = registered_.find(fd);
    if (it == registered_.end()) {
      return std::unexpected(
          core::Error{core::ErrorCode::kNotFound, "Poller::Remove: fd not registered"});
    }
    registered_.erase(it);
    return {};
  }

  core::Result<std::span<const Event>> Wait(std::chrono::milliseconds timeout) override {
    fds_.clear();
    fds_.reserve(registered_.size() + 1);

    fds_.push_back(WSAPOLLFD{wake_read_, POLLRDNORM, 0});
    for (const auto& [fd, entry] : registered_) {
      fds_.push_back(WSAPOLLFD{fd, ToPollEvents(entry.interest), 0});
    }

    const int timeout_ms = (timeout.count() < 0) ? -1 : static_cast<int>(timeout.count());
    const int n = ::WSAPoll(fds_.data(), static_cast<ULONG>(fds_.size()), timeout_ms);
    user_events_.clear();
    if (n < 0) {
      const int err = pnet::LastError();
      if (pnet::IsInterrupted(err)) {
        return std::span<const Event>{};
      }
      return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "WSAPoll"));
    }
    if (n == 0) return std::span<const Event>{};

    if (fds_[0].revents != 0) {
      char drain[64];
      while (::recv(wake_read_, drain, sizeof(drain), 0) > 0) {
      }
    }

    user_events_.reserve(static_cast<std::size_t>(n));
    for (std::size_t i = 1; i < fds_.size(); ++i) {
      if (fds_[i].revents == 0) continue;
      auto it = registered_.find(fds_[i].fd);
      if (it == registered_.end()) continue;  // Removed mid-wait.
      user_events_.push_back(Event{
          .user_data = it->second.user_data,
          .kinds = FromPollRevents(fds_[i].revents),
      });
    }
    return std::span<const Event>{user_events_};
  }

  core::Result<void> Wake() override {
    const std::scoped_lock lock(wake_mu_);
    const char one = 1;
    if (::send(wake_write_, &one, 1, 0) == SOCKET_ERROR) {
      const int err = pnet::LastError();
      // Coalesce: if the receive queue is full the wake is already pending.
      if (pnet::IsWouldBlock(err)) return {};
      return std::unexpected(MakeWsaError(core::ErrorCode::kInternal, "wake send"));
    }
    return {};
  }

 private:
  struct Entry {
    EventKind interest;
    void* user_data;
  };

  WsaPollPoller(Socket wake_read, Socket wake_write)
      : wake_read_(wake_read), wake_write_(wake_write) {}

  Socket wake_read_;
  Socket wake_write_;
  std::mutex wake_mu_;  // Serialises wake_write_ across threads.
  std::unordered_map<Socket, Entry> registered_;
  std::vector<WSAPOLLFD> fds_;
  std::vector<Event> user_events_;
};

}  // namespace

core::Result<std::unique_ptr<Poller>> CreatePoller() {
  auto wp = WsaPollPoller::Create();
  if (!wp) return std::unexpected(wp.error());
  return std::unique_ptr<Poller>(std::move(*wp));
}

}  // namespace abyss::net
