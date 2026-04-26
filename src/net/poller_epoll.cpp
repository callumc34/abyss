#if !defined(__linux__)
#error "poller_epoll.cpp built on a non-Linux platform"
#endif

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"

namespace abyss::net {

namespace {

core::Error MakeErrno(core::ErrorCode code, std::string_view what) {
  std::string msg(what);
  msg += ": ";
  msg += std::strerror(errno);
  return {code, std::move(msg)};
}

uint32_t ToEpollMask(EventKind interest) {
  uint32_t mask = EPOLLET;
  if (Has(interest, EventKind::kReadable)) mask |= EPOLLIN | EPOLLRDHUP;
  if (Has(interest, EventKind::kWritable)) mask |= EPOLLOUT;
  return mask;
}

EventKind FromEpollEvents(uint32_t events) {
  EventKind kinds = EventKind::kNone;
  if ((events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0U) kinds |= EventKind::kReadable;
  if ((events & EPOLLOUT) != 0U) kinds |= EventKind::kWritable;
  return kinds;
}

class EpollPoller final : public Poller {
 public:
  static core::Result<std::unique_ptr<EpollPoller>> Create() {
    const int epfd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "epoll_create1"));

    const int evfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evfd < 0) {
      ::close(epfd);
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "eventfd"));
    }

    auto self = std::unique_ptr<EpollPoller>(new EpollPoller(epfd, evfd));

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = &self->wake_marker_;
    if (::epoll_ctl(epfd, EPOLL_CTL_ADD, evfd, &ev) < 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "epoll_ctl ADD eventfd"));
    }
    return self;
  }

  ~EpollPoller() override {
    if (event_fd_ >= 0) ::close(event_fd_);
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
  }

  EpollPoller(const EpollPoller&) = delete;
  EpollPoller& operator=(const EpollPoller&) = delete;
  EpollPoller(EpollPoller&&) = delete;
  EpollPoller& operator=(EpollPoller&&) = delete;

  core::Result<void> Add(Socket fd, EventKind interest, void* user_data) override {
    epoll_event ev{};
    ev.events = ToEpollMask(interest);
    ev.data.ptr = user_data;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
      const auto code =
          (errno == EEXIST) ? core::ErrorCode::kAlreadyExists : core::ErrorCode::kInternal;
      return std::unexpected(MakeErrno(code, "epoll_ctl ADD"));
    }
    return {};
  }

  core::Result<void> Modify(Socket fd, EventKind interest, void* user_data) override {
    epoll_event ev{};
    ev.events = ToEpollMask(interest);
    ev.data.ptr = user_data;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
      const auto code = (errno == ENOENT) ? core::ErrorCode::kNotFound : core::ErrorCode::kInternal;
      return std::unexpected(MakeErrno(code, "epoll_ctl MOD"));
    }
    return {};
  }

  core::Result<void> Remove(Socket fd) override {
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
      const auto code = (errno == ENOENT) ? core::ErrorCode::kNotFound : core::ErrorCode::kInternal;
      return std::unexpected(MakeErrno(code, "epoll_ctl DEL"));
    }
    return {};
  }

  core::Result<std::span<const Event>> Wait(std::chrono::milliseconds timeout) override {
    if (raw_events_.size() < kWaitBatch) raw_events_.resize(kWaitBatch);

    int timeout_ms = (timeout.count() < 0) ? -1 : static_cast<int>(timeout.count());
    const int n = ::epoll_wait(epoll_fd_, raw_events_.data(), static_cast<int>(raw_events_.size()),
                               timeout_ms);
    if (n < 0) {
      if (errno == EINTR) {
        user_events_.clear();
        return std::span<const Event>{};
      }
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "epoll_wait"));
    }

    user_events_.clear();
    user_events_.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
      const auto& ev = raw_events_[static_cast<size_t>(i)];
      if (ev.data.ptr == &wake_marker_) {
        uint64_t sink = 0;
        while (::read(event_fd_, &sink, sizeof(sink)) > 0) {
        }
        continue;
      }
      user_events_.push_back(Event{
          .user_data = ev.data.ptr,
          .kinds = FromEpollEvents(ev.events),
      });
    }
    return std::span<const Event>{user_events_};
  }

  core::Result<void> Wake() override {
    const uint64_t one = 1;
    ssize_t w = 0;
    do {
      w = ::write(event_fd_, &one, sizeof(one));
    } while (w < 0 && errno == EINTR);
    if (w < 0) {
      // Counter saturated; the wake will be observed next Wait().
      if (errno == EAGAIN) return {};
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "eventfd write"));
    }
    return {};
  }

 private:
  static constexpr size_t kWaitBatch = 64;

  EpollPoller(int epoll_fd, int event_fd) : epoll_fd_(epoll_fd), event_fd_(event_fd) {}

  int epoll_fd_ = -1;
  int event_fd_ = -1;
  // Pointer identity distinguishes wake events from user fds.
  char wake_marker_ = 0;
  std::vector<epoll_event> raw_events_;
  std::vector<Event> user_events_;
};

}  // namespace

core::Result<std::unique_ptr<Poller>> CreatePoller() {
  auto ep = EpollPoller::Create();
  if (!ep) return std::unexpected(ep.error());
  return std::unique_ptr<Poller>(std::move(*ep));
}

}  // namespace abyss::net
