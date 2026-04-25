#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__OpenBSD__) && !defined(__NetBSD__)
#error "poller_kqueue.cpp built on a non-kqueue platform"
#endif

#include <fcntl.h>
#include <sys/event.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <ctime>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
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

constexpr uintptr_t kWakeIdent = 1;

class KqueuePoller final : public Poller {
 public:
  static core::Result<std::unique_ptr<KqueuePoller>> Create() {
    const int kq = ::kqueue();
    if (kq < 0) return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "kqueue"));

    // kqueue() on macOS pre-10.12 doesn't carry FD_CLOEXEC; set it explicitly.
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
    const int fd_flags = ::fcntl(kq, F_GETFD, 0);
    if (fd_flags >= 0) (void)::fcntl(kq, F_SETFD, fd_flags | FD_CLOEXEC);
    // NOLINTEND(cppcoreguidelines-pro-type-vararg)

    auto self = std::unique_ptr<KqueuePoller>(new KqueuePoller(kq));

    // EV_CLEAR coalesces bursts of NOTE_TRIGGER into one delivery.
    struct kevent ev{};
    EV_SET(&ev, kWakeIdent, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "kevent EVFILT_USER add"));
    }
    return self;
  }

  ~KqueuePoller() override {
    if (kq_ >= 0) CloseFd(kq_);
  }

  KqueuePoller(const KqueuePoller&) = delete;
  KqueuePoller& operator=(const KqueuePoller&) = delete;
  KqueuePoller(KqueuePoller&&) = delete;
  KqueuePoller& operator=(KqueuePoller&&) = delete;

  core::Result<void> Add(int fd, EventKind interest, void* user_data) override {
    if (registered_.contains(fd)) {
      return std::unexpected(
          core::Error{core::ErrorCode::kAlreadyExists, "Poller::Add: fd already registered"});
    }
    if (auto r = ApplyChange(fd, EventKind::kNone, interest, user_data); !r) return r;
    registered_[fd] = interest;
    return {};
  }

  core::Result<void> Modify(int fd, EventKind interest, void* user_data) override {
    auto it = registered_.find(fd);
    if (it == registered_.end()) {
      return std::unexpected(
          core::Error{core::ErrorCode::kNotFound, "Poller::Modify: fd not registered"});
    }
    if (auto r = ApplyChange(fd, it->second, interest, user_data); !r) return r;
    it->second = interest;
    return {};
  }

  core::Result<void> Remove(int fd) override {
    auto it = registered_.find(fd);
    if (it == registered_.end()) {
      return std::unexpected(
          core::Error{core::ErrorCode::kNotFound, "Poller::Remove: fd not registered"});
    }
    if (auto r = ApplyChange(fd, it->second, EventKind::kNone, nullptr); !r) return r;
    registered_.erase(it);
    return {};
  }

  core::Result<std::span<const Event>> Wait(std::chrono::milliseconds timeout) override {
    if (raw_events_.size() < kWaitBatch) raw_events_.resize(kWaitBatch);

    timespec ts{};
    ts.tv_sec = static_cast<time_t>(timeout.count() / 1000);
    ts.tv_nsec = static_cast<long>((timeout.count() % 1000) * 1'000'000);
    const timespec* ts_ptr = (timeout.count() < 0) ? nullptr : &ts;

    const int n =
        ::kevent(kq_, nullptr, 0, raw_events_.data(), static_cast<int>(raw_events_.size()), ts_ptr);
    if (n < 0) {
      if (errno == EINTR) {
        user_events_.clear();
        return std::span<const Event>{};
      }
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "kevent wait"));
    }

    user_events_.clear();
    user_events_.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
      const auto& ev = raw_events_[static_cast<size_t>(i)];
      if (ev.filter == EVFILT_USER && ev.ident == kWakeIdent) continue;
      EventKind kinds = EventKind::kNone;
      if (ev.filter == EVFILT_READ) kinds |= EventKind::kReadable;
      if (ev.filter == EVFILT_WRITE) kinds |= EventKind::kWritable;
      user_events_.push_back(Event{.user_data = ev.udata, .kinds = kinds});
    }
    return std::span<const Event>{user_events_};
  }

  core::Result<void> Wake() override {
    struct kevent ev{};
    EV_SET(&ev, kWakeIdent, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    if (::kevent(kq_, &ev, 1, nullptr, 0, nullptr) < 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "kevent EVFILT_USER trigger"));
    }
    return {};
  }

 private:
  static constexpr size_t kWaitBatch = 64;

  explicit KqueuePoller(int kq) : kq_(kq) {}

  core::Result<void> ApplyChange(int fd, EventKind old_interest, EventKind next_interest,
                                 void* user_data) const {
    std::array<struct kevent, 2> changes{};
    int n = 0;

    auto add_change = [&](int filter, uint16_t flags, void* udata) {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
      EV_SET(&changes[n], static_cast<uintptr_t>(fd), filter, flags, 0, 0, udata);
      ++n;
    };

    const bool old_r = Has(old_interest, EventKind::kReadable);
    const bool old_w = Has(old_interest, EventKind::kWritable);
    const bool new_r = Has(next_interest, EventKind::kReadable);
    const bool new_w = Has(next_interest, EventKind::kWritable);

    if (new_r) {
      add_change(EVFILT_READ, EV_ADD | EV_CLEAR, user_data);
    } else if (old_r) {
      add_change(EVFILT_READ, EV_DELETE, nullptr);
    }
    if (new_w) {
      add_change(EVFILT_WRITE, EV_ADD | EV_CLEAR, user_data);
    } else if (old_w) {
      add_change(EVFILT_WRITE, EV_DELETE, nullptr);
    }

    if (n == 0) return {};
    if (::kevent(kq_, changes.data(), n, nullptr, 0, nullptr) < 0) {
      return std::unexpected(MakeErrno(core::ErrorCode::kInternal, "kevent change"));
    }
    return {};
  }

  int kq_ = -1;
  std::unordered_map<int, EventKind> registered_;
  std::vector<struct kevent> raw_events_;
  std::vector<Event> user_events_;
};

}  // namespace

core::Result<std::unique_ptr<Poller>> CreatePoller() {
  auto kp = KqueuePoller::Create();
  if (!kp) return std::unexpected(kp.error());
  return std::unique_ptr<Poller>(std::move(*kp));
}

}  // namespace abyss::net
