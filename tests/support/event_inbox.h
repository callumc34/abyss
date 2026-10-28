#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/types.h"

namespace abyss::testing {

// A test's sink: events in order. Post notifies under the lock, and a
// driver lets it go only once nothing it waits for is outstanding, so
// no poster is still inside it.
class EventInbox final : public core::CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    const std::scoped_lock lock(mu_);
    events_.insert(events_.end(), events.begin(), events.end());
    cv_.notify_all();
  }

  // The next event; nullopt once `deadline` passes with none.
  std::optional<core::CompletionEvent> Take(std::optional<core::SteadyTime> deadline) {
    std::unique_lock lock(mu_);
    const auto ready = [this] { return next_ < events_.size(); };
    if (!deadline.has_value()) {
      cv_.wait(lock, ready);
    } else if (!cv_.wait_until(lock, *deadline, ready)) {
      return std::nullopt;
    }
    core::CompletionEvent event = std::move(events_[next_++]);
    if (next_ == events_.size()) {
      events_.clear();
      next_ = 0;
    }
    return event;
  }

  size_t Pending() {
    const std::scoped_lock lock(mu_);
    return events_.size() - next_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::vector<core::CompletionEvent> events_;
  size_t next_ = 0;
};

}  // namespace abyss::testing
