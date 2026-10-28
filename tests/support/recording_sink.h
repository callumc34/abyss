#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <span>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/seq_waiters.h"

namespace abyss::testing {

// Records what is posted to it. A wait that times out fails the test
// and goes on.
class RecordingSink final : public core::CompletionSink {
 public:
  // Notifies under mu_: a waiter that sees the events may destroy the
  // sink as soon as it relocks.
  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    const std::scoped_lock lock(mu_);
    events_.insert(events_.end(), events.begin(), events.end());
    cv_.notify_all();
  }

  bool WaitFor(std::size_t count, std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
    std::unique_lock lock(mu_);
    if (cv_.wait_for(lock, timeout, [&] { return events_.size() >= count; })) return true;
    ADD_FAILURE() << "posted " << events_.size() << " of " << count;
    return false;
  }

  std::vector<core::CompletionEvent> Events() const {
    const std::scoped_lock lock(mu_);
    return events_;
  }

  // A node for `seq` posting here, its slot_seq the seq.
  core::Completion For(core::SequenceId seq) {
    return core::Completion{.sink = this, .client_id = 1, .slot_seq = seq};
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<core::CompletionEvent> events_;
};

}  // namespace abyss::testing
