#pragma once

#include <functional>
#include <utility>

namespace abyss::testing {

// Runs `fn` when the scope ends, however the test leaves it, so a failed
// ASSERT still releases what other threads are waiting on.
class OnExit {
 public:
  explicit OnExit(std::function<void()> fn) : fn_(std::move(fn)) {}
  // A cleanup that throws in a test may terminate it.
  // NOLINTNEXTLINE(bugprone-exception-escape)
  ~OnExit() {
    if (fn_) fn_();
  }

  OnExit(const OnExit&) = delete;
  OnExit& operator=(const OnExit&) = delete;
  OnExit(OnExit&&) = delete;
  OnExit& operator=(OnExit&&) = delete;

 private:
  std::function<void()> fn_;
};

}  // namespace abyss::testing
