#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace abyss::testing {

// A latch tests open once. A wait that times out fails the test and
// goes on, so no thread stays parked behind a failed assertion.
class Latch {
 public:
  void Open() {
    {
      const std::scoped_lock lock(mu_);
      open_ = true;
    }
    cv_.notify_all();
  }
  bool Wait(std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
    std::unique_lock lock(mu_);
    if (cv_.wait_for(lock, timeout, [this] { return open_; })) return true;
    ADD_FAILURE() << "latch wait timed out";
    return false;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool open_ = false;
};

}  // namespace abyss::testing
