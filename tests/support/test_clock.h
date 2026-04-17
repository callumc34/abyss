#pragma once

#include <chrono>
#include <mutex>

#include "abyss/core/types.h"

namespace abyss::testing {

class TestClock {
 public:
  explicit TestClock(core::SteadyTime initial = core::SteadyTime{std::chrono::seconds{1000000}})
      : steady_(initial),
        wall_(core::WallTime{
            std::chrono::duration_cast<core::WallClock::duration>(initial.time_since_epoch())}) {}

  core::SteadyTime SteadyNow() const {
    const std::lock_guard lock(mu_);
    return steady_;
  }

  core::WallTime WallNow() const {
    const std::lock_guard lock(mu_);
    return wall_;
  }

  void Advance(std::chrono::milliseconds delta) {
    const std::lock_guard lock(mu_);
    steady_ += delta;
    wall_ += delta;
  }

  void Set(core::SteadyTime t) {
    const std::lock_guard lock(mu_);
    steady_ = t;
  }

  void SetWall(core::WallTime t) {
    const std::lock_guard lock(mu_);
    wall_ = t;
  }

  core::SteadyClockFn SteadyFn() {
    return [this]() { return SteadyNow(); };
  }

  core::WallClockFn WallFn() {
    return [this]() { return WallNow(); };
  }

 private:
  mutable std::mutex mu_;
  core::SteadyTime steady_;
  core::WallTime wall_;
};

}  // namespace abyss::testing
