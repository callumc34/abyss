#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>

#include "abyss/core/result.h"
#include "abyss/engine/io_pool.h"
#include "latch.h"

namespace abyss::engine {

// Blocks each cold call for a held key, as HookedColdStore's hook,
// until that key is released; counts every cold call per key.
class ColdKeyGate {
 public:
  void Hold(const std::string& key) {
    const std::scoped_lock lock(mu_);
    held_.insert(key);
  }
  void Release(std::string_view key) {
    {
      const std::scoped_lock lock(mu_);
      if (const auto it = held_.find(key); it != held_.end()) held_.erase(it);
    }
    cv_.notify_all();
  }
  void ReleaseAll() {
    {
      const std::scoped_lock lock(mu_);
      held_.clear();
    }
    cv_.notify_all();
  }
  void Hook(std::string_view key) {
    std::unique_lock lock(mu_);
    ++calls_[std::string(key)];
    if (!held_.contains(key)) return;
    ++blocked_;
    cv_.notify_all();
    cv_.wait_for(lock, std::chrono::seconds{30}, [this, key] { return !held_.contains(key); });
    --blocked_;
  }
  // Until `count` cold calls are blocked here.
  bool AwaitBlocked(size_t count, std::chrono::milliseconds budget = std::chrono::seconds{10}) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, budget, [this, count] { return blocked_ >= count; });
  }
  int Calls(std::string_view key) {
    const std::scoped_lock lock(mu_);
    const auto it = calls_.find(key);
    return it == calls_.end() ? 0 : it->second;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::set<std::string, std::less<>> held_;
  std::map<std::string, int, std::less<>> calls_;
  size_t blocked_ = 0;
};

// Holds a pool thread until released.
class PoolBlocker final : public IoJob {
 public:
  void Run() noexcept override {
    started.Open();
    (void)release.Wait(std::chrono::seconds{30});
    done.Open();
  }
  void Fail(core::Error /*error*/) noexcept override { done.Open(); }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::Latch started;
  abyss::testing::Latch release;
  abyss::testing::Latch done;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

}  // namespace abyss::engine
