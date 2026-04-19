#pragma once

#include <atomic>
#include <cstdint>

#include "abyss/core/result.h"

namespace abyss::core {

// Drops a Result in a way that names the intent and records failures.
template <typename T>
void FireAndForget(Result<T> result, std::atomic<uint64_t>& failure_counter) noexcept {
  if (!result.has_value()) {
    failure_counter.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace abyss::core
