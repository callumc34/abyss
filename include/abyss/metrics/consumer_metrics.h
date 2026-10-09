#pragma once

#include <atomic>
#include <cstdint>

namespace abyss::metrics {

struct ConsumerCounters {
  std::atomic<uint64_t> parse_failures{0};
  std::atomic<uint64_t> apply_failures{0};
  std::atomic<uint64_t> queue_read_failures{0};
  std::atomic<uint64_t> commit_failures{0};
};

struct ConsumerSnapshot {
  uint64_t parse_failures = 0;
  uint64_t apply_failures = 0;
  uint64_t queue_read_failures = 0;
  uint64_t commit_failures = 0;
};

inline ConsumerSnapshot SnapshotOf(const ConsumerCounters& c) noexcept {
  return {
      .parse_failures = c.parse_failures.load(std::memory_order_relaxed),
      .apply_failures = c.apply_failures.load(std::memory_order_relaxed),
      .queue_read_failures = c.queue_read_failures.load(std::memory_order_relaxed),
      .commit_failures = c.commit_failures.load(std::memory_order_relaxed),
  };
}

}  // namespace abyss::metrics
