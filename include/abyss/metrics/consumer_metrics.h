#pragma once

#include <atomic>
#include <cstdint>

namespace abyss::metrics {

struct ConsumerCounters {
  std::atomic<uint64_t> applied{0};
  std::atomic<uint64_t> parse_failures{0};
  std::atomic<uint64_t> apply_failures{0};
  std::atomic<uint64_t> queue_read_failures{0};
  std::atomic<uint64_t> ack_failures{0};
  std::atomic<uint64_t> block_and_scan_timeouts{0};
  // Replay-only: increments when a hot replay entry is dropped because the
  // wall-clock eviction window from `appended_at` has elapsed. Per ADP-007,
  // such keys re-enter hot via cold-hit promotion on first read.
  std::atomic<uint64_t> replay_skipped_eviction{0};
  // Replay-only: increments when a hot replay entry's parsed op carries an
  // absolute TTL (`SET ... EX/PX/EXAT/PXAT`, `EXPIRE...`) whose deadline has
  // already passed. The end state matches what lazy expiry would produce.
  std::atomic<uint64_t> replay_skipped_abs_ttl{0};
};

struct ConsumerSnapshot {
  uint64_t applied = 0;
  uint64_t parse_failures = 0;
  uint64_t apply_failures = 0;
  uint64_t queue_read_failures = 0;
  uint64_t ack_failures = 0;
  uint64_t block_and_scan_timeouts = 0;
  uint64_t replay_skipped_eviction = 0;
  uint64_t replay_skipped_abs_ttl = 0;
};

inline ConsumerSnapshot SnapshotOf(const ConsumerCounters& c) noexcept {
  return {
      .applied = c.applied.load(std::memory_order_relaxed),
      .parse_failures = c.parse_failures.load(std::memory_order_relaxed),
      .apply_failures = c.apply_failures.load(std::memory_order_relaxed),
      .queue_read_failures = c.queue_read_failures.load(std::memory_order_relaxed),
      .ack_failures = c.ack_failures.load(std::memory_order_relaxed),
      .block_and_scan_timeouts = c.block_and_scan_timeouts.load(std::memory_order_relaxed),
      .replay_skipped_eviction = c.replay_skipped_eviction.load(std::memory_order_relaxed),
      .replay_skipped_abs_ttl = c.replay_skipped_abs_ttl.load(std::memory_order_relaxed),
  };
}

}  // namespace abyss::metrics
