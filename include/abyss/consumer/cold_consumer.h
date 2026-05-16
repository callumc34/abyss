#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/flush_strategy.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/metrics/consumer_metrics.h"

namespace abyss::consumer {

class ColdConsumer {
 public:
  struct Config {
    std::chrono::seconds quiet_threshold{30};
    std::chrono::seconds safety_margin{300};
    double jitter_fraction = 0.1;
    size_t buffer_high_water_bytes = 536870912;
    size_t buffer_low_water_bytes = 0;
    size_t max_flush_batch_size = 10000;
    size_t queue_read_max_count = 1024;
    // Used by ReplayUntil(); larger than queue_read_max_count to amortise
    // queue reads while draining a long catch-up backlog.
    size_t replay_batch_size = 50000;
    std::chrono::milliseconds queue_read_timeout{50};
    std::chrono::milliseconds retry_initial_backoff{50};
    std::chrono::milliseconds retry_max_backoff{30000};
    std::chrono::milliseconds block_and_scan_timeout{1000};
    std::optional<uint64_t> rng_seed = std::nullopt;
  };

  enum class Mode : uint8_t { kNormal = 0, kAggressive = 1 };

  struct Metrics {
    size_t buffer_entries = 0;
    size_t buffer_bytes = 0;
    Mode mode = Mode::kNormal;
    std::chrono::milliseconds oldest_unflushed_age{0};
    uint64_t flushes_quiet = 0;
    uint64_t flushes_deadline = 0;
    uint64_t flushes_aggressive = 0;
    uint64_t ops_flushed = 0;
    uint64_t entries_dropped_abs_ttl = 0;
    uint64_t apply_failures = 0;
    uint64_t apply_poisoned = 0;
    uint64_t retry_attempts = 0;
    uint64_t parse_failures = 0;
    uint64_t queue_read_failures = 0;
    core::SequenceId last_ack_seq = 0;
    core::SequenceId latest_drained_seq = 0;
    uint32_t mode_transitions = 0;
  };

  // `eviction_policy` is borrowed; the server owns the single instance and
  // outlives every consumer.
  ColdConsumer(core::Queue& queue, core::ColdStore& cold_store, core::ShardId shard, Config config,
               const core::EvictionPolicy& eviction_policy,
               core::SteadyClockFn steady_clock = core::DefaultSteadyClock,
               core::WallClockFn wall_clock = core::DefaultWallClock);
  ~ColdConsumer();
  ColdConsumer(const ColdConsumer&) = delete;
  ColdConsumer& operator=(const ColdConsumer&) = delete;
  ColdConsumer(ColdConsumer&&) = delete;
  ColdConsumer& operator=(ColdConsumer&&) = delete;

  void Start();

  // Signal the worker to exit. Non-blocking; the thread wakes from its next
  // queue Read (bounded by config.queue_read_timeout) and returns.
  void RequestStop();

  // Wait for the worker thread. Must be preceded by RequestStop.
  void Join();

  // RequestStop + Join. Pools owning many consumers should call the split
  // pair to avoid O(N * queue_read_timeout) serial teardown.
  void Stop();

  // Synchronous replay drive. Drains entries from the queue into the buffer
  // until `target` is reached, then flushes the buffer to the cold store so
  // post-recovery reads do not hit a cold-store-on-disk that lags the WAL.
  // Returns when caught up, cancelled, or on unrecoverable error. Must NOT
  // be called while Start() is running on the same instance.
  core::Result<void> ReplayUntil(core::SequenceId target, const std::atomic<bool>& cancel);

  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

  CompactionBuffer& Buffer() { return buffer_; }
  const CompactionBuffer& Buffer() const { return buffer_; }

  core::ShardId Shard() const { return shard_; }

  // Single-writer on this consumer: must not be called from multiple threads
  // concurrently. Safe to interleave with buffer reads from I/O threads.
  size_t Drain();
  // Replay-mode drain — uses replay_batch_size for amortised reads. ReplayUntil
  // routes through this; steady-state Run() uses Drain().
  size_t DrainWithBatch(size_t max_count);
  bool Flush();
  // Replay variant: pops oldest buffer entries regardless of quiet/deadline
  // timing and flushes them. Steady-state Flush() honours the strategy timers
  // and returns false if no entries are due, which deadlocks a replay loop
  // that has fresh entries with future scheduled_times.
  bool FlushUnscheduled();

  Metrics Snapshot() const;
  Mode CurrentMode() const { return mode_.load(std::memory_order_acquire); }

 private:
  void RunLoop();

  void HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write);
  void HandleConditional(const core::QueueEntry& entry, const core::entry::Conditional& cond);
  void HandleResolved(const core::QueueEntry& entry, const core::entry::Resolved& resolved);

  // Multi-key forms (DEL, MSET) expand into per-key absorbs.
  bool AbsorbResolvedOp(const core::RespCommand& cmd, core::SequenceId seq);

  std::optional<core::SequenceId> OldestPendingConditional() const ABYSS_EXCLUDES(pending_mu_);
  void CheckBlockAndScanTimeout();

  // Reinserts entries on shutdown-during-retry so the next run replays them.
  bool ApplyBatchWithRetry(std::vector<BufferEntry> entries);

  // Shared implementation between Flush() and FlushUnscheduled() — once a
  // batch has been popped from the buffer, the apply path is identical.
  bool ApplyFlushBatch(std::vector<BufferEntry> to_flush, bool aggressive,
                       std::chrono::steady_clock::time_point flush_start);

  std::vector<core::ops::WriteOp> BuildBatchOps(const std::vector<BufferEntry>& entries,
                                                std::vector<core::ops::Del>& del_storage) const;

  bool AbsTtlExpired(const BufferEntry& entry, core::WallTime wall_now) const;
  size_t LowWaterBytes() const;
  void UpdateMode(size_t current_bytes);
  void TryAdvanceAck();

  core::Queue& queue_;
  core::ColdStore& cold_store_;
  core::ShardId shard_;
  Config config_;
  const core::EvictionPolicy& eviction_policy_;
  core::SteadyClockFn steady_clock_;
  core::WallClockFn wall_clock_;
  FlushStrategy strategy_;
  CompactionBuffer buffer_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::thread thread_;

  std::atomic<core::SequenceId> latest_drained_seq_{0};
  std::atomic<core::SequenceId> last_ack_seq_{0};
  bool first_ack_recorded_ = false;

  struct PendingConditional {
    core::SequenceId seq = 0;
    std::chrono::steady_clock::time_point received_at;
  };
  mutable std::mutex pending_mu_;
  std::unordered_map<core::SequenceId, PendingConditional> pending_conditionals_
      ABYSS_GUARDED_BY(pending_mu_);
  bool block_and_scan_warning_emitted_ = false;

  metrics::ConsumerCounters counters_;
  std::atomic<uint64_t> retry_attempts_{0};
  std::atomic<uint64_t> apply_poisoned_{0};
  std::atomic<uint64_t> flushes_quiet_{0};
  std::atomic<uint64_t> flushes_deadline_{0};
  std::atomic<uint64_t> flushes_aggressive_{0};
  std::atomic<uint64_t> ops_flushed_{0};
  std::atomic<uint64_t> entries_dropped_abs_ttl_{0};
  std::atomic<uint32_t> mode_transitions_{0};
  std::atomic<Mode> mode_{Mode::kNormal};
};

}  // namespace abyss::consumer
