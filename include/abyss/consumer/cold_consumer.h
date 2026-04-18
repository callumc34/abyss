#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/flush_strategy.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

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
    std::chrono::milliseconds queue_read_timeout{50};
    std::chrono::milliseconds retry_initial_backoff{50};
    std::chrono::milliseconds retry_max_backoff{30000};
    std::optional<uint64_t> rng_seed;
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
    uint64_t retry_attempts = 0;
    uint64_t parse_failures = 0;
    core::SequenceId last_ack_seq = 0;
    core::SequenceId latest_drained_seq = 0;
    uint32_t mode_transitions = 0;
  };

  ColdConsumer(core::Queue& queue, core::ColdStore& cold_store, core::ShardId shard, Config config,
               core::EvictionPolicy eviction_policy,
               core::SteadyClockFn steady_clock = core::DefaultSteadyClock,
               core::WallClockFn wall_clock = core::DefaultWallClock);
  ~ColdConsumer();
  ColdConsumer(const ColdConsumer&) = delete;
  ColdConsumer& operator=(const ColdConsumer&) = delete;
  ColdConsumer(ColdConsumer&&) = delete;
  ColdConsumer& operator=(ColdConsumer&&) = delete;

  void Start();
  void Stop();
  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

  CompactionBuffer& Buffer() { return buffer_; }
  const CompactionBuffer& Buffer() const { return buffer_; }

  core::ShardId Shard() const { return shard_; }

  // Single-writer on this consumer: must not be called from multiple threads
  // concurrently. Safe to interleave with buffer reads from I/O threads.
  size_t Drain();
  bool Flush();

  Metrics Snapshot() const;
  Mode CurrentMode() const;

 private:
  void RunLoop(std::atomic<bool>& keep_running);

  bool AbsorbQueueEntry(const core::QueueEntry& entry);

  // On persistent failure (shutdown during retry) entries are Reinserted into
  // the buffer so the next run replays them from the queue.
  bool ApplyBatchWithRetry(std::vector<BufferEntry> entries);

  std::vector<core::ops::WriteOp> BuildBatchOps(const std::vector<BufferEntry>& entries,
                                                std::vector<core::ops::Del>& del_storage) const;

  bool AbsTtlExpired(const BufferEntry& entry, core::WallTime wall_now) const;
  size_t LowWaterBytes() const;
  void UpdateMode(size_t current_bytes) ABYSS_REQUIRES(metrics_mutex_);
  void TryAdvanceAck();

  core::Queue& queue_;
  core::ColdStore& cold_store_;
  core::ShardId shard_;
  Config config_;
  core::EvictionPolicy eviction_policy_;
  core::SteadyClockFn steady_clock_;
  core::WallClockFn wall_clock_;
  FlushStrategy strategy_;
  CompactionBuffer buffer_;

  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
  std::unique_ptr<std::thread> thread_;

  std::atomic<core::SequenceId> latest_drained_seq_{0};
  std::atomic<core::SequenceId> last_ack_seq_{0};

  mutable std::mutex metrics_mutex_;
  Metrics metrics_ ABYSS_GUARDED_BY(metrics_mutex_);
  Mode mode_ ABYSS_GUARDED_BY(metrics_mutex_) = Mode::kNormal;
};

}  // namespace abyss::consumer
