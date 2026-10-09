#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/queue.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/hot_replayer.h"
#include "abyss/engine/shard_scheduler.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"

namespace abyss::engine {

struct RecoveryConfig {
  uint32_t replay_parallelism = 4;
  // The hot replayer's clocks, read once, after the Scan.
  HotReplayer::Config replayer;
};

struct RecoverySnapshot {
  enum class Phase : uint8_t {
    kQueueOpen = 0,
    kColdHotReplay = 1,
    kComplete = 2,
  };

  Phase phase = Phase::kQueueOpen;

  uint64_t cold_entries_replayed = 0;
  uint64_t cold_entries_target = 0;

  uint64_t hot_entries_replayed = 0;
  uint64_t hot_entries_target = 0;

  std::chrono::milliseconds elapsed{0};
};

// Drives the recovery state machine described in ADP-007: queue
// self-recovery (complete by the time WalQueue::Open returns), then one
// queue Scan to the durable end that feeds each shard's cold consumer
// and the hot replayer, cold first. Cold then flushes its buffer, and
// the replayer checks it saw every frame and sweeps hot.
//
// Per-shard work is dispatched through a ShardScheduler; the coordinator
// itself is runtime-agnostic. Cancellation propagates via the std::atomic<bool>
// passed to Run() — typically the same flag that backs the SIGTERM handler.
class RecoveryCoordinator {
 public:
  RecoveryCoordinator(core::Queue& queue, consumer::ColdConsumerPool& cold_pool,
                      hot::ShardedHotStore& hot, ShardScheduler& scheduler, RecoveryConfig config);
  ~RecoveryCoordinator() = default;

  RecoveryCoordinator(const RecoveryCoordinator&) = delete;
  RecoveryCoordinator& operator=(const RecoveryCoordinator&) = delete;
  RecoveryCoordinator(RecoveryCoordinator&&) = delete;
  RecoveryCoordinator& operator=(RecoveryCoordinator&&) = delete;

  // Runs all phases sequentially. Blocks until complete, cancelled, or a
  // shard returns a non-recoverable error. After return — success or
  // failure — IsRecovering() returns false so downstream gates (LOADING,
  // /ready) flip in a single observable transition.
  core::Result<void> Run(const std::atomic<bool>& cancel);

  bool IsRecovering() const noexcept {
    return phase_.load(std::memory_order_acquire) != RecoverySnapshot::Phase::kComplete;
  }

  RecoverySnapshot Snapshot() const;

  const HotReplayer& Replayer() const { return replayer_; }

 private:
  core::Result<void> RunColdHotPhase(const std::atomic<bool>& cancel);
  // Rebuilds cold and hot through one queue Scan up to `end`, from
  // `first` for hot and each cold consumer's cursor.
  core::Result<void> ScanColdHot(const std::vector<core::SequenceId>& first,
                                 const std::vector<core::SequenceId>& end,
                                 const std::atomic<bool>& cancel);
  // Makes `shard`'s cold flush through `through`, capped at its power
  // end, for the replayer.
  core::Result<void> DrainCold(core::ShardId shard, core::SequenceId through);

  // Updates phase_ atomic and the phase gauge in lockstep.
  void TransitionPhase(RecoverySnapshot::Phase next);

  // Cold's committed offset per shard; 0 when never committed.
  std::vector<core::SequenceId> CaptureColdOffsets() const;

  core::Queue& queue_;
  consumer::ColdConsumerPool& cold_pool_;
  ShardScheduler& scheduler_;
  RecoveryConfig config_;
  HotReplayer replayer_;
  uint32_t shard_count_;

  std::atomic<RecoverySnapshot::Phase> phase_{RecoverySnapshot::Phase::kQueueOpen};
  std::chrono::steady_clock::time_point started_at_{};

  // Run writes these, and started_at_, under progress_mu_ as each phase
  // opens; Snapshot reads them under it. Run's own reads need no lock.
  mutable std::mutex progress_mu_;
  std::vector<core::SequenceId> cold_starting_;
  std::vector<core::SequenceId> cold_target_;
  uint64_t hot_target_ = 0;

  // Metric handles updated at phase boundaries. Live progress per shard is
  // visible through /status; the gauges deliberately track phase-level totals
  // so a Prometheus query yields stable monotonic curves.
  metrics::GaugeHandle phase_gauge_;
  metrics::GaugeHandle cold_replayed_gauge_;
  metrics::GaugeHandle cold_target_gauge_;
  metrics::GaugeHandle hot_replayed_gauge_;
  metrics::GaugeHandle hot_target_gauge_;
  metrics::GaugeHandle duration_gauge_;
};

}  // namespace abyss::engine
