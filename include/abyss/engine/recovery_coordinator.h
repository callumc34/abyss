#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer_pool.h"
#include "abyss/consumer/resolver_pool.h"
#include "abyss/core/queue.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/shard_scheduler.h"
#include "abyss/metrics/metrics.h"

namespace abyss::engine {

struct RecoveryConfig {
  uint32_t replay_parallelism = 4;
  size_t hot_replay_batch_size = 10000;
  size_t cold_replay_batch_size = 50000;
  size_t resolver_replay_batch_size = 5000;
};

struct RecoverySnapshot {
  enum class Phase : uint8_t {
    kQueueOpen = 0,
    kResolverReplay = 1,
    kColdHotReplay = 2,
    kComplete = 3,
  };

  Phase phase = Phase::kQueueOpen;

  uint64_t resolver_entries_replayed = 0;
  uint64_t resolver_entries_target = 0;

  uint64_t cold_entries_replayed = 0;
  uint64_t cold_entries_target = 0;

  uint64_t hot_entries_replayed = 0;
  uint64_t hot_entries_target = 0;

  std::chrono::milliseconds elapsed{0};
};

// Drives the recovery state machine described in ADP-007: queue self-recovery
// (already complete by the time WalQueue::Open returns) → resolver replay
// (re-emits any dangling Resolved entries, deterministic per ADP-011) →
// cold + hot replay (parallel, drains each shard to the post-resolver tail).
//
// Per-shard work is dispatched through a ShardScheduler; the coordinator
// itself is runtime-agnostic. Cancellation propagates via the std::atomic<bool>
// passed to Run() — typically the same flag that backs the SIGTERM handler.
class RecoveryCoordinator {
 public:
  RecoveryCoordinator(core::Queue& queue, consumer::ResolverPool& resolver_pool,
                      consumer::ColdConsumerPool& cold_pool, consumer::HotConsumerPool& hot_pool,
                      ShardScheduler& scheduler, RecoveryConfig config);
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

 private:
  core::Result<void> RunResolverPhase(const std::atomic<bool>& cancel);
  core::Result<void> RunColdHotPhase(const std::atomic<bool>& cancel);

  // Updates phase_ atomic and the phase gauge in lockstep.
  void TransitionPhase(RecoverySnapshot::Phase next);

  // Captures TailSeq per shard. Consumers replay until their progress
  // marker reaches the captured tail.
  std::vector<core::SequenceId> CaptureTargets() const;
  // Committed offset per shard; 0 when never committed.
  std::vector<core::SequenceId> CaptureCommittedOffsets(core::ConsumerId consumer) const;
  std::vector<core::SequenceId> CaptureFirstSeqs() const;

  static uint64_t SumDelta(const std::vector<core::SequenceId>& target,
                           const std::vector<core::SequenceId>& starting) noexcept;

  core::Queue& queue_;
  consumer::ResolverPool& resolver_pool_;
  consumer::ColdConsumerPool& cold_pool_;
  consumer::HotConsumerPool& hot_pool_;
  ShardScheduler& scheduler_;
  RecoveryConfig config_;

  std::atomic<RecoverySnapshot::Phase> phase_{RecoverySnapshot::Phase::kQueueOpen};
  std::chrono::steady_clock::time_point started_at_{};

  // Targets and starting offsets for each phase. Set under started_at_ when
  // the phase opens; immutable thereafter, so reads from Snapshot() race-free.
  std::vector<core::SequenceId> resolver_starting_;
  std::vector<core::SequenceId> resolver_target_;
  std::vector<core::SequenceId> cold_starting_;
  std::vector<core::SequenceId> cold_target_;
  std::vector<core::SequenceId> hot_starting_;
  std::vector<core::SequenceId> hot_target_;

  // Metric handles updated at phase boundaries. Live progress per shard is
  // visible through /status; the gauges deliberately track phase-level totals
  // so a Prometheus query yields stable monotonic curves.
  metrics::GaugeHandle phase_gauge_;
  metrics::GaugeHandle resolver_replayed_gauge_;
  metrics::GaugeHandle resolver_target_gauge_;
  metrics::GaugeHandle cold_replayed_gauge_;
  metrics::GaugeHandle cold_target_gauge_;
  metrics::GaugeHandle hot_replayed_gauge_;
  metrics::GaugeHandle hot_target_gauge_;
  metrics::GaugeHandle duration_gauge_;
};

}  // namespace abyss::engine
