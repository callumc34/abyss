#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/flush_strategy.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/metrics/consumer_metrics.h"
#include "abyss/metrics/metrics.h"

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
    // Bounded checkpoint cadence (decision 3): the cold store is fsynced
    // (Checkpoint) at most once every `checkpoint_max_flushes` applied batches
    // or `checkpoint_min_interval`, whichever comes first — never per tiny
    // batch (F_FULLFSYNC is expensive). Tied to the group-commit interval.
    size_t checkpoint_max_flushes = 32;
    std::chrono::milliseconds checkpoint_min_interval{50};
    // Initial/maximum loop backoff when the drain/flush loop makes no progress
    // (idle, poisoned, or unwritable). Capped exponential, reset on progress.
    std::chrono::milliseconds loop_initial_backoff{1};
    std::chrono::milliseconds loop_max_backoff{1000};
    std::optional<uint64_t> rng_seed = std::nullopt;
  };

  enum class Mode : uint8_t { kNormal = 0, kAggressive = 1 };

  // Outcome of one drain+flush iteration; drives the RunLoop's backoff state
  // machine. kProgress resets the backoff; the others grow it (capped).
  enum class FlushOutcome : uint8_t { kProgress, kIdle, kPoisoned, kBackpressure };

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
    // Structurally-undecodable ops that pinned the WAL retention floor below
    // their seq (XERR-5). Distinct from parse_failures (the legacy counter,
    // retained for the empty-cmd / empty-key cases that are not poison).
    uint64_t parse_poison = 0;
    uint64_t queue_read_failures = 0;
    core::SequenceId last_ack_seq = 0;
    core::SequenceId latest_drained_seq = 0;
    uint32_t mode_transitions = 0;
  };

  // `eviction_policy` is borrowed; the server owns the single instance and
  // outlives every consumer.
  ColdConsumer(core::Queue& queue, core::ColdStore& cold_store, core::ShardId shard, Config config,
               const core::EvictionPolicy& eviction_policy, core::ConsumerRpc& rpc,
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

  // Graceful-stop entry point (distinct from the abrupt RequestStop). Sets a
  // draining flag and a deadline so the loop, on exit, drains the buffer to
  // cold, checkpoints (A6), and advances the durable ack before stopping —
  // bounded by `deadline`. Non-blocking; finalised by Join(). Composes with
  // RequestStop: a graceful stop still wakes the loop the same way, but the
  // exit path runs the bounded drain instead of dropping the buffer.
  void RequestStopAndDrain(std::chrono::steady_clock::time_point deadline);

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
  FlushOutcome Flush();
  // Replay variant: pops oldest buffer entries regardless of quiet/deadline
  // timing and flushes them. Steady-state Flush() honours the strategy timers
  // and returns kIdle if no entries are due, which deadlocks a replay loop
  // that has fresh entries with future scheduled_times. `reason` attributes the
  // flush in abyss_cold_flush_reason_total — kPressure for replay drains,
  // kDrain for a graceful-shutdown drain.
  FlushOutcome FlushUnscheduled(metrics::FlushReason reason = metrics::FlushReason::kPressure);

  Metrics Snapshot() const;
  Mode CurrentMode() const { return mode_.load(std::memory_order_acquire); }

  // Highest queue seq this consumer has drained.
  core::SequenceId LatestDrainedSeq() const {
    return latest_drained_seq_.load(std::memory_order_acquire);
  }

  // Blocks until this consumer drains through `target` (true) or `timeout`
  // elapses (false). Signal-driven by the drain loop, not a poll.
  bool WaitForDrainedSeq(core::SequenceId target, std::chrono::milliseconds timeout);

 private:
  void RunLoop();

  // Final drain-to-durable on graceful stop (G6). Flushes the buffer
  // unconditionally (FlushReason::kDrain), then forces a checkpoint + ack so
  // the advanced cold ack is durable — bounded by drain_deadline_. On deadline
  // expiry the remaining buffer is left for WAL replay and the truncation is
  // surfaced (abyss_cold_drain_truncated_total). Runs once, on RunLoop exit.
  void DrainAndFlush();

  // Handlers return the poison seq (the un-materialised WAL seq) when an op is
  // structurally undecodable, std::nullopt otherwise. The drain loop clamps the
  // drained/ack frontier below it so the WAL retains the entry (XERR-5).
  std::optional<core::SequenceId> HandleWrite(const core::QueueEntry& entry,
                                              const core::entry::Write& write);
  void HandleConditional(const core::QueueEntry& entry, const core::entry::Conditional& cond);
  std::optional<core::SequenceId> HandleResolved(const core::QueueEntry& entry,
                                                 const core::entry::Resolved& resolved);
  void HandleFlush(const core::QueueEntry& entry);

  // `wall_now_ms` must be the entry's appended_at so hot and cold materialise
  // identical absolute TTLs from PX/EX args. Returns the seq as poison when the
  // op cannot be parsed into a materialisable WriteOp (XERR-5).
  std::optional<core::SequenceId> AbsorbResolvedOp(const core::RespCommand& cmd,
                                                   core::SequenceId seq, uint64_t wall_now_ms);

  // Records `seq` as poison: increments the metric, logs CRITICAL, and lowers
  // oldest_poison_seq_ so TryAdvanceAck pins the ack below it.
  void RecordPoison(core::SequenceId seq, std::string_view reason);

  std::optional<core::SequenceId> OldestPendingConditional() const ABYSS_EXCLUDES(pending_mu_);
  void CheckBlockAndScanTimeout();

  // Reinserts entries on shutdown-during-retry so the next run replays them.
  // kProgress on apply success, kPoisoned on a terminal error (the batch is
  // reinserted), kBackpressure if a retriable error persisted through stop.
  FlushOutcome ApplyBatchWithRetry(std::vector<BufferEntry> entries,
                                   core::SequenceId highest_wal_seq);

  // Shared implementation between Flush() and FlushUnscheduled() — once a
  // batch has been popped from the buffer, the apply path is identical.
  // `aggressive_reason` attributes a bypass-the-strategy flush (replay or
  // graceful drain) to its FlushReason; scheduled flushes pass nullopt and are
  // attributed quiet/deadline per entry trigger.
  FlushOutcome ApplyFlushBatch(std::vector<BufferEntry> to_flush,
                               std::optional<metrics::FlushReason> aggressive_reason,
                               std::chrono::steady_clock::time_point flush_start);

  // Highest first-seen WAL seq among `entries`; passed to ApplyBatch as the
  // batch's highest_wal_seq. The ack frontier is derived from the live buffer
  // state, not from this.
  static core::SequenceId HighestSeqOf(const std::vector<BufferEntry>& entries);

  // Runs Checkpoint(shard, up_to) when the bounded cadence
  // (checkpoint_max_flushes / checkpoint_min_interval) is due, or when `force`
  // is set (replay/flush drain), recording `up_to` as the durable frontier on
  // success. Returns false if a due checkpoint failed (the ack stays pinned).
  bool MaybeCheckpoint(core::SequenceId up_to, bool force);

  std::vector<core::ops::WriteOp> BuildBatchOps(const std::vector<BufferEntry>& entries,
                                                std::vector<core::ops::Del>& del_storage) const;

  bool AbsTtlExpired(const BufferEntry& entry, core::WallTime wall_now) const;
  size_t LowWaterBytes() const;
  void UpdateMode(size_t current_bytes);
  // `force_checkpoint` bypasses the bounded cadence so the post-recovery /
  // graceful-drain ack is durable-gated even when fewer than the cadence
  // threshold of batches flushed.
  void TryAdvanceAck(bool force_checkpoint = false);
  void NotifyDrained();

  core::Queue& queue_;
  core::ColdStore& cold_store_;
  core::ConsumerRpc& rpc_;
  core::ShardId shard_;
  Config config_;
  const core::EvictionPolicy& eviction_policy_;
  core::SteadyClockFn steady_clock_;
  core::WallClockFn wall_clock_;
  FlushStrategy strategy_;
  CompactionBuffer buffer_;

  std::atomic<bool> stop_requested_{false};
  // Distinct from stop_requested_: when set, RunLoop runs DrainAndFlush on exit
  // (a bounded final flush + checkpoint + ack) instead of dropping the buffer.
  std::atomic<bool> draining_{false};
  std::atomic<bool> running_{false};
  // Deadline for the graceful drain; only read when draining_ is set.
  std::chrono::steady_clock::time_point drain_deadline_{};
  std::thread thread_;
  // Wakes the loop's backoff sleep promptly on RequestStop so teardown is not
  // bounded by the current backoff interval.
  std::mutex stop_mu_;
  std::condition_variable stop_cv_;

  std::atomic<core::SequenceId> latest_drained_seq_{0};
  std::atomic<core::SequenceId> last_ack_seq_{0};
  // Highest seq of an applied `entry::Flush`; gates Resolveds whose ref was wiped.
  std::atomic<core::SequenceId> latest_flush_seq_{0};
  // Highest WAL seq materialised by an ApplyBatch but not yet made durable by a
  // Checkpoint, and the highest seq a successful Checkpoint has made durable.
  // The cold ack target is clamped to last_checkpointed_seq_ so it can never
  // pass data not yet on cold's stable storage (XDUR-1).
  std::atomic<core::SequenceId> highest_applied_uncheckpointed_seq_{0};
  std::atomic<core::SequenceId> last_checkpointed_seq_{0};
  // Cadence bookkeeping for MaybeCheckpoint (single-writer: the loop thread).
  size_t flushes_since_checkpoint_ = 0;
  std::chrono::steady_clock::time_point last_checkpoint_at_{};
  bool first_ack_recorded_ = false;
  // Disambiguates `latest_drained_seq_=0` between "nothing drained" and "drained
  // seq 0"; prevents Ack(0) before any entry has been appended.
  bool drained_anything_ = false;
  std::atomic<uint64_t> flushes_applied_{0};

  // Guards the read-consistency wait.
  std::mutex drain_wait_mu_;
  std::condition_variable drain_wait_cv_;

  struct PendingConditional {
    core::SequenceId seq = 0;
    std::chrono::steady_clock::time_point received_at;
  };
  mutable std::mutex pending_mu_;
  std::unordered_map<core::SequenceId, PendingConditional> pending_conditionals_
      ABYSS_GUARDED_BY(pending_mu_);
  bool block_and_scan_warning_emitted_ = false;

  // Lowest seq of a structurally-undecodable op the cold consumer could not
  // materialise (XERR-5). The ack/drain frontier is pinned below it so the WAL
  // retains the un-materialised entry until operator intervention. kNoPoison
  // (max) means no poison seen this run; set monotonically downward.
  static constexpr core::SequenceId kNoPoison = std::numeric_limits<core::SequenceId>::max();
  std::atomic<core::SequenceId> oldest_poison_seq_{kNoPoison};

  metrics::ConsumerCounters counters_;
  std::atomic<uint64_t> retry_attempts_{0};
  std::atomic<uint64_t> apply_poisoned_{0};
  std::atomic<uint64_t> parse_poison_{0};
  std::atomic<uint64_t> flushes_quiet_{0};
  std::atomic<uint64_t> flushes_deadline_{0};
  std::atomic<uint64_t> flushes_aggressive_{0};
  std::atomic<uint64_t> ops_flushed_{0};
  std::atomic<uint64_t> entries_dropped_abs_ttl_{0};
  std::atomic<uint32_t> mode_transitions_{0};
  std::atomic<Mode> mode_{Mode::kNormal};

  metrics::CounterHandle flush_reason_quiet_;
  metrics::CounterHandle flush_reason_deadline_;
  metrics::CounterHandle flush_reason_pressure_;
  metrics::CounterHandle flush_reason_drain_;
  metrics::CounterHandle drain_truncated_;
  metrics::CounterHandle flush_total_success_;
  metrics::CounterHandle flush_total_failure_;
  metrics::CounterHandle checkpoint_total_success_;
  metrics::CounterHandle checkpoint_total_failure_;
  metrics::HistogramHandle checkpoint_duration_;
  metrics::GaugeHandle checkpoint_interval_;
  metrics::CounterHandle backoff_idle_;
  metrics::CounterHandle backoff_poisoned_;
  metrics::CounterHandle backoff_backpressure_;
  metrics::CounterHandle parse_poison_total_;
  // Updated from the const Snapshot() accessor (observability side-effect only).
  mutable metrics::GaugeHandle flush_heap_depth_;
};

}  // namespace abyss::consumer
