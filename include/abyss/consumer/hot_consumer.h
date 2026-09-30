#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <unordered_map>
#include <vector>

#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/metrics/consumer_metrics.h"

namespace abyss::consumer {

// One thread per shard in steady state. Conditional entries are held without
// ack until the matching Resolved arrives (block-and-scan, ADP-011); ack is
// clamped behind the oldest pending Conditional.
//
// During recovery the consumer is driven synchronously through ReplayUntil()
// instead of Start()/Run(). Replay sets replay_mode_, which gates the
// skip-stale checks (ADP-007 invariants 3-4) so they don't fire on fresh
// steady-state writes.
class HotConsumer {
 public:
  struct Config {
    core::ShardId shard = 0;
    size_t read_batch_size = 256;
    // Used by ReplayUntil(); larger than read_batch_size to amortise queue
    // reads while draining a long catch-up backlog.
    size_t replay_batch_size = 10000;
    // Bounds Stop() latency; loop wakes at this cadence to check stop flag.
    core::Duration read_timeout{100};
    std::chrono::milliseconds block_and_scan_timeout{1000};
    core::WallClockFn wall_clock = core::DefaultWallClock;
  };

  HotConsumer(core::Queue& queue, core::HotStore& store, core::ConsumerRpc& rpc,
              core::ApplyNotifier& apply_notifier, Config config,
              const core::EvictionPolicy& eviction_policy);
  ~HotConsumer();

  HotConsumer(const HotConsumer&) = delete;
  HotConsumer& operator=(const HotConsumer&) = delete;
  HotConsumer(HotConsumer&&) = delete;
  HotConsumer& operator=(HotConsumer&&) = delete;

  void Start();

  // Non-blocking; the thread wakes from its next queue Read and returns.
  void RequestStop();
  void Join();

  // Pools should call the RequestStop/Join split to avoid O(N * read_timeout)
  // serial teardown across shards.
  void Stop();

  // Synchronous replay drive: drain entries to `target` (inclusive) using
  // replay_batch_size, applying skip-stale rules. Returns when caught up,
  // cancelled, or on unrecoverable queue error. Must NOT be called while
  // Start() is running on the same instance.
  core::Result<void> ReplayUntil(core::SequenceId target, const std::atomic<bool>& cancel);

  bool Running() const { return running_.load(std::memory_order_acquire); }
  core::ShardId shard() const { return config_.shard; }

  metrics::ConsumerSnapshot Snapshot() const { return metrics::SnapshotOf(counters_); }

  // Test/diagnostic: count of Conditionals awaiting their matching Resolved.
  size_t PendingConditionalCount() const;

  // Highest seq that is BOTH hot-applied AND not behind any unresolved pending
  // Conditional (the settled floor = min(highest_applied, oldest_pending - 1)).
  // Never advances past an undecided Conditional, so the tiering engine's
  // buffer-consistency gate cannot clear ahead of one.
  core::SequenceId HighestSettledSeq() const {
    return settled_floor_.load(std::memory_order_acquire);
  }

 private:
  void Run();
  void ProcessBatch(std::vector<core::QueueEntry>& batch);

  void HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write);
  void HandleConditional(core::QueueEntry entry, const core::entry::Conditional& cond);
  void HandleResolved(const core::QueueEntry& entry, const core::entry::Resolved& resolved);
  void HandleFlush(const core::QueueEntry& entry);

  // Applies materialised ops from a Resolved entry. `reference_at` is the
  // Conditional's appended_at, used for the eviction-window skip-stale check
  // (per ADP-011: Resolved takes effect at the Conditional's seq position).
  // `seq` is the Resolved entry's queue seq — the position the cold consumer
  // drains the materialised ops at, so a delete's tombstone is keyed to it.
  core::Result<void> ApplyResolvedOps(const std::vector<core::RespCommand>& ops,
                                      core::WallTime reference_at, core::SequenceId seq);

  // Skip-stale gates. Both consult the entry's wall-clock appended_at and
  // the parsed op's `abs_ttl_ms` (when present). Active only in replay mode.
  bool ShouldSkipForEvictionElapsed(core::WallTime appended_at, std::string_view key,
                                    core::WallTime wall_now) const;
  static bool ShouldSkipForAbsTtlElapsed(uint64_t abs_ttl_ms, core::WallTime wall_now);

  // Acks min(highest_settled, oldest_pending_conditional - 1).
  void MarkSettledAndMaybeAck(core::SequenceId seq);

  void CheckBlockAndScanTimeout();

  core::Queue& queue_;
  core::HotStore& store_;
  core::ConsumerRpc& rpc_;
  core::ApplyNotifier& apply_notifier_;
  Config config_;
  // Borrowed; the server owns the single EvictionPolicy instance and outlives
  // every consumer that references it.
  const core::EvictionPolicy& eviction_policy_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  // True only while ReplayUntil() is executing on this consumer. Gates the
  // skip-stale checks so steady-state writes through Run() apply normally.
  std::atomic<bool> replay_mode_{false};
  std::thread thread_;

  metrics::ConsumerCounters counters_;

  struct PendingConditional {
    core::QueueEntry entry;
    std::chrono::steady_clock::time_point received_at;
  };
  mutable std::mutex pending_mu_;
  std::unordered_map<core::SequenceId, PendingConditional> pending_conditionals_
      ABYSS_GUARDED_BY(pending_mu_);

  std::atomic<core::SequenceId> highest_settled_seq_{0};
  // The settled floor published to HotConsumerProgress: clamped behind the
  // oldest pending Conditional so it never exceeds an unresolved one.
  std::atomic<core::SequenceId> settled_floor_{0};
  // Highest seq of an applied `entry::Flush`; no-ops Resolveds whose Conditional was wiped.
  std::atomic<core::SequenceId> latest_flush_seq_{0};
  bool block_and_scan_warning_emitted_ = false;
};

}  // namespace abyss::consumer
