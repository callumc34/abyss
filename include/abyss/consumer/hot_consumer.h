#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <unordered_map>

#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/metrics/consumer_metrics.h"

namespace abyss::consumer {

// One thread per shard. Conditional entries are held without ack until the
// matching Resolved arrives (block-and-scan, ADP-011); ack is clamped behind
// the oldest pending Conditional.
class HotConsumer {
 public:
  struct Config {
    core::ShardId shard = 0;
    size_t read_batch_size = 256;
    // Bounds Stop() latency; loop wakes at this cadence to check stop flag.
    core::Duration read_timeout{100};
    std::chrono::milliseconds block_and_scan_timeout{1000};
  };

  HotConsumer(core::Queue& queue, core::HotStore& store, core::ConsumerRpc& rpc,
              core::ApplyNotifier& apply_notifier, Config config,
              core::EvictionPolicy eviction_policy);
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

  bool Running() const { return running_.load(std::memory_order_acquire); }
  core::ShardId shard() const { return config_.shard; }

  metrics::ConsumerSnapshot Snapshot() const { return metrics::SnapshotOf(counters_); }

  // Test/diagnostic: count of Conditionals awaiting their matching Resolved.
  size_t PendingConditionalCount() const;

  // Test/diagnostic: highest seq the consumer has fully settled.
  core::SequenceId HighestSettledSeq() const {
    return highest_settled_seq_.load(std::memory_order_acquire);
  }

 private:
  void Run();

  void HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write);
  void HandleConditional(core::QueueEntry entry, const core::entry::Conditional& cond);
  void HandleResolved(const core::QueueEntry& entry, const core::entry::Resolved& resolved);

  core::Result<void> ApplyOps(const std::vector<core::RespCommand>& ops);

  // Acks min(highest_settled, oldest_pending_conditional - 1).
  void MarkSettledAndMaybeAck(core::SequenceId seq);

  void CheckBlockAndScanTimeout();

  core::Queue& queue_;
  core::HotStore& store_;
  core::ConsumerRpc& rpc_;
  core::ApplyNotifier& apply_notifier_;
  Config config_;
  core::EvictionPolicy eviction_policy_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
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
  bool block_and_scan_warning_emitted_ = false;
};

}  // namespace abyss::consumer
