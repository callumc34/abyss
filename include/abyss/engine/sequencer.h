#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/predicate.h"
#include "abyss/core/queue.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/decide.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/pending_write.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::engine {

struct SequencerConfig {
  // One write's whole budget, every wait in it included.
  std::chrono::milliseconds write_timeout{5000};
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

struct SequencerStats {
  uint64_t locked_copy_bytes = 0;
  // By metrics::RedecideReason.
  std::array<uint64_t, 4> redecides{};
  uint64_t backpressure_waits = 0;
  uint64_t backpressure_rejections = 0;
};

// Every write's path (ADP-015 §Sequenced write path): decide against
// hot's complete state under the exclusive locks of the keys' shards,
// reserve the effects in the log, apply them to hot, unlock, then fill,
// publish and wait until durable at the ack class.
//
// A write runs as attempts, each on its owner's thread. An attempt
// that must wait registers itself first, then re-checks; it returns
// only once registered, and its wake runs the next attempt. A write
// whose deadline passes before it is linearised is never applied.
class Sequencer {
 public:
  Sequencer(hot::ShardedHotStore& hot, core::Queue& queue, Loader& loader,
            consumer::CompactionBufferRouter& buffers, SequencerConfig config);

  // The write, single-key or multi-key (its keys' shards must share a
  // log), or FLUSHDB (a Flush per shard, every shard wiped under its
  // lock), until it replies or must wait.
  Step Attempt(PendingWrite& write);
  // A write's own loads, the wait kLoads names; on any thread.
  void Load(PendingWrite& write);
  // At its deadline, for a write still waiting: true if its wait was
  // cancelled, and the next Attempt replies with the timeout. False:
  // its wake is on the way, and the Attempt that follows replies.
  bool Expire(PendingWrite& write);
  // Waits until each shard's seq is durable at the ack class: a reply
  // that observed it may otherwise show a write not yet published. A 0,
  // loaded state's, is below every durable end, so returns at once.
  core::Result<void> Fence(std::span<const ShardSeq> fences, core::SteadyTime deadline);

  SequencerStats Snapshot() const;
  const SequencerConfig& config() const { return config_; }

 private:
  class Hold;
  class Phases;

  Step WriteAttempt(PendingWrite& write);
  Step FlushAttempt(PendingWrite& write);
  void Start(PendingWrite& write);
  // Each of these is a wait site: nullopt once its wait is over, or
  // the step to return.
  std::optional<Step> AwaitAdmission(PendingWrite& write);
  std::optional<Step> AwaitSpares(PendingWrite& write);
  // Off every lock: evicts each shard over its limit to its budget and
  // waits for its cold drain until it is not; OOM at the deadline.
  std::optional<Step> AwaitMemory(PendingWrite& write);
  std::optional<Step> AwaitForeignLoads(PendingWrite& write);
  // Each of the write's durable seqs at the ack class: its ranges' last,
  // or its fences.
  Step AwaitDurable(PendingWrite& write);
  // Registered, to wait for `reason`.
  static Step Waiting(PendingWrite& write, WaitReason reason, core::ShardId shard = 0);
  Step Done(PendingWrite& write, core::Result<core::RespValue> reply);
  // A write past its deadline, not yet linearised: ends its wait and
  // its loads, and replies with the wait's timeout.
  Step Abandon(PendingWrite& write);
  void AbortLoads(PendingWrite& write);
  void InstallLoads(PendingWrite& write);
  static void ClearLoads(PendingWrite& write);
  Step DurableTimedOut(PendingWrite& write);
  Step DurableFailed(PendingWrite& write, const core::Error& error);
  void Redecided(metrics::RedecideReason reason);
  void Again(PendingWrite& write, metrics::RedecideReason reason);

  hot::ShardedHotStore& hot_;
  core::Queue& queue_;
  Loader& loader_;
  consumer::CompactionBufferRouter& buffers_;
  SequencerConfig config_;

  std::atomic<uint64_t> locked_copy_bytes_{0};
  std::array<std::atomic<uint64_t>, 4> redecides_{};
  std::atomic<uint64_t> backpressure_waits_{0};
  std::atomic<uint64_t> backpressure_rejections_{0};

  metrics::CounterHandle locked_copy_bytes_metric_;
  std::array<metrics::CounterHandle, 4> redecides_metric_;
  metrics::CounterHandle backpressure_waits_metric_;
  metrics::CounterHandle backpressure_rejections_metric_;
  metrics::HistogramHandle lock_hold_;
  std::array<metrics::HistogramHandle, metrics::kWritePhaseCount> phase_seconds_;
};

}  // namespace abyss::engine
