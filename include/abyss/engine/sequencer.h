#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/completion.h"
#include "abyss/core/durable_wait.h"
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
  // A command's whole budget, every wait in it included: reads take
  // it as well as writes.
  std::chrono::milliseconds command_timeout{5000};
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
class Sequencer final : private core::DurableWait::Timer {
 public:
  Sequencer(hot::ShardedHotStore& hot, core::Queue& queue, Loader& loader,
            consumer::CompactionBufferRouter& buffers, SequencerConfig config);

  // The write, single-key or multi-key (its keys' shards must share a
  // log), or FLUSHDB (a Flush per shard, every shard wiped under its
  // lock), until it replies, is linearised, or must wait.
  Step Attempt(PendingWrite& write);
  // A write's own loads, the wait kLoads names; on any thread.
  void Load(PendingWrite& write);
  // Instead of Load, for loads that will not run (`error`): ends each
  // one's placeholder, so writers and readers of its key go on. Any
  // error but the deadline's is the write's reply.
  void Unload(PendingWrite& write, const core::Error& error);
  // For a write that returned kSequenced: hands its reply and the seqs
  // it shows to `wait`, whose posts go to `completion`. The reply now,
  // if all of them are durable already.
  std::optional<core::Result<core::RespValue>> Sequence(PendingWrite& write,
                                                        core::DurableWait& wait,
                                                        core::Completion completion);
  // At its deadline, for a write that returned kWait: its timeout
  // reply, and it is never applied. Its wait is cancelled if it can be;
  // otherwise its wake or its loads are on their way, and the Attempt
  // they run only cleans up: FinishWait's hand-off, and installs of the
  // loads it took whose tokens still allow them.
  core::Result<core::RespValue> Abandon(PendingWrite& write);

  SequencerStats Snapshot() const;
  const SequencerConfig& config() const { return config_; }
  core::Queue& queue() const { return queue_; }

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
  static Step Linearised(PendingWrite& write);
  // Registered, to wait for `reason`.
  static Step Waiting(PendingWrite& write, WaitReason reason, core::ShardId shard = 0);
  Step Done(PendingWrite& write, core::Result<core::RespValue> reply);
  // A write past its deadline, not yet linearised: ends its wait and
  // its loads, and replies with the wait's timeout, unless Abandon
  // already has.
  Step Expired(PendingWrite& write);
  // Ends the wait `wake` answered, if any, and the loads.
  void EndWait(PendingWrite& write, const std::optional<core::Result<void>>& wake);
  // The wait's timeout reply, counted as its rejection.
  core::Result<core::RespValue> TimeoutOf(PendingWrite& write,
                                          const std::optional<core::Result<void>>& wake);
  void AbortLoads(PendingWrite& write);
  void InstallLoads(PendingWrite& write);
  static void ClearLoads(PendingWrite& write);
  void Redecided(metrics::RedecideReason reason);
  void Again(PendingWrite& write, metrics::RedecideReason reason);
  void Waited(std::chrono::steady_clock::duration waited) noexcept override;

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
