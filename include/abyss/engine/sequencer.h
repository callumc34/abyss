#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::engine {

struct SequencerConfig {
  // One write's whole budget, every wait in it included.
  std::chrono::milliseconds write_timeout{5000};
  // Each wait for cold to drain before eviction is tried again.
  std::chrono::milliseconds backpressure_wait_slice{10};
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
class Sequencer {
 public:
  Sequencer(hot::ShardedHotStore& hot, core::Queue& queue, Loader& loader,
            consumer::CompactionBufferRouter& buffers, SequencerConfig config);

  // A write, single-key or multi-key: its keys' shards must share a log.
  core::Result<core::RespValue> Execute(core::RespCommand cmd, core::PredicateFlags flags);
  // FLUSHDB: a Flush per shard, every shard wiped under its lock.
  core::Result<core::RespValue> Flush();
  // Waits until each shard's seq is durable at the ack class: a reply
  // that observed it may otherwise show a write not yet published.
  core::Result<void> Fence(std::span<const ShardSeq> fences, core::SteadyTime deadline);

  SequencerStats Snapshot() const;
  const SequencerConfig& config() const { return config_; }

 private:
  class Hold;

  // Off every lock: evicts each shard over its limit to its budget and
  // waits for its cold drain until it is not; OOM at the deadline.
  core::Result<void> AwaitMemory(std::span<const core::ShardId> shards, core::SteadyTime deadline);
  core::Result<void> AwaitDurable(queue::DurableFutures& futures,
                                  const std::vector<queue::ReservedRange>& ranges,
                                  core::SteadyTime deadline, bool flush) const;
  void Redecided(metrics::RedecideReason reason);

  hot::ShardedHotStore& hot_;
  core::Queue& queue_;
  Loader& loader_;
  consumer::CompactionBufferRouter& buffers_;
  SequencerConfig config_;

  std::atomic<uint64_t> holds_{0};
  std::atomic<uint64_t> locked_copy_bytes_{0};
  std::array<std::atomic<uint64_t>, 4> redecides_{};
  std::atomic<uint64_t> backpressure_waits_{0};
  std::atomic<uint64_t> backpressure_rejections_{0};

  metrics::CounterHandle locked_copy_bytes_metric_;
  std::array<metrics::CounterHandle, 4> redecides_metric_;
  metrics::CounterHandle backpressure_waits_metric_;
  metrics::CounterHandle backpressure_rejections_metric_;
  metrics::HistogramHandle lock_hold_;
};

}  // namespace abyss::engine
