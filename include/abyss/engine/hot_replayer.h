#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"

namespace abyss::engine {

// Rebuilds hot from recovery's one queue Scan; nothing else feeds it.
// Frames apply through ApplyEffects as the sequencer applied them, and
// the residency rule (ShardedHotStore::Replay) keeps hot from building
// a key out of part of its history. Replay reads no clock: written
// keys link at their appended_at until Finish maps them to the steady
// clock and sweeps.
//
// In the engine, not hot: making room may need cold, and hot sees cold
// only through its drained seq.
class HotReplayer {
 public:
  // Makes cold flush its buffer through `through`, so hot can evict;
  // an error fails recovery.
  using DrainFn = std::function<core::Result<void>(core::ShardId shard, core::SequenceId through)>;

  struct Config {
    core::SteadyClockFn steady_clock = core::DefaultSteadyClock;
    core::WallClockFn wall_clock = core::DefaultWallClock;
  };

  HotReplayer(hot::ShardedHotStore& hot, DrainFn drain, Config config);
  HotReplayer(hot::ShardedHotStore& hot, DrainFn drain)
      : HotReplayer(hot, std::move(drain), Config{}) {}
  ~HotReplayer() = default;
  HotReplayer(const HotReplayer&) = delete;
  HotReplayer& operator=(const HotReplayer&) = delete;
  HotReplayer(HotReplayer&&) = delete;
  HotReplayer& operator=(HotReplayer&&) = delete;

  // Expects each shard's frames [from[s], end[s]), in seq order;
  // one element per hot shard.
  void Begin(std::span<const core::SequenceId> from, std::span<const core::SequenceId> end);
  // Applies or skips each of `shard`'s frames in `batch`, which must
  // continue where the last one ended; applied payloads are moved out.
  // At its backpressure limit with nothing evictable, the shard's cold
  // is made to flush through the frame, then hot evicts again, once a
  // batch. Fails once the shard passes twice that limit.
  core::Result<void> Apply(core::ShardId shard, std::vector<core::QueueEntry>& batch);
  // After cold's FinishReplay on every shard: fails unless every shard
  // got exactly its frames, then reads the clocks once to map replay's
  // links and evicts keys past their eviction, then down to the budget.
  core::Result<void> Finish();

  // Frames applied or skipped so far, all shards.
  uint64_t Replayed() const;
  uint64_t Skipped() const { return skipped_.load(std::memory_order_relaxed); }
  uint64_t DrainRequests() const { return drain_requests_.load(std::memory_order_relaxed); }

 private:
  struct ShardProgress {
    std::atomic<core::SequenceId> from{0};
    // The next seq expected; written by the shard's Scan worker only.
    std::atomic<core::SequenceId> next{0};
    core::SequenceId end = 0;
    // Stayed over after a drain, until a frame finds it back under or
    // the next batch.
    bool stuck = false;
    // Warned for this spell over the limit; a frame back under ends it.
    bool warned = false;
  };

  // `hot_shard` is at its backpressure limit after `seq` of stream
  // `shard`: evict, else drain the stream's cold then evict; still
  // over, the shard is stuck.
  core::Result<void> MakeRoom(core::ShardId shard, core::ShardId hot_shard, core::SequenceId seq);

  hot::ShardedHotStore& hot_;
  DrainFn drain_;
  Config config_;
  // One per hot shard, never resized: its atomics cannot move.
  std::vector<ShardProgress> shards_;
  std::atomic<uint64_t> skipped_{0};
  std::atomic<uint64_t> drain_requests_{0};
  metrics::CounterHandle skipped_total_;
  metrics::CounterHandle drain_requests_total_;
};

}  // namespace abyss::engine
