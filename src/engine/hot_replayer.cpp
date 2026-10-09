#include "abyss/engine/hot_replayer.h"

#include <cstddef>
#include <string>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.engine.replay")

namespace abyss::engine {

HotReplayer::HotReplayer(hot::ShardedHotStore& hot, DrainFn drain, Config config)
    : hot_(hot), drain_(std::move(drain)), config_(std::move(config)), shards_(hot.shard_count()) {
  auto& reg = metrics::Registry::Instance();
  skipped_total_ = reg.Counter(metrics::names::kRecoveryHotSkippedFramesTotal);
  drain_requests_total_ = reg.Counter(metrics::names::kRecoveryColdDrainRequestsTotal);
}

void HotReplayer::Begin(std::span<const core::SequenceId> from,
                        std::span<const core::SequenceId> end) {
  const bool one_per_shard = from.size() == shards_.size() && end.size() == shards_.size();
  ABYSS_DCHECK(one_per_shard, "hot replay expects one range per hot shard");
  for (size_t s = 0; s < shards_.size(); ++s) {
    ShardProgress& shard = shards_[s];
    shard.from.store(from[s], std::memory_order_relaxed);
    shard.next.store(from[s], std::memory_order_release);
    shard.end = end[s];
    shard.stuck = false;
    shard.warned = false;
  }
}

core::Result<void> HotReplayer::Apply(core::ShardId shard, std::vector<core::QueueEntry>& batch) {
  ShardProgress& progress = shards_[shard];
  // Cold has absorbed this batch: a drain may free what the last could
  // not.
  progress.stuck = false;
  for (core::QueueEntry& entry : batch) {
    const core::SequenceId expected = progress.next.load(std::memory_order_relaxed);
    if (entry.seq != expected) {
      return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                         "hot replay of shard " + std::to_string(shard) +
                                             " was handed seq " + std::to_string(entry.seq) +
                                             " at its cursor " + std::to_string(expected)});
    }
    const auto replayed = hot_.Replay(shard, entry);
    progress.next.store(entry.seq + 1, std::memory_order_relaxed);
    if (!replayed.applied) {
      skipped_.fetch_add(1, std::memory_order_relaxed);
      skipped_total_.Increment();
    }
    if (!replayed.over_backpressure) {
      progress.stuck = false;
      progress.warned = false;
      continue;
    }
    hot::ShardedHotStore::ShardMemory memory = replayed.memory;
    if (!progress.stuck) {
      if (auto made = MakeRoom(shard, replayed.shard, entry.seq); !made.has_value()) return made;
      if (!progress.stuck) continue;
      memory = hot_.Memory(replayed.shard);
    }
    if (memory.used_bytes > 2 * memory.limit_bytes) {
      return std::unexpected(core::Error{
          core::ErrorCode::kResourceExhausted,
          "hot replay of shard " + std::to_string(replayed.shard) + " passed twice its " +
              std::to_string(memory.limit_bytes) + "-byte backpressure limit at seq " +
              std::to_string(entry.seq) + " (" + std::to_string(memory.used_bytes) +
              " bytes) with nothing it can evict: cold's drained seq is pinned, as by an " +
              "entry it cannot parse, or one entry is larger than the shard's budget"});
    }
  }
  return {};
}

core::Result<void> HotReplayer::MakeRoom(core::ShardId shard, core::ShardId hot_shard,
                                         core::SequenceId seq) {
  hot_.GcTombstones(hot_shard);
  if (hot_.EvictShardToTarget(hot_shard)) return {};
  // Everything left is newer than cold's drained seq.
  drain_requests_.fetch_add(1, std::memory_order_relaxed);
  drain_requests_total_.Increment();
  if (auto drained = drain_(shard, seq); !drained.has_value()) {
    return std::unexpected(core::Error{
        drained.error().code(),
        "hot replay of shard " + std::to_string(shard) +
            " is over its memory limit and cold could not drain: " + drained.error().message()});
  }
  hot_.GcTombstones(hot_shard);
  if (hot_.EvictShardToTarget(hot_shard)) return {};
  ShardProgress& progress = shards_[shard];
  progress.stuck = true;
  // Once a spell: a shard stuck over many batches would otherwise warn
  // on each one.
  if (!progress.warned) {
    progress.warned = true;
    ABYSS_LOG_WARN("hot replay is over its memory limit with nothing it can evict",
                   {"shard", static_cast<int64_t>(hot_shard)}, {"seq", static_cast<uint64_t>(seq)});
  }
  return {};
}

core::Result<void> HotReplayer::Finish() {
  for (size_t s = 0; s < shards_.size(); ++s) {
    const ShardProgress& shard = shards_[s];
    const core::SequenceId from = shard.from.load(std::memory_order_relaxed);
    const core::SequenceId next = shard.next.load(std::memory_order_relaxed);
    if (next != shard.end) {
      return std::unexpected(core::Error{
          core::ErrorCode::kInternal,
          "hot replay of shard " + std::to_string(s) + " got " + std::to_string(next - from) +
              " of its " + std::to_string(shard.end - from) + " frames from the Scan"});
    }
  }
  // The only clock reads: a link at appended_at t maps to
  // steady_now - (wall_now - t).
  const core::SteadyTime steady_now = config_.steady_clock();
  const core::WallTime wall_now = config_.wall_clock();
  hot_.ShiftReplayedLinks(steady_now, wall_now);
  const auto expired = hot_.EvictExpired(steady_now);
  const size_t reclaimed = hot_.GcTombstones();
  const size_t evicted = hot_.EvictToMemoryTarget();
  ABYSS_LOG_INFO("hot replay complete", {"frames", Replayed()}, {"skipped", Skipped()},
                 {"drain_requests", DrainRequests()},
                 {"evicted_past_eviction", static_cast<uint64_t>(expired.by_deadline)},
                 {"expired", static_cast<uint64_t>(expired.by_ttl)},
                 {"tombstones_reclaimed", static_cast<uint64_t>(reclaimed)},
                 {"evicted_for_memory", static_cast<uint64_t>(evicted)});
  return {};
}

uint64_t HotReplayer::Replayed() const {
  uint64_t total = 0;
  for (const ShardProgress& shard : shards_) {
    // Begin stores from before next, so a next it stored brings it.
    const core::SequenceId next = shard.next.load(std::memory_order_acquire);
    const core::SequenceId from = shard.from.load(std::memory_order_relaxed);
    if (next > from) total += next - from;
  }
  return total;
}

}  // namespace abyss::engine
