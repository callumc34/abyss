#include "abyss/queue/segment_reaper.h"

#include <cstdint>
#include <string_view>
#include <utility>

#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.queue.reaper")

namespace abyss::queue {

namespace {

constexpr std::size_t kSweepBatch = 64;

void NoteUnreaped(SegmentReaper::ReapOutcome& outcome, core::WallTime sealed_at) {
  if (!outcome.oldest_eligible_unreaped.has_value() ||
      sealed_at < *outcome.oldest_eligible_unreaped) {
    outcome.oldest_eligible_unreaped = sealed_at;
  }
}

}  // namespace

SegmentReaper::SegmentReaper(SegmentRegistry& registry, const OffsetStore& offsets,
                             SegmentReaperConfig config)
    : registry_(registry), offsets_(offsets), config_(std::move(config)) {}

core::Result<SegmentReaper::ReapOutcome> SegmentReaper::RunOnce() {
  ReapOutcome outcome;
  if (config_.consumers.empty()) return outcome;
  const auto now = config_.wall_clock();
  for (uint32_t log = 0; log < registry_.LogCount(); ++log) SweepLog(log, now, outcome);
  return outcome;
}

// Reclaiming past a pinned segment would let a rebuild from FirstSeq
// replay a shard's older write across the hole, so a sweep stops at
// the first segment it cannot reclaim. It looks on through that batch
// only to report the oldest segment held back.
void SegmentReaper::SweepLog(uint32_t log, core::WallTime now, ReapOutcome& outcome) {
  bool held = false;
  for (;;) {
    const auto batch = registry_.ListSealedSegments(log, kSweepBatch);
    for (const auto& info : batch) {
      // Later segments were sealed later still.
      if (now - info.sealed_at < config_.min_retention) return;
      const bool released = Released(info);
      if (held) {
        if (released) {
          NoteUnreaped(outcome, info.sealed_at);
          return;
        }
        continue;
      }
      if (!released) {
        held = true;
        continue;
      }
      auto removed = registry_.RemoveSegment(log, info.ordinal);
      if (!removed.has_value()) {
        ++outcome.failed;
        if (!outcome.first_error.has_value()) outcome.first_error = removed.error();
        NoteUnreaped(outcome, info.sealed_at);
        ABYSS_LOG_WARN("segment reclaim failed; the sweep of this log stops",
                       {"log", static_cast<int64_t>(log)}, {"ordinal", info.ordinal},
                       {"err", std::string_view{removed.error().message()}});
        return;
      }
      ABYSS_LOG_DEBUG("segment reclaimed", {"log", static_cast<int64_t>(log)},
                      {"ordinal", info.ordinal});
      ++outcome.deleted;
    }
    if (held || batch.size() < kSweepBatch) return;
  }
}

bool SegmentReaper::Released(const SegmentRegistry::SealedSegmentInfo& info) const {
  for (const auto& range : info.shards) {
    for (auto consumer : config_.consumers) {
      const auto floor = offsets_.ReclaimFloor(consumer, range.shard);
      if (!floor.has_value() || *floor < range.max_seq) return false;
    }
  }
  return true;
}

}  // namespace abyss::queue
