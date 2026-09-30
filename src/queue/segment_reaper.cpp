#include "abyss/queue/segment_reaper.h"

#include <cstdint>
#include <string_view>
#include <utility>

#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.queue.reaper")

namespace abyss::queue {

SegmentReaper::SegmentReaper(SegmentRegistry& registry, const OffsetStore& offsets,
                             SegmentReaperConfig config)
    : registry_(registry), offsets_(offsets), config_(std::move(config)) {}

core::Result<SegmentReaper::ReapOutcome> SegmentReaper::RunOnce() {
  auto sealed = registry_.ListSealedSegments();
  const auto now = config_.wall_clock();

  ReapOutcome outcome;
  for (const auto& info : sealed) {
    if (!ShouldDelete(info, now)) continue;

    auto removed = registry_.RemoveSegment(info.shard, info.base_seq);
    if (!removed.has_value()) {
      ++outcome.failed;
      if (!outcome.first_error.has_value()) outcome.first_error = removed.error();
      if (!outcome.oldest_eligible_unreaped.has_value() ||
          info.created_at < *outcome.oldest_eligible_unreaped) {
        outcome.oldest_eligible_unreaped = info.created_at;
      }
      ABYSS_LOG_WARN("segment removal failed; sweep continues",
                     {"shard", static_cast<int64_t>(info.shard)},
                     {"base_seq", static_cast<uint64_t>(info.base_seq)},
                     {"err", std::string_view{removed.error().message()}});
      continue;
    }
    ABYSS_LOG_DEBUG("segment removed", {"shard", static_cast<int64_t>(info.shard)},
                    {"base_seq", static_cast<uint64_t>(info.base_seq)},
                    {"last_seq", static_cast<uint64_t>(info.last_seq)});
    ++outcome.deleted;
  }
  return outcome;
}

bool SegmentReaper::ShouldDelete(const SegmentRegistry::SealedSegmentInfo& info,
                                 core::WallTime now) const {
  if (config_.consumers.empty()) return false;

  for (auto consumer : config_.consumers) {
    auto ack = offsets_.Get(consumer, info.shard);
    if (!ack.has_value()) return false;
    if (*ack < info.last_seq) return false;
  }

  const auto age = now - info.created_at;
  return age >= config_.min_retention;
}

}  // namespace abyss::queue
