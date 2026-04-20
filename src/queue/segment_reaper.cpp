#include "abyss/queue/segment_reaper.h"

#include <utility>

#include "abyss/log/log.h"

namespace abyss::queue {

namespace {
const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.queue.reaper");
  return l;
}
}  // namespace

SegmentReaper::SegmentReaper(SegmentRegistry& registry, const OffsetStore& offsets,
                             SegmentReaperConfig config)
    : registry_(registry), offsets_(offsets), config_(std::move(config)) {}

core::Result<size_t> SegmentReaper::RunOnce() {
  auto sealed = registry_.ListSealedSegments();
  const auto now = config_.wall_clock();

  size_t deleted = 0;
  for (const auto& info : sealed) {
    if (!ShouldDelete(info, now)) continue;

    auto removed = registry_.RemoveSegment(info.shard, info.base_seq);
    if (!removed.has_value()) {
      return std::unexpected(removed.error());
    }
    ABYSS_LOG_DEBUG(Log(), "segment removed", {"shard", static_cast<int64_t>(info.shard)},
                    {"base_seq", static_cast<uint64_t>(info.base_seq)},
                    {"last_seq", static_cast<uint64_t>(info.last_seq)});
    ++deleted;
  }
  return deleted;
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
