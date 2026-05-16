#include "abyss/consumer/compaction_buffer.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include "abyss/core/resp_format.h"
#include "abyss/core/thread_annotations.h"

namespace abyss::consumer {

namespace {

// Approximate per-entry overhead.
constexpr size_t kEntryOverhead = 128;

size_t EntryBytes(const BufferEntry& entry) {
  return entry.key.size() + kEntryOverhead + entry.state.EstimatedBytes();
}

}  // namespace

CompactionBuffer::CompactionBuffer(FlushStrategy strategy, core::SteadyClockFn clock,
                                   std::optional<uint64_t> rng_seed, core::WallClockFn wall_clock)
    : strategy_(strategy),
      clock_(std::move(clock)),
      wall_clock_(std::move(wall_clock)),
      rng_(rng_seed.value_or(std::random_device{}())) {}

CompactionBuffer::CompactionBuffer(core::SteadyClockFn clock, core::WallClockFn wall_clock)
    : CompactionBuffer(FlushStrategy{}, std::move(clock), std::nullopt, std::move(wall_clock)) {}

void CompactionBuffer::Absorb(const std::string& key, const core::ops::WriteOp& op,
                              core::EvictionTTL eviction,
                              core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  auto& entry = entries_[key];
  bool is_new = entry.key.empty();

  size_t old_entry_bytes = is_new ? 0 : EntryBytes(entry);

  if (is_new) {
    entry.key = key;
    entry.first_seen = clock_();
    entry.jitter_offset = ComputeJitter();
    entry.first_seen_seq = seq;
  }

  entry.eviction = eviction;
  entry.state.Absorb(op);
  entry.last_modified = clock_();
  ++entry.write_count;

  bytes_estimate_ -= old_entry_bytes;
  bytes_estimate_ += EntryBytes(entry);

  auto next = strategy_.NextFlushTime(entry, entry.eviction);
  entry.last_trigger = next.trigger;
  const auto scheduled = next.time + entry.jitter_offset;
  flush_heap_.push({scheduled, key});
}

namespace {

constexpr std::string_view kBufferMissMsg = "buffer has no state for key";

}  // namespace

core::Result<core::RespValue> CompactionBuffer::Exec(const core::ops::ReadOp& op) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);

  // Lazy abs-TTL expiry mirrors the hot-store read path: an entry whose
  // absolute TTL has elapsed must surface as a miss/null even before the
  // cold consumer flushes a tombstone.
  const uint64_t now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_clock_().time_since_epoch())
          .count());
  auto is_expired = [&](const CompactedState& state) {
    const uint64_t ttl = state.AbsTtlMs();
    return ttl > 0 && ttl <= now_ms;
  };

  return std::visit(
      [&](const auto& read) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(read)>;

        if constexpr (std::is_same_v<T, core::ops::Exists>) {
          int64_t count = 0;
          for (auto key : read.keys) {
            auto it = entries_.find(std::string(key));
            if (it == entries_.end()) continue;
            if (it->second.state.IsTombstone()) continue;
            if (is_expired(it->second.state)) continue;
            ++count;
          }
          return core::RespValue::Integer(count);
        }

        auto key = core::ops::PrimaryKey(core::ops::ReadOp{read});
        auto it = entries_.find(std::string(key));
        if (it == entries_.end()) {
          return std::unexpected(
              core::Error(core::ErrorCode::kNotFound, std::string{kBufferMissMsg}));
        }
        const auto& state = it->second.state;

        if (state.IsTombstone() || is_expired(state)) return core::RespValue::Null();

        if constexpr (std::is_same_v<T, core::ops::StringGet>) {
          if (state.Type() != CompactedState::DataType::kString) {
            // Defer to cold for the canonical WRONGTYPE against live state.
            return std::unexpected(
                core::Error(core::ErrorCode::kNotFound, "buffer key is not a string"));
          }
          return core::RespValue::BulkString(state.StringValue());
        } else if constexpr (std::is_same_v<T, core::ops::HashGet>) {
          auto value = state.HashFieldValue(read.field);
          if (!value.has_value()) return core::RespValue::Null();
          return core::RespValue::BulkString(*value);
        } else if constexpr (std::is_same_v<T, core::ops::HashGetAll> ||
                             std::is_same_v<T, core::ops::HashKeys> ||
                             std::is_same_v<T, core::ops::HashVals> ||
                             std::is_same_v<T, core::ops::HashLen> ||
                             std::is_same_v<T, core::ops::HashMultiGet> ||
                             std::is_same_v<T, core::ops::HashFieldExists>) {
          // Multi-field hash reads are answered by the engine merging this
          // overlay with cold's full state. The buffer alone never has the
          // complete picture for these ops.
          return std::unexpected(core::Error(core::ErrorCode::kNotFound,
                                             "buffer defers multi-field hash reads to engine"));
        } else if constexpr (std::is_same_v<T, core::ops::SetIsMember>) {
          if (state.Type() != CompactedState::DataType::kSet) {
            return std::unexpected(
                core::Error(core::ErrorCode::kWrongType,
                            "Operation against a key holding the wrong kind of value"));
          }
          return core::RespValue::Integer(state.SetHasMember(read.member) ? 1 : 0);
        } else if constexpr (std::is_same_v<T, core::ops::SetMembers>) {
          return std::unexpected(core::Error(core::ErrorCode::kNotFound,
                                             "buffer defers full-collection reads to cold"));
        } else if constexpr (std::is_same_v<T, core::ops::SetCard>) {
          if (state.Type() != CompactedState::DataType::kSet) {
            return std::unexpected(
                core::Error(core::ErrorCode::kWrongType,
                            "Operation against a key holding the wrong kind of value"));
          }
          return core::RespValue::Integer(static_cast<int64_t>(state.SetCardinality()));
        } else if constexpr (std::is_same_v<T, core::ops::ZsetScore>) {
          if (state.Type() != CompactedState::DataType::kZset) {
            return std::unexpected(
                core::Error(core::ErrorCode::kWrongType,
                            "Operation against a key holding the wrong kind of value"));
          }
          auto score = state.ZsetMemberScore(read.member);
          if (!score.has_value()) return core::RespValue::Null();
          return core::RespValue::BulkString(core::FormatRespDouble(*score));
        } else if constexpr (std::is_same_v<T, core::ops::ZsetCard>) {
          if (state.Type() != CompactedState::DataType::kZset) {
            return std::unexpected(
                core::Error(core::ErrorCode::kWrongType,
                            "Operation against a key holding the wrong kind of value"));
          }
          return core::RespValue::Integer(static_cast<int64_t>(state.ZsetCardinality()));
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRange>) {
          // Range needs cold's score-index; buffer's hash view can't iterate.
          return std::unexpected(
              core::Error(core::ErrorCode::kNotFound, "buffer defers ZRANGE to cold"));
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported buffer read op"));
        }
      },
      op);
}

core::Result<core::RespValue> CompactionBuffer::Read(const std::string& key) const {
  return Exec(core::ops::ReadOp{core::ops::StringGet{.key = key}});
}

BufferKeyPresence CompactionBuffer::Probe(std::string_view key) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  const auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return BufferKeyPresence::kAbsent;

  const auto& state = it->second.state;
  const uint64_t now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_clock_().time_since_epoch())
          .count());
  const uint64_t ttl = state.AbsTtlMs();
  const bool ttl_expired = ttl > 0 && ttl <= now_ms;
  if (state.IsTombstone() || ttl_expired) return BufferKeyPresence::kTombstoned;
  if (state.Type() == CompactedState::DataType::kNone) return BufferKeyPresence::kAbsent;
  return BufferKeyPresence::kPresent;
}

HashOverlay CompactionBuffer::HashOverlayFor(std::string_view key) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return HashOverlay{.kind = HashOverlay::Kind::kNotPresent};

  const auto& state = it->second.state;

  const uint64_t now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_clock_().time_since_epoch())
          .count());
  const uint64_t ttl = state.AbsTtlMs();
  const bool ttl_expired = ttl > 0 && ttl <= now_ms;

  if (state.IsTombstone() || ttl_expired) {
    return HashOverlay{.kind = HashOverlay::Kind::kTombstone};
  }
  if (state.Type() != CompactedState::DataType::kHash) {
    // kNone falls through as "no useful overlay" rather than wrong-type:
    // None means the buffer's state has been cleared without a tombstone,
    // which is not a typed conflict for the caller.
    if (state.Type() == CompactedState::DataType::kNone) {
      return HashOverlay{.kind = HashOverlay::Kind::kNotPresent};
    }
    return HashOverlay{.kind = HashOverlay::Kind::kWrongType};
  }
  return HashOverlay{
      .kind = HashOverlay::Kind::kHash,
      .fields = state.HashFields(),
      .removed_fields = state.HashRemovedFields(),
  };
}

std::vector<BufferEntry> CompactionBuffer::FlushReady(core::SteadyTime now, size_t max_count)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  std::vector<BufferEntry> result;

  while (!flush_heap_.empty() && flush_heap_.top().scheduled_time <= now &&
         result.size() < max_count) {
    auto heap_time = flush_heap_.top().scheduled_time;
    auto heap_key = flush_heap_.top().key;
    flush_heap_.pop();

    auto it = entries_.find(heap_key);
    if (it == entries_.end()) continue;

    auto& entry = it->second;
    const auto next = strategy_.NextFlushTime(entry, entry.eviction);
    const auto expected = next.time + entry.jitter_offset;
    if (expected != heap_time) continue;
    entry.last_trigger = next.trigger;

    bytes_estimate_ -= EntryBytes(entry);
    result.push_back(std::move(entry));
    entries_.erase(it);
  }

  return result;
}

std::vector<BufferEntry> CompactionBuffer::FlushOldest(size_t target_bytes, size_t max_count)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  std::vector<BufferEntry> result;

  while (!flush_heap_.empty() && result.size() < max_count && bytes_estimate_ > target_bytes) {
    auto heap_time = flush_heap_.top().scheduled_time;
    auto heap_key = flush_heap_.top().key;
    flush_heap_.pop();

    auto it = entries_.find(heap_key);
    if (it == entries_.end()) continue;

    auto& entry = it->second;
    const auto next = strategy_.NextFlushTime(entry, entry.eviction);
    const auto expected = next.time + entry.jitter_offset;
    if (expected != heap_time) continue;
    entry.last_trigger = next.trigger;

    bytes_estimate_ -= EntryBytes(entry);
    result.push_back(std::move(entry));
    entries_.erase(it);
  }

  return result;
}

void CompactionBuffer::Reinsert(std::vector<BufferEntry> entries) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  for (auto& entry : entries) {
    const std::string key = entry.key;
    bytes_estimate_ += EntryBytes(entry);
    const auto next = strategy_.NextFlushTime(entry, entry.eviction);
    entry.last_trigger = next.trigger;
    const auto scheduled = next.time + entry.jitter_offset;
    entries_.insert_or_assign(key, std::move(entry));
    flush_heap_.push({scheduled, key});
  }
}

std::optional<core::SequenceId> CompactionBuffer::OldestPendingSeq() const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  if (entries_.empty()) return std::nullopt;
  core::SequenceId oldest = std::numeric_limits<core::SequenceId>::max();
  for (const auto& [_, entry] : entries_) {
    oldest = std::min(oldest, entry.first_seen_seq);
  }
  return oldest;
}

size_t CompactionBuffer::Size() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  return entries_.size();
}

size_t CompactionBuffer::BytesEstimate() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  return bytes_estimate_;
}

std::chrono::milliseconds CompactionBuffer::ComputeJitter() {
  auto max_jitter = strategy_.MaxJitter();
  if (max_jitter.count() <= 0) return std::chrono::milliseconds{0};
  std::uniform_int_distribution<int64_t> dist(0, max_jitter.count());
  return std::chrono::milliseconds{dist(rng_)};
}

}  // namespace abyss::consumer
