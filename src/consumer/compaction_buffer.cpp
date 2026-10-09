#include "abyss/consumer/compaction_buffer.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <string>
#include <utility>

#include "abyss/core/fatal.h"
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
                              core::EvictionTTL eviction, core::SequenceId position,
                              core::SequenceId carrier,
                              uint64_t appended_at_ms) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  ABYSS_DCHECK(std::min(position, carrier) >= core::kFirstSeq,
               "absorbed an effect at seq 0, which names no entry");
  const std::unique_lock lock(mutex_);
  // A selected batch is applied by reference; changing it would let
  // EraseFlushed drop state that never reached cold.
  if (in_flight_ != 0) core::Fatal("compaction buffer absorbed during an in-flight flush");
  auto& entry = entries_[key];
  bool is_new = entry.key.empty();

  size_t old_entry_bytes = is_new ? 0 : EntryBytes(entry);

  // The sequencer stamps a shard's writes in seq order, so a stamp
  // below one absorbed already is a regression, not a race. Absorbed,
  // it would let the log clock pass a write it understates.
  ABYSS_DCHECK(appended_at_ms >= max_absorbed_ms_,
               "a shard's appended_at went backwards: " + std::to_string(appended_at_ms) + " < " +
                   std::to_string(max_absorbed_ms_));
  max_absorbed_ms_ = std::max(max_absorbed_ms_, appended_at_ms);
  if (is_new) {
    entry.key = key;
    entry.first_seen = clock_();
    entry.jitter_offset = ComputeJitter();
    entry.first_seen_seq = position;
    entry.first_appended_at_ms = max_absorbed_ms_;
    pending_seqs_.insert(position);
    pending_times_.insert(max_absorbed_ms_);
  }
  PublishLogClock();

  entry.last_seq = std::max(entry.last_seq, carrier);
  entry.eviction = eviction;
  entry.state.Absorb(op);
  entry.last_modified = clock_();
  ++entry.write_count;

  bytes_estimate_ -= old_entry_bytes;
  bytes_estimate_ += EntryBytes(entry);

  auto next = strategy_.NextFlushTime(entry, entry.eviction);
  entry.last_trigger = next.trigger;
  const auto scheduled = next.time + entry.jitter_offset;
  // Dedup (COLDC-4): re-absorbs that do not move the schedule (same clock
  // instant) reuse the live heap entry instead of pushing a stale duplicate.
  // A fresh entry always has a default-constructed scheduled_in_heap_ and so
  // gets its first push here.
  if (!is_new && entry.scheduled_in_heap_ == scheduled) {
    return;
  }
  PushHeapEntry(entry, scheduled);
}

void CompactionBuffer::PushHeapEntry(BufferEntry& entry, core::SteadyTime scheduled) {
  entry.scheduled_in_heap_ = scheduled;
  heap_overhead_bytes_ += kHeapEntryOverhead + entry.key.size();
  flush_heap_.push({scheduled, entry.key});
}

void CompactionBuffer::ErasePending(const BufferEntry& entry) {
  const auto seq = pending_seqs_.find(entry.first_seen_seq);
  const auto time = pending_times_.find(entry.first_appended_at_ms);
  ABYSS_DCHECK(seq != pending_seqs_.end() && time != pending_times_.end(),
               "a buffered entry is missing from the pending order");
  pending_seqs_.erase(seq);
  pending_times_.erase(time);
}

void CompactionBuffer::PublishLogClock() {
  const uint64_t clock = pending_times_.empty() ? max_absorbed_ms_ : *pending_times_.begin();
  ABYSS_DCHECK(clock >= log_clock_ms_.load(std::memory_order_relaxed),
               "compaction buffer log clock moved backwards");
  log_clock_ms_.store(clock, std::memory_order_release);
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

std::optional<CompactedState> CompactionBuffer::Snapshot(std::string_view key) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  const auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return std::nullopt;
  return it->second.state;
}

FlushBatch CompactionBuffer::FlushReady(core::SteadyTime now,
                                        size_t max_count) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  FlushBatch result;

  while (!flush_heap_.empty() && flush_heap_.top().scheduled_time <= now &&
         result.size() < max_count) {
    auto heap_time = flush_heap_.top().scheduled_time;
    auto heap_key = flush_heap_.top().key;
    flush_heap_.pop();
    // Charge the pop against the heap budget regardless of whether the entry is
    // accepted below — every push had a matching charge (COLDC-4).
    heap_overhead_bytes_ -= kHeapEntryOverhead + heap_key.size();

    BufferEntry* entry = SelectLocked(heap_key, heap_time);
    if (entry != nullptr) result.emplace_back(*entry);
  }

  return result;
}

FlushBatch CompactionBuffer::FlushOldest(size_t target_bytes,
                                         size_t max_count) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  FlushBatch result;
  // Selected entries stay buffered, so count them off the estimate here.
  size_t selected_bytes = 0;

  // Compare the combined estimate (entry bytes + heap overhead) against the
  // target so heap-driven pressure actually drains, mirroring BytesEstimate().
  while (!flush_heap_.empty() && result.size() < max_count &&
         bytes_estimate_ - selected_bytes + heap_overhead_bytes_ > target_bytes) {
    auto heap_time = flush_heap_.top().scheduled_time;
    auto heap_key = flush_heap_.top().key;
    flush_heap_.pop();
    heap_overhead_bytes_ -= kHeapEntryOverhead + heap_key.size();

    BufferEntry* entry = SelectLocked(heap_key, heap_time);
    if (entry == nullptr) continue;
    selected_bytes += EntryBytes(*entry);
    result.emplace_back(*entry);
  }

  return result;
}

BufferEntry* CompactionBuffer::SelectLocked(const std::string& key, core::SteadyTime heap_time) {
  auto it = entries_.find(key);
  if (it == entries_.end()) return nullptr;

  auto& entry = it->second;
  // A key can hold two heap entries for one time; select it only once.
  if (entry.in_flight_) return nullptr;
  const auto next = strategy_.NextFlushTime(entry, entry.eviction);
  const auto expected = next.time + entry.jitter_offset;
  if (expected != heap_time) return nullptr;
  entry.last_trigger = next.trigger;
  entry.in_flight_ = true;
  ++in_flight_;
  return &entry;
}

void CompactionBuffer::EraseFlushed(const FlushBatch& batch) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  for (const BufferEntry& entry : batch) {
    // Erase by iterator: the key argument would alias the erased node.
    const auto it = entries_.find(entry.key);
    bytes_estimate_ -= EntryBytes(it->second);
    ErasePending(it->second);
    entries_.erase(it);
    --in_flight_;
  }
  PublishLogClock();
}

void CompactionBuffer::Reschedule(const FlushBatch& batch) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  for (const BufferEntry& selected : batch) {
    auto& entry = entries_.find(selected.key)->second;
    entry.in_flight_ = false;
    --in_flight_;
    const auto next = strategy_.NextFlushTime(entry, entry.eviction);
    entry.last_trigger = next.trigger;
    PushHeapEntry(entry, next.time + entry.jitter_offset);
  }
}

std::optional<core::SequenceId> CompactionBuffer::OldestPendingSeq() const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  if (pending_seqs_.empty()) return std::nullopt;
  return *pending_seqs_.begin();
}

std::optional<core::SequenceId> CompactionBuffer::OldestPendingSeqScanForTesting() const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  if (entries_.empty()) return std::nullopt;
  core::SequenceId oldest = std::numeric_limits<core::SequenceId>::max();
  for (const auto& [_, entry] : entries_) {
    oldest = std::min(oldest, entry.first_seen_seq);
  }
  return oldest;
}

uint64_t CompactionBuffer::LogClockScanForTesting() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  if (entries_.empty()) return max_absorbed_ms_;
  uint64_t oldest = std::numeric_limits<uint64_t>::max();
  for (const auto& [_, entry] : entries_) {
    oldest = std::min(oldest, entry.first_appended_at_ms);
  }
  return oldest;
}

std::optional<core::SteadyTime> CompactionBuffer::OldestFirstSeen() const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  if (entries_.empty()) return std::nullopt;
  // entries_ is unordered and flush_heap_ is keyed on scheduled_time, not
  // first_seen, so a scan is the only exact answer.
  core::SteadyTime oldest = core::SteadyTime::max();
  for (const auto& [_, entry] : entries_) {
    oldest = std::min(oldest, entry.first_seen);
  }
  return oldest;
}

void CompactionBuffer::Clear(uint64_t flush_appended_at_ms) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::unique_lock lock(mutex_);
  if (in_flight_ != 0) core::Fatal("compaction buffer cleared during an in-flight flush");
  entries_.clear();
  // std::priority_queue has no clear(); swap with an empty instance.
  decltype(flush_heap_) empty;
  flush_heap_.swap(empty);
  bytes_estimate_ = 0;
  heap_overhead_bytes_ = 0;
  pending_seqs_.clear();
  pending_times_.clear();
  max_absorbed_ms_ = std::max(max_absorbed_ms_, flush_appended_at_ms);
  PublishLogClock();
}

size_t CompactionBuffer::Size() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  return entries_.size();
}

size_t CompactionBuffer::BytesEstimate() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  return bytes_estimate_ + heap_overhead_bytes_;
}

size_t CompactionBuffer::HeapDepth() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  return flush_heap_.size();
}

std::chrono::milliseconds CompactionBuffer::ComputeJitter() {
  auto max_jitter = strategy_.MaxJitter();
  if (max_jitter.count() <= 0) return std::chrono::milliseconds{0};
  std::uniform_int_distribution<int64_t> dist(0, max_jitter.count());
  return std::chrono::milliseconds{dist(rng_)};
}

}  // namespace abyss::consumer
