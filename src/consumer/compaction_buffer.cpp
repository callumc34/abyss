#include "abyss/consumer/compaction_buffer.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

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
                                   std::optional<uint64_t> rng_seed)
    : strategy_(strategy),
      clock_(std::move(clock)),
      rng_(rng_seed.value_or(std::random_device{}())) {}

CompactionBuffer::CompactionBuffer(core::SteadyClockFn clock)
    : CompactionBuffer(FlushStrategy{}, std::move(clock)) {}

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

core::Result<core::RespValue> CompactionBuffer::Read(const std::string& key) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);

  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "buffer has no state for key"));
  }

  const auto& state = it->second.state;

  if (state.IsTombstone()) {
    return core::RespValue::Null();
  }

  if (state.Type() == CompactedState::DataType::kString) {
    return core::RespValue::BulkString(state.StringValue());
  }

  return std::unexpected(
      core::Error(core::ErrorCode::kNotFound, "collection read bypasses buffer"));
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
