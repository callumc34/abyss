#include "abyss/consumer/compaction_buffer.h"

#include <algorithm>
#include <deque>
#include <limits>
#include <mutex>
#include <string>
#include <utility>

#include "abyss/core/fatal.h"
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
    : CompactionBuffer(FlushStrategy{}, std::move(clock), std::nullopt) {}

void CompactionBuffer::Absorb(const std::string& key, const core::ops::WriteOp& op,
                              core::EvictionTTL eviction, core::SequenceId seq,
                              uint64_t appended_at_ms) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  ABYSS_DCHECK(seq >= core::kFirstSeq, "absorbed an effect at seq 0, which names no entry");
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
  // The pending order is absorb order, so it orders seqs only if they
  // arrive in order.
  ABYSS_DCHECK(seq >= max_absorbed_seq_,
               "a shard's seqs were absorbed out of order: " + std::to_string(seq) + " < " +
                   std::to_string(max_absorbed_seq_));
  max_absorbed_seq_ = std::max(max_absorbed_seq_, seq);
  if (is_new) {
    entry.key = key;
    // Never before an earlier entry's, so the pending order orders it.
    last_first_seen_ = std::max(last_first_seen_, clock_());
    entry.first_seen = last_first_seen_;
    entry.jitter_offset = ComputeJitter();
    entry.first_seen_seq = seq;
    entry.first_appended_at_ms = max_absorbed_ms_;
    entry.pending_ticket_ = pending_base_ + pending_.size();
    pending_.push_back(&entry);
  }
  PublishLogClock();

  entry.last_seq = std::max(entry.last_seq, seq);
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
  const uint64_t slot = entry.pending_ticket_ - pending_base_;
  const bool listed = slot < pending_.size() && pending_[slot] == &entry;
  ABYSS_DCHECK(listed, "a buffered entry is missing from the pending order");
  pending_[slot] = nullptr;
  ++pending_dead_;
}

void CompactionBuffer::TrimPending() {
  while (!pending_.empty() && pending_.front() == nullptr) {
    pending_.pop_front();
    ++pending_base_;
    --pending_dead_;
  }
  // Flushes run out of absorb order, so a long-lived front entry can
  // hold many dead slots behind it.
  if (pending_dead_ <= pending_.size() - pending_dead_) return;
  std::deque<BufferEntry*> live;
  for (BufferEntry* entry : pending_) {
    if (entry == nullptr) continue;
    entry->pending_ticket_ = pending_base_ + live.size();
    live.push_back(entry);
  }
  pending_.swap(live);
  pending_dead_ = 0;
}

const BufferEntry* CompactionBuffer::OldestPending() const {
  const bool trimmed = pending_.empty() || pending_.front() != nullptr;
  ABYSS_DCHECK(trimmed, "the pending order's front was flushed but not trimmed");
  return pending_.empty() ? nullptr : pending_.front();
}

void CompactionBuffer::PublishLogClock() {
  const BufferEntry* oldest = OldestPending();
  const uint64_t clock = oldest == nullptr ? max_absorbed_ms_ : oldest->first_appended_at_ms;
  ABYSS_DCHECK(clock >= log_clock_ms_.load(std::memory_order_relaxed),
               "compaction buffer log clock moved backwards");
  log_clock_ms_.store(clock, std::memory_order_release);
}

std::optional<CompactedState> CompactionBuffer::Snapshot(std::string_view key) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  const auto it = entries_.find(key);
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
  TrimPending();
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
  const BufferEntry* oldest = OldestPending();
  if (oldest == nullptr) return std::nullopt;
  return oldest->first_seen_seq;
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
  const BufferEntry* oldest = OldestPending();
  if (oldest == nullptr) return std::nullopt;
  return oldest->first_seen;
}

std::optional<core::SteadyTime> CompactionBuffer::OldestFirstSeenScanForTesting() const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  if (entries_.empty()) return std::nullopt;
  core::SteadyTime oldest = core::SteadyTime::max();
  for (const auto& [_, entry] : entries_) {
    oldest = std::min(oldest, entry.first_seen);
  }
  return oldest;
}

size_t CompactionBuffer::PendingSlotsForTesting() const ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const std::shared_lock lock(mutex_);
  return pending_.size();
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
  pending_.clear();
  pending_dead_ = 0;
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
