#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "abyss/consumer/buffer_entry.h"
#include "abyss/consumer/flush_strategy.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

// Type-agnostic verdict on a key's presence in the buffer. EXISTS-style fan-out
// can't use Read/Exec for this — Read is string-typed (NotFound for a hash in
// the buffer) and Exec(Exists) collapses tombstone and miss to the same zero.
// kTombstoned must override cold so a not-yet-flushed DEL suppresses a stale
// cold residual.
enum class BufferKeyPresence : uint8_t {
  kAbsent,
  kTombstoned,
  kPresent,
};

// Snapshot of a key's hash state in the buffer. Multi-field hash reads cannot
// be answered from the buffer alone: the buffer represents the delta since
// the last flush, while cold holds the prior committed state. The engine
// pulls this overlay and merges it with cold's result for the full answer.
struct HashOverlay {
  enum class Kind : uint8_t {
    // No buffer entry for this key — engine reads cold as-is.
    kNotPresent,
    // Buffer holds a DEL; key is dead regardless of cold's content.
    kTombstone,
    // Buffer holds a different type (e.g. a SET that re-typed the key);
    // engine surfaces WRONGTYPE without consulting cold's hash records.
    kWrongType,
    // Buffer holds hash state; merge with cold.
    kHash,
  };
  Kind kind = Kind::kNotPresent;
  std::unordered_map<std::string, std::string> fields;
  std::unordered_set<std::string> removed_fields;
};

// Entries selected for a flush. They stay buffered and readable until
// EraseFlushed or Reschedule; nothing else may mutate the buffer meanwhile.
using FlushBatch = std::vector<std::reference_wrapper<const BufferEntry>>;

class CompactionBuffer {
 public:
  CompactionBuffer(FlushStrategy strategy, core::SteadyClockFn clock,
                   std::optional<uint64_t> rng_seed = std::nullopt,
                   core::WallClockFn wall_clock = core::DefaultWallClock);

  explicit CompactionBuffer(core::SteadyClockFn clock = core::DefaultSteadyClock,
                            core::WallClockFn wall_clock = core::DefaultWallClock);

  // `position` is the seq replay must resume from to re-derive the effect;
  // `carrier` is the seq of the entry that holds it (ADP-004), and
  // `appended_at_ms` that entry's appended_at.
  void Absorb(const std::string& key, const core::ops::WriteOp& op, core::EvictionTTL eviction,
              core::SequenceId position, core::SequenceId carrier, uint64_t appended_at_ms)
      ABYSS_EXCLUDES(mutex_);

  // kNotFound signals "fall through to next tier"; tombstones surface as
  // RespValue::Null so the caller treats a buffered DEL as authoritative.
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const ABYSS_EXCLUDES(mutex_);

  core::Result<core::RespValue> Read(const std::string& key) const ABYSS_EXCLUDES(mutex_);

  BufferKeyPresence Probe(std::string_view key) const ABYSS_EXCLUDES(mutex_);

  HashOverlay HashOverlayFor(std::string_view key) const ABYSS_EXCLUDES(mutex_);

  // A copy of `key`'s compacted delta, TTL unjudged; nullopt if none.
  std::optional<CompactedState> Snapshot(std::string_view key) const ABYSS_EXCLUDES(mutex_);

  FlushBatch FlushReady(core::SteadyTime now, size_t max_count = std::numeric_limits<size_t>::max())
      ABYSS_EXCLUDES(mutex_);

  FlushBatch FlushOldest(size_t target_bytes, size_t max_count) ABYSS_EXCLUDES(mutex_);

  // After the batch is applied; its references dangle afterwards.
  void EraseFlushed(const FlushBatch& batch) ABYSS_EXCLUDES(mutex_);

  // After a failed or deferred apply: the batch is due again.
  void Reschedule(const FlushBatch& batch) ABYSS_EXCLUDES(mutex_);

  std::optional<core::SequenceId> OldestPendingSeq() const ABYSS_EXCLUDES(mutex_);

  // The shard's log clock, in ms: the first unflushed appended_at of the
  // oldest entry still buffered, or the newest absorbed when none is.
  // Every write not yet in cold is at or after it, so cold may delete a
  // key whose TTL is at or below it (ADP-004). Never moves backwards.
  uint64_t LogClockMs() const { return log_clock_ms_.load(std::memory_order_acquire); }

  // min(first_seen) over live entries; nullopt when empty. Backs the ADP-004
  // oldest_unflushed_age lag signal (COLDC-5).
  std::optional<core::SteadyTime> OldestFirstSeen() const ABYSS_EXCLUDES(mutex_);

  // Drops every buffered entry without emitting to cold, and advances the
  // log clock to the Flush's appended_at. Used by FLUSHDB.
  void Clear(uint64_t flush_appended_at_ms) ABYSS_EXCLUDES(mutex_);

  // Full scans that OldestPendingSeq and LogClockMs must agree with.
  std::optional<core::SequenceId> OldestPendingSeqScanForTesting() const ABYSS_EXCLUDES(mutex_);
  uint64_t LogClockScanForTesting() const ABYSS_EXCLUDES(mutex_);

  size_t Size() const ABYSS_EXCLUDES(mutex_);
  size_t BytesEstimate() const ABYSS_EXCLUDES(mutex_);
  // Live flush-heap depth (after lazy-stale skips are accounted on pop). Surfaces
  // heap growth as an observable gauge (COLDC-4).
  size_t HeapDepth() const ABYSS_EXCLUDES(mutex_);

 private:
  struct HeapEntry {
    core::SteadyTime scheduled_time;
    std::string key;

    friend bool operator>(const HeapEntry& a, const HeapEntry& b) {
      return a.scheduled_time > b.scheduled_time;
    }
  };

  // Per-live-heap-entry overhead charged into BytesEstimate so the flush heap's
  // own memory drives the high-water/aggressive trigger (COLDC-4); a key whose
  // quiet deadline keeps sliding can no longer accumulate heap entries silently.
  static constexpr size_t kHeapEntryOverhead = 64;

  // Pushes a heap entry for `key` scheduled at `scheduled`, charging the heap
  // overhead. Records the scheduled time on the entry for dedup on re-absorb.
  void PushHeapEntry(BufferEntry& entry, core::SteadyTime scheduled) ABYSS_REQUIRES(mutex_);

  // Marks `key` in flight if `heap_time` is its live schedule; else null.
  BufferEntry* SelectLocked(const std::string& key, core::SteadyTime heap_time)
      ABYSS_REQUIRES(mutex_);

  std::chrono::milliseconds ComputeJitter() ABYSS_REQUIRES(mutex_);

  void ErasePending(const BufferEntry& entry) ABYSS_REQUIRES(mutex_);
  void PublishLogClock() ABYSS_REQUIRES(mutex_);

  const FlushStrategy strategy_;
  core::SteadyClockFn clock_;
  core::WallClockFn wall_clock_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, BufferEntry> entries_ ABYSS_GUARDED_BY(mutex_);
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> flush_heap_
      ABYSS_GUARDED_BY(mutex_);
  size_t bytes_estimate_ ABYSS_GUARDED_BY(mutex_) = 0;
  // Charged on every heap push, decremented on every pop (including stale-skip
  // pops). Folded into BytesEstimate so heap growth surfaces as backpressure.
  size_t heap_overhead_bytes_ ABYSS_GUARDED_BY(mutex_) = 0;
  // Entries selected for a flush and not yet erased or rescheduled.
  size_t in_flight_ ABYSS_GUARDED_BY(mutex_) = 0;
  std::mt19937_64 rng_ ABYSS_GUARDED_BY(mutex_);
  // One element per buffered entry: its first_seen_seq, and its
  // first_appended_at_ms. They differ in order only while Resolved
  // effects carry an older position than their entry.
  std::multiset<core::SequenceId> pending_seqs_ ABYSS_GUARDED_BY(mutex_);
  std::multiset<uint64_t> pending_times_ ABYSS_GUARDED_BY(mutex_);
  uint64_t max_absorbed_ms_ ABYSS_GUARDED_BY(mutex_) = 0;
  std::atomic<uint64_t> log_clock_ms_{0};
};

}  // namespace abyss::consumer
