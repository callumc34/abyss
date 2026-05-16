#pragma once

#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <random>
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

class CompactionBuffer {
 public:
  CompactionBuffer(FlushStrategy strategy, core::SteadyClockFn clock,
                   std::optional<uint64_t> rng_seed = std::nullopt,
                   core::WallClockFn wall_clock = core::DefaultWallClock);

  explicit CompactionBuffer(core::SteadyClockFn clock = core::DefaultSteadyClock,
                            core::WallClockFn wall_clock = core::DefaultWallClock);

  void Absorb(const std::string& key, const core::ops::WriteOp& op, core::EvictionTTL eviction,
              core::SequenceId seq = 0) ABYSS_EXCLUDES(mutex_);

  // kNotFound signals "fall through to next tier"; tombstones surface as
  // RespValue::Null so the caller treats a buffered DEL as authoritative.
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const ABYSS_EXCLUDES(mutex_);

  core::Result<core::RespValue> Read(const std::string& key) const ABYSS_EXCLUDES(mutex_);

  BufferKeyPresence Probe(std::string_view key) const ABYSS_EXCLUDES(mutex_);

  HashOverlay HashOverlayFor(std::string_view key) const ABYSS_EXCLUDES(mutex_);

  std::vector<BufferEntry> FlushReady(core::SteadyTime now,
                                      size_t max_count = std::numeric_limits<size_t>::max())
      ABYSS_EXCLUDES(mutex_);

  std::vector<BufferEntry> FlushOldest(size_t target_bytes, size_t max_count)
      ABYSS_EXCLUDES(mutex_);

  void Reinsert(std::vector<BufferEntry> entries) ABYSS_EXCLUDES(mutex_);

  std::optional<core::SequenceId> OldestPendingSeq() const ABYSS_EXCLUDES(mutex_);

  // Drops every buffered entry without emitting to cold. Used by FLUSHDB.
  void Clear() ABYSS_EXCLUDES(mutex_);

  size_t Size() const ABYSS_EXCLUDES(mutex_);
  size_t BytesEstimate() const ABYSS_EXCLUDES(mutex_);

 private:
  struct HeapEntry {
    core::SteadyTime scheduled_time;
    std::string key;

    friend bool operator>(const HeapEntry& a, const HeapEntry& b) {
      return a.scheduled_time > b.scheduled_time;
    }
  };

  std::chrono::milliseconds ComputeJitter() ABYSS_REQUIRES(mutex_);

  const FlushStrategy strategy_;
  core::SteadyClockFn clock_;
  core::WallClockFn wall_clock_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, BufferEntry> entries_ ABYSS_GUARDED_BY(mutex_);
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> flush_heap_
      ABYSS_GUARDED_BY(mutex_);
  size_t bytes_estimate_ ABYSS_GUARDED_BY(mutex_) = 0;
  std::mt19937_64 rng_ ABYSS_GUARDED_BY(mutex_);
};

}  // namespace abyss::consumer
