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

class CompactionBuffer {
 public:
  CompactionBuffer(FlushStrategy strategy, core::SteadyClockFn clock,
                   std::optional<uint64_t> rng_seed = std::nullopt);

  explicit CompactionBuffer(core::SteadyClockFn clock = core::DefaultSteadyClock);

  void Absorb(const std::string& key, const core::ops::WriteOp& op, core::EvictionTTL eviction,
              core::SequenceId seq = 0) ABYSS_EXCLUDES(mutex_);

  // kNotFound signals "fall through to next tier"; tombstones surface as
  // RespValue::Null so the caller treats a buffered DEL as authoritative.
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const ABYSS_EXCLUDES(mutex_);

  core::Result<core::RespValue> Read(const std::string& key) const ABYSS_EXCLUDES(mutex_);

  std::vector<BufferEntry> FlushReady(core::SteadyTime now,
                                      size_t max_count = std::numeric_limits<size_t>::max())
      ABYSS_EXCLUDES(mutex_);

  std::vector<BufferEntry> FlushOldest(size_t target_bytes, size_t max_count)
      ABYSS_EXCLUDES(mutex_);

  void Reinsert(std::vector<BufferEntry> entries) ABYSS_EXCLUDES(mutex_);

  std::optional<core::SequenceId> OldestPendingSeq() const ABYSS_EXCLUDES(mutex_);

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
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, BufferEntry> entries_ ABYSS_GUARDED_BY(mutex_);
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> flush_heap_
      ABYSS_GUARDED_BY(mutex_);
  size_t bytes_estimate_ ABYSS_GUARDED_BY(mutex_) = 0;
  std::mt19937_64 rng_ ABYSS_GUARDED_BY(mutex_);
};

}  // namespace abyss::consumer
