#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "abyss/consumer/compacted_state.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

struct BufferEntry {
  std::string key;
  CompactedState state;
  core::SteadyTime first_seen;
  core::SteadyTime last_modified;
  uint64_t write_count = 0;
};

class CompactionBuffer {
 public:
  explicit CompactionBuffer(core::SteadyClockFn clock = core::DefaultSteadyClock)
      : clock_(std::move(clock)) {}

  void Absorb(const std::string& key, const core::ops::WriteOp& op) ABYSS_EXCLUDES(mutex_);

  core::Result<core::RespValue> Read(const std::string& key) const ABYSS_EXCLUDES(mutex_);

  std::vector<BufferEntry> FlushReady(core::SteadyTime now) ABYSS_EXCLUDES(mutex_);

  size_t Size() const ABYSS_EXCLUDES(mutex_);
  size_t BytesEstimate() const ABYSS_EXCLUDES(mutex_);

 private:
  core::SteadyClockFn clock_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, BufferEntry> entries_ ABYSS_GUARDED_BY(mutex_);
};

}  // namespace abyss::consumer
