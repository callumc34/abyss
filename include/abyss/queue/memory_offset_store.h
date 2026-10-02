#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/queue/offset_store.h"

namespace abyss::queue {

// In-memory OffsetStore whose Set is the persist; drives reaper tests.
class MemoryOffsetStore : public OffsetStore {
 public:
  MemoryOffsetStore() = default;

  std::optional<core::SequenceId> Get(core::ConsumerId consumer,
                                      core::ShardId shard) const override;
  core::Result<void> Set(core::ConsumerId consumer, core::ShardId shard, core::SequenceId seq);

 private:
  static uint64_t Key(core::ConsumerId consumer, core::ShardId shard) {
    return (static_cast<uint64_t>(consumer) << 32) | static_cast<uint64_t>(shard);
  }

  mutable std::mutex mu_;
  std::unordered_map<uint64_t, core::SequenceId> offsets_ ABYSS_GUARDED_BY(mu_);
};

}  // namespace abyss::queue
