#pragma once

#include "abyss/core/types.h"

namespace abyss::consumer {

// Per-shard view of how far the hot consumer has settled the queue. Defined
// here (separate from HotConsumerPool) so the engine can depend on the
// progress surface without pulling in HotConsumer's transitive headers.
class HotConsumerProgress {
 public:
  HotConsumerProgress() = default;
  virtual ~HotConsumerProgress() = default;
  HotConsumerProgress(const HotConsumerProgress&) = delete;
  HotConsumerProgress& operator=(const HotConsumerProgress&) = delete;
  HotConsumerProgress(HotConsumerProgress&&) = delete;
  HotConsumerProgress& operator=(HotConsumerProgress&&) = delete;

  // Highest seq the per-shard hot consumer has fully settled (applied AND
  // any pending Conditional has resolved). 0 if nothing has been settled yet.
  virtual core::SequenceId HighestSettledSeq(core::ShardId shard) const = 0;
};

}  // namespace abyss::consumer
