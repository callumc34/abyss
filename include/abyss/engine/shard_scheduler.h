#pragma once

#include <functional>

#include "abyss/core/types.h"

namespace abyss::engine {

// Submit per-shard work to be run concurrently, then block until all
// submitted work completes. The runtime that backs the scheduler is
// implementation-defined (bounded thread pool today; per-core dispatch when
// the shared-nothing runtime lands). Coordinators depend on this surface
// only — they never observe how the work runs.
class ShardScheduler {
 public:
  ShardScheduler() = default;
  virtual ~ShardScheduler() = default;
  ShardScheduler(const ShardScheduler&) = delete;
  ShardScheduler& operator=(const ShardScheduler&) = delete;
  ShardScheduler(ShardScheduler&&) = delete;
  ShardScheduler& operator=(ShardScheduler&&) = delete;

  // The shard id is advisory; backends that pin work to a core or thread
  // use it to choose an executor. Pure thread-pool backends ignore it.
  // `work` must not throw — uncaught exceptions terminate.
  virtual void Submit(core::ShardId shard, std::function<void()> work) = 0;

  // Blocks until every Submit() since the last WaitAll() has run to
  // completion. Re-entrant submission from within submitted work is not
  // supported.
  virtual void WaitAll() = 0;
};

}  // namespace abyss::engine
