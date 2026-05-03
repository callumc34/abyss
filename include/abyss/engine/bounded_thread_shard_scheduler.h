#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "abyss/core/thread_annotations.h"
#include "abyss/engine/shard_scheduler.h"

namespace abyss::engine {

// A bounded thread pool ShardScheduler. `worker_count` workers pull from a
// FIFO of (shard, work) pairs. Suitable for Phase 1 / Phase 2 single-pod
// recovery where parallelism caps thread count below shard count.
//
// Lifetime: workers are spawned in the constructor and joined in the
// destructor. Submit() between WaitAll() boundaries is allowed; concurrent
// Submit() from multiple producers is allowed.
class BoundedThreadShardScheduler : public ShardScheduler {
 public:
  explicit BoundedThreadShardScheduler(uint32_t worker_count);
  ~BoundedThreadShardScheduler() override;

  BoundedThreadShardScheduler(const BoundedThreadShardScheduler&) = delete;
  BoundedThreadShardScheduler& operator=(const BoundedThreadShardScheduler&) = delete;
  BoundedThreadShardScheduler(BoundedThreadShardScheduler&&) = delete;
  BoundedThreadShardScheduler& operator=(BoundedThreadShardScheduler&&) = delete;

  void Submit(core::ShardId shard, std::function<void()> work) override;
  void WaitAll() override;

  uint32_t worker_count() const noexcept { return static_cast<uint32_t>(workers_.size()); }

 private:
  struct Task {
    core::ShardId shard;
    std::function<void()> work;
  };

  void RunWorker();

  mutable std::mutex mu_;
  std::condition_variable not_empty_;
  std::condition_variable idle_;
  std::queue<Task> tasks_ ABYSS_GUARDED_BY(mu_);
  size_t in_flight_ ABYSS_GUARDED_BY(mu_) = 0;
  bool shutting_down_ ABYSS_GUARDED_BY(mu_) = false;

  std::vector<std::thread> workers_;
};

}  // namespace abyss::engine
