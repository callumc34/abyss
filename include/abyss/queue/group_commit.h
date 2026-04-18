#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/fsync_policy.h"

namespace abyss::queue {

struct GroupCommitConfig {
  FsyncPolicy policy = FsyncPolicy::kGroupCommit;
  std::chrono::microseconds interval{1000};
  size_t max_bytes = 1048576;
};

// Coordinates fsyncs across multiple producer threads.
class GroupCommitter {
 public:
  using FsyncFn = std::function<core::Result<void>()>;

  GroupCommitter(GroupCommitConfig config, FsyncFn fsync_fn);
  ~GroupCommitter();

  GroupCommitter(const GroupCommitter&) = delete;
  GroupCommitter& operator=(const GroupCommitter&) = delete;
  GroupCommitter(GroupCommitter&&) = delete;
  GroupCommitter& operator=(GroupCommitter&&) = delete;

  // Enqueue bytes for the next fsync.
  DurabilityFuture Submit(size_t bytes);

  // Force an immediate fsync and wait for its completion. No-op for kNone.
  core::Result<void> Drain();

  // Signal the commit thread to stop.
  void Stop();

  FsyncPolicy policy() const { return config_.policy; }

 private:
  void Run();

  struct Pending {
    std::promise<core::Result<void>> promise;
    size_t bytes = 0;
  };

  GroupCommitConfig config_;
  FsyncFn fsync_fn_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<Pending> pending_ ABYSS_GUARDED_BY(mu_);
  size_t pending_bytes_ ABYSS_GUARDED_BY(mu_) = 0;
  bool flush_requested_ ABYSS_GUARDED_BY(mu_) = false;
  bool stopped_ ABYSS_GUARDED_BY(mu_) = false;

  std::thread thread_;
};

}  // namespace abyss::queue
