#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
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

  // Enqueue `bytes` for the next fsync. `batch_last_seq` is the highest seq the
  // submitted bytes materialise; durable_seq advances to it once the fsync that
  // covers it completes (the Kafka log-end-offset vs high-watermark split).
  DurabilityFuture Submit(size_t bytes, core::SequenceId batch_last_seq);

  // Force an immediate fsync and wait for its completion. No-op for kNone.
  core::Result<void> Drain();

  // Signal the commit thread to stop.
  void Stop();

  FsyncPolicy policy() const { return config_.policy; }

  // Highest seq whose group-commit fsync has completed. Monotonic; 0 = none
  // durable. Under kNone it tracks the highest submitted (published) seq so the
  // retention-Ack gate is a correct no-op (durability is disabled — see the
  // CRITICAL startup warning in the validator).
  core::SequenceId DurableSeq() const noexcept {
    return durable_seq_.load(std::memory_order_acquire);
  }

  // True once any seq has become durable. Disambiguates the seq-0 edge: a
  // 0-valued DurableSeq() means "none durable" until the first fsync lands, at
  // which point seq 0 (the first WAL entry) is genuinely durable.
  bool HasDurable() const noexcept { return has_durable_.load(std::memory_order_acquire); }

  // Block until seq is durable (HasDurable() && DurableSeq() >= seq) or
  // `timeout` elapses. Returns true iff that condition held in time.
  bool AwaitDurable(core::SequenceId seq, std::chrono::nanoseconds timeout) const;

 private:
  void Run();

  // Advance durable_seq_ monotonically to `seq` and wake awaiters. Caller must
  // not hold mu_.
  void PublishDurable(core::SequenceId seq);

  struct Pending {
    std::promise<core::Result<void>> promise;
    size_t bytes = 0;
    core::SequenceId batch_last_seq = 0;
  };

  GroupCommitConfig config_;
  FsyncFn fsync_fn_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<Pending> pending_ ABYSS_GUARDED_BY(mu_);
  size_t pending_bytes_ ABYSS_GUARDED_BY(mu_) = 0;
  // Highest seq submitted into the current (not-yet-flushed) batch.
  core::SequenceId batch_high_seq_ ABYSS_GUARDED_BY(mu_) = 0;
  bool flush_requested_ ABYSS_GUARDED_BY(mu_) = false;
  bool stopped_ ABYSS_GUARDED_BY(mu_) = false;

  // Monotonic durable high-water. Advanced only after fsync_fn() succeeds
  // (kGroupCommit), synchronously per write (kPerWrite), or to the published
  // seq (kNone). Awaited via durable_cv_ under durable_mu_.
  std::atomic<core::SequenceId> durable_seq_{0};
  // Set true the first time any seq becomes durable, so a 0 watermark is not
  // ambiguous with "seq 0 is durable".
  std::atomic<bool> has_durable_{false};
  mutable std::mutex durable_mu_;
  mutable std::condition_variable durable_cv_;

  std::thread thread_;
};

}  // namespace abyss::queue
