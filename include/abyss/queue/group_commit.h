#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <utility>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/queue/append_result.h"

namespace abyss::queue {

// Natural-batching group commit: a flush starts as soon as the previous
// one ends and covers everything published meanwhile. There is no timer.
// A failed flush is fatal: the kernel may have dropped the dirty pages.
class GroupCommitter {
 public:
  struct Extent {
    // Exclusive: seqs below `end` are covered.
    core::SequenceId end = 0;
    // Cumulative entry bytes below `end`.
    uint64_t bytes = 0;
  };

  // Makes everything published when it is called power-durable and
  // returns that extent. Runs on the commit thread.
  using FlushFn = std::function<core::Result<Extent>()>;
  // Runs on the commit thread after each flush that advanced the end.
  using FlushedFn = std::function<void(Extent previous, Extent flushed)>;

  GroupCommitter(Extent durable, FlushFn flush, FlushedFn on_flushed);
  ~GroupCommitter();

  GroupCommitter(const GroupCommitter&) = delete;
  GroupCommitter& operator=(const GroupCommitter&) = delete;
  GroupCommitter(GroupCommitter&&) = delete;
  GroupCommitter& operator=(GroupCommitter&&) = delete;

  // Appender: entries below `end` are published. Cheap while a flush is
  // in flight.
  void Published(core::SequenceId end) noexcept;

  // Resolves once `seq` is durable. Callers register in seq order (under
  // the shard's append lock).
  DurabilityFuture WhenDurable(core::SequenceId seq);

  core::SequenceId DurableEnd() const noexcept {
    return durable_end_.load(std::memory_order_acquire);
  }
  // True iff `seq` became durable within `timeout`; false on Stop.
  bool AwaitDurable(core::SequenceId seq, std::chrono::nanoseconds timeout) const;

  // Flushes what is published (if `final_flush`), then stops the commit
  // thread. Outstanding futures resolve kUnavailable. Idempotent.
  void Stop(bool final_flush);

 private:
  void Run();
  void FlushOnce();
  // Advances durable_end_ and fulfils waiters below it.
  void Advance(Extent flushed);
  void RecordFlush(std::chrono::steady_clock::duration elapsed, uint64_t entries) noexcept;

  FlushFn flush_;
  FlushedFn on_flushed_;
  metrics::HistogramHandle flush_duration_;
  metrics::HistogramHandle flush_batch_entries_;

  std::atomic<core::SequenceId> published_end_;
  std::atomic<core::SequenceId> durable_end_;
  // Commit thread only.
  Extent durable_;

  // Dekker handshake with Published: the flusher sets idle_ before its
  // last look at published_end_, the appender reads it after its store.
  std::mutex wake_mu_;
  std::condition_variable wake_cv_;
  std::atomic<bool> idle_{false};
  bool stop_ ABYSS_GUARDED_BY(wake_mu_) = false;
  bool final_flush_ ABYSS_GUARDED_BY(wake_mu_) = false;

  mutable std::mutex durable_mu_;
  mutable std::condition_variable durable_cv_;
  bool stopped_ ABYSS_GUARDED_BY(durable_mu_) = false;
  // Seq order, so Advance fulfils from the front.
  std::deque<std::pair<core::SequenceId, std::promise<core::Result<void>>>> waiters_
      ABYSS_GUARDED_BY(durable_mu_);

  std::mutex stop_mu_;
  std::thread thread_;
};

}  // namespace abyss::queue
