#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/metrics/metrics.h"
#include "abyss/queue/log.h"

namespace abyss::queue {

// Natural-batching group commit for one log: a flush starts as soon as
// the previous one ends and covers everything published meanwhile.
// There is no timer. A failed flush is fatal: the kernel may have
// dropped the dirty pages.
class GroupCommitter {
 public:
  struct Extent {
    // Exclusive: positions below `end` are durable.
    LogPosition end = 0;
    // Entries this flush made durable.
    uint64_t entries = 0;
  };

  // Makes everything published when it is called power-durable and
  // returns that extent. Runs on the commit thread.
  using FlushFn = std::function<core::Result<Extent>()>;
  // Runs on the commit thread after each flush that advanced the end,
  // before DurableEnd() shows it.
  using FlushedFn = std::function<void(LogPosition previous, Extent flushed)>;

  GroupCommitter(LogPosition durable, FlushFn flush, FlushedFn on_flushed);
  ~GroupCommitter();

  GroupCommitter(const GroupCommitter&) = delete;
  GroupCommitter& operator=(const GroupCommitter&) = delete;
  GroupCommitter(GroupCommitter&&) = delete;
  GroupCommitter& operator=(GroupCommitter&&) = delete;

  // Appender: positions below `end` are published. Cheap while a flush
  // is in flight.
  void Published(LogPosition end) noexcept;

  LogPosition DurableEnd() const noexcept { return durable_end_.load(std::memory_order_acquire); }

  // Flushes what is published (if `final_flush`), then stops the commit
  // thread. Idempotent.
  void Stop(bool final_flush);

 private:
  void Run();
  void FlushOnce();
  void RecordFlush(std::chrono::steady_clock::duration elapsed, uint64_t entries) noexcept;

  FlushFn flush_;
  FlushedFn on_flushed_;
  metrics::HistogramHandle flush_duration_;
  metrics::HistogramHandle flush_batch_entries_;

  std::atomic<LogPosition> published_end_;
  std::atomic<LogPosition> durable_end_;
  // Commit thread only.
  LogPosition durable_;

  // Dekker handshake with Published: the flusher sets idle_ before its
  // last look at published_end_, the appender reads it after its store.
  std::mutex wake_mu_;
  std::condition_variable wake_cv_;
  std::atomic<bool> idle_{false};
  bool stop_ ABYSS_GUARDED_BY(wake_mu_) = false;
  bool final_flush_ ABYSS_GUARDED_BY(wake_mu_) = false;

  std::mutex stop_mu_;
  std::thread thread_;
};

}  // namespace abyss::queue
