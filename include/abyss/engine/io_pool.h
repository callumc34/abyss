#pragma once

#include <array>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::engine {

// One cold read for the pool. A foreground job belongs to its owner,
// which keeps it until the job has posted or a Cancel took it back; a
// background job belongs to the pool. Unless a Cancel takes it, the
// pool calls exactly one of Run and Fail, and reads nothing of the job
// once the call starts.
class IoJob {
 public:
  virtual ~IoJob() = default;

  IoJob(const IoJob&) = delete;
  IoJob& operator=(const IoJob&) = delete;
  IoJob(IoJob&&) = delete;
  IoJob& operator=(IoJob&&) = delete;

  // On a pool thread, before the deadline.
  virtual void Run() noexcept = 0;
  // Instead of Run, reading no cold: kTimeout when dequeued past its
  // deadline, kUnavailable once the pool has shut down, and for a
  // background job kResourceExhausted when its queue is full.
  virtual void Fail(core::Error error) noexcept = 0;

 protected:
  IoJob() = default;

 private:
  friend class IoPool;

  IoJob* prev_ = nullptr;
  IoJob* next_ = nullptr;
  metrics::IoKind kind_ = metrics::IoKind::kPoint;
  core::SteadyTime deadline_{};
  core::SteadyTime enqueued_{};
  // FIFO order across the point and scan queues.
  uint64_t order_ = 0;
  bool queued_ = false;
};

struct IoPoolConfig {
  // At least 2: scans run on at most threads - 1 of them.
  uint32_t threads = 4;
  size_t background_capacity = 1024;
};

// engine.cold_io_threads resolved: 0 is min(4, max(2, hardware)).
uint32_t ColdIoThreads(uint32_t configured, uint32_t hardware);

// Threads for cold reads, which block in pread: a write's loads, a
// read miss's answer and cache fills. The foreground queue (loads and
// misses) runs first; the bounded background queue (fills) runs only
// while it is empty. Scans run on at most threads - 1 threads, so a
// point read always has one.
class IoPool {
 public:
  explicit IoPool(IoPoolConfig config);
  ~IoPool();

  IoPool(const IoPool&) = delete;
  IoPool& operator=(const IoPool&) = delete;
  IoPool(IoPool&&) = delete;
  IoPool& operator=(IoPool&&) = delete;

  // A load or a read miss: kPoint, kScan or kLoad. Fails it here once
  // the pool has shut down.
  void Submit(IoJob& job, metrics::IoKind kind, core::SteadyTime deadline);
  // A fill. Fails it here when the queue is full or the pool shut down.
  void SubmitBackground(std::unique_ptr<IoJob> job, core::SteadyTime deadline);
  // For a foreground job's owner. True: it was still queued and is
  // unlinked; it never runs, Fail is never called and nothing posts,
  // so what it would have released on Fail is the owner's to release.
  // False: a thread has taken it, and it lands as usual. Decided under
  // the queue's lock, in the same hold a dequeue takes a job in.
  bool Cancel(IoJob& job);
  // Fails every queued job with kUnavailable, unrun, and joins the
  // running ones. Every later Submit fails at once.
  void Shutdown();

  uint32_t threads() const { return config_.threads; }
  // Jobs a thread has taken and not finished.
  size_t RunningJobs() const;
  size_t QueuedForTesting(metrics::IoQueue queue) const;
  // Until no job is queued or running.
  void WaitIdleForTesting();

 private:
  struct Fifo {
    IoJob* head = nullptr;
    IoJob* tail = nullptr;
    size_t size = 0;

    bool empty() const { return head == nullptr; }
    void Push(IoJob& job);
    IoJob* Pop();
    void Remove(IoJob& job);
  };

  void Work();
  // The next job a free thread may start, unlinked; null for none.
  IoJob* Next() ABYSS_REQUIRES(mu_);
  void Depths() ABYSS_REQUIRES(mu_);

  const IoPoolConfig config_;
  mutable std::mutex mu_;
  std::condition_variable work_;
  std::condition_variable idle_;
  Fifo points_ ABYSS_GUARDED_BY(mu_);
  Fifo scans_ ABYSS_GUARDED_BY(mu_);
  Fifo background_ ABYSS_GUARDED_BY(mu_);
  uint32_t scans_running_ ABYSS_GUARDED_BY(mu_) = 0;
  uint32_t running_ ABYSS_GUARDED_BY(mu_) = 0;
  uint64_t next_order_ ABYSS_GUARDED_BY(mu_) = 0;
  bool stopping_ ABYSS_GUARDED_BY(mu_) = false;
  std::vector<std::thread> workers_;

  metrics::GaugeHandle foreground_depth_;
  metrics::GaugeHandle background_depth_;
  metrics::HistogramHandle wait_;
  std::array<metrics::HistogramHandle, metrics::kIoKindCount> run_;
};

}  // namespace abyss::engine
