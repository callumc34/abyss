#include "abyss/engine/io_pool.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include "abyss/core/fatal.h"

namespace abyss::engine {

namespace {

using metrics::IoKind;

double Seconds(std::chrono::steady_clock::duration d) {
  return std::chrono::duration<double>(d).count();
}

core::Error Expired() {
  return core::Error{core::ErrorCode::kTimeout,
                     "command deadline passed while queued for cold I/O"};
}

core::Error Stopped() {
  return core::Error{core::ErrorCode::kUnavailable, "cold I/O pool shut down"};
}

}  // namespace

uint32_t ColdIoThreads(uint32_t configured, uint32_t hardware) {
  if (configured != 0) return configured;
  return std::min<uint32_t>(4, std::max<uint32_t>(2, hardware));
}

void IoPool::Fifo::Push(IoJob& job) {
  job.next_ = nullptr;
  job.prev_ = tail;
  if (tail == nullptr) {
    head = &job;
  } else {
    tail->next_ = &job;
  }
  tail = &job;
  ++size;
}

IoJob* IoPool::Fifo::Pop() {
  IoJob* const job = head;
  Remove(*job);
  return job;
}

void IoPool::Fifo::Remove(IoJob& job) {
  if (job.prev_ == nullptr) {
    head = job.next_;
  } else {
    job.prev_->next_ = job.next_;
  }
  if (job.next_ == nullptr) {
    tail = job.prev_;
  } else {
    job.next_->prev_ = job.prev_;
  }
  job.prev_ = nullptr;
  job.next_ = nullptr;
  --size;
}

IoPool::IoPool(IoPoolConfig config) : config_(config) {
  ABYSS_DCHECK(config_.threads >= 2, "a cold I/O pool needs a thread scans cannot take");
  auto& reg = metrics::Registry::Instance();
  foreground_depth_ = reg.Gauge(metrics::names::kEngineIoQueueDepth, metrics::IoQueue::kForeground);
  background_depth_ = reg.Gauge(metrics::names::kEngineIoQueueDepth, metrics::IoQueue::kBackground);
  foreground_depth_.Set(0);
  background_depth_.Set(0);
  wait_ = reg.Histogram(metrics::names::kEngineIoWaitSeconds);
  for (const IoKind kind : {IoKind::kPoint, IoKind::kScan, IoKind::kLoad, IoKind::kFill}) {
    run_.at(static_cast<size_t>(kind)) = reg.Histogram(metrics::names::kEngineIoRunSeconds, kind);
  }
  workers_.reserve(config_.threads);
  for (uint32_t i = 0; i < config_.threads; ++i) workers_.emplace_back([this] { Work(); });
}

// NOLINTNEXTLINE(bugprone-exception-escape): a failed join may terminate.
IoPool::~IoPool() { Shutdown(); }

void IoPool::Submit(IoJob& job, IoKind kind, core::SteadyTime deadline) {
  ABYSS_DCHECK(kind != IoKind::kFill, "a fill submitted to the foreground");
  {
    const std::scoped_lock lock(mu_);
    ABYSS_DCHECK(!job.queued_, "an I/O job submitted twice");
    if (!stopping_) {
      job.kind_ = kind;
      job.deadline_ = deadline;
      job.enqueued_ = core::SteadyClock::now();
      job.order_ = next_order_++;
      job.queued_ = true;
      (kind == IoKind::kScan ? scans_ : points_).Push(job);
      Depths();
      work_.notify_one();
      return;
    }
  }
  job.Fail(Stopped());
}

void IoPool::SubmitBackground(std::unique_ptr<IoJob> job, core::SteadyTime deadline) {
  core::Error refused = Stopped();
  {
    const std::scoped_lock lock(mu_);
    if (!stopping_ && background_.size < config_.background_capacity) {
      IoJob& queued = *job.release();
      queued.kind_ = IoKind::kFill;
      queued.deadline_ = deadline;
      queued.enqueued_ = core::SteadyClock::now();
      queued.queued_ = true;
      background_.Push(queued);
      Depths();
      work_.notify_one();
      return;
    }
    if (!stopping_) {
      refused = core::Error{core::ErrorCode::kResourceExhausted, "background I/O queue full"};
    }
  }
  job->Fail(std::move(refused));
}

bool IoPool::Cancel(IoJob& job) {
  const std::scoped_lock lock(mu_);
  if (!job.queued_) return false;
  ABYSS_DCHECK(job.kind_ != IoKind::kFill, "a fill cancelled: the pool owns it");
  (job.kind_ == IoKind::kScan ? scans_ : points_).Remove(job);
  job.queued_ = false;
  Depths();
  if (running_ == 0 && points_.empty() && scans_.empty() && background_.empty()) {
    idle_.notify_all();
  }
  return true;
}

void IoPool::Shutdown() {
  std::vector<IoJob*> foreground;
  std::vector<std::unique_ptr<IoJob>> background;
  {
    const std::scoped_lock lock(mu_);
    stopping_ = true;
    // Both foreground queues, oldest first.
    while (!points_.empty() || !scans_.empty()) {
      const bool point =
          scans_.empty() || (!points_.empty() && points_.head->order_ < scans_.head->order_);
      IoJob* const job = (point ? points_ : scans_).Pop();
      job->queued_ = false;
      foreground.push_back(job);
    }
    while (!background_.empty()) {
      IoJob* const job = background_.Pop();
      job->queued_ = false;
      background.emplace_back(job);
    }
    Depths();
  }
  work_.notify_all();
  // Each fails once, and is not touched after: its owner may reuse it.
  for (IoJob* const job : foreground) job->Fail(Stopped());
  for (const std::unique_ptr<IoJob>& job : background) job->Fail(Stopped());
  background.clear();
  for (std::thread& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
}

size_t IoPool::QueuedForTesting(metrics::IoQueue queue) const {
  const std::scoped_lock lock(mu_);
  if (queue == metrics::IoQueue::kBackground) return background_.size;
  return points_.size + scans_.size;
}

void IoPool::WaitIdleForTesting() {
  std::unique_lock lock(mu_);
  idle_.wait(lock, [this] ABYSS_REQUIRES(mu_) {
    return running_ == 0 && points_.empty() && scans_.empty() && background_.empty();
  });
}

void IoPool::Work() {
  std::unique_lock lock(mu_);
  for (;;) {
    IoJob* const job = Next();
    if (job == nullptr) {
      if (stopping_) return;
      work_.wait(lock);
      continue;
    }
    // Read before the call: a foreground job's owner may reuse it as
    // soon as it posts.
    const IoKind kind = job->kind_;
    const core::SteadyTime deadline = job->deadline_;
    const core::SteadyTime enqueued = job->enqueued_;
    if (kind == IoKind::kScan) ++scans_running_;
    ++running_;
    Depths();
    lock.unlock();

    const core::SteadyTime start = core::SteadyClock::now();
    wait_.Observe(Seconds(start - enqueued));
    std::unique_ptr<IoJob> owned(kind == IoKind::kFill ? job : nullptr);
    if (start >= deadline) {
      job->Fail(Expired());
    } else {
      job->Run();
      run_.at(static_cast<size_t>(kind)).Observe(Seconds(core::SteadyClock::now() - start));
    }
    owned.reset();

    lock.lock();
    if (kind == IoKind::kScan) --scans_running_;
    if (--running_ == 0) idle_.notify_all();
  }
}

IoJob* IoPool::Next() {
  IoJob* job = nullptr;
  const bool lane_open = scans_running_ + 1 < config_.threads;
  const bool scan_ready = lane_open && !scans_.empty();
  if (!points_.empty() || scan_ready) {
    const bool point =
        !scan_ready || (!points_.empty() && points_.head->order_ < scans_.head->order_);
    job = (point ? points_ : scans_).Pop();
  } else if (points_.empty() && scans_.empty() && !background_.empty()) {
    job = background_.Pop();
  }
  if (job != nullptr) job->queued_ = false;
  return job;
}

void IoPool::Depths() {
  foreground_depth_.Set(static_cast<double>(points_.size + scans_.size));
  background_depth_.Set(static_cast<double>(background_.size));
}

}  // namespace abyss::engine
