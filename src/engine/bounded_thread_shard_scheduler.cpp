#include "abyss/engine/bounded_thread_shard_scheduler.h"

#include <algorithm>
#include <utility>

namespace abyss::engine {

namespace {
constexpr uint32_t kMinWorkers = 1;
}  // namespace

BoundedThreadShardScheduler::BoundedThreadShardScheduler(uint32_t worker_count) {
  const uint32_t n = std::max(worker_count, kMinWorkers);
  workers_.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    workers_.emplace_back([this] { RunWorker(); });
  }
}

BoundedThreadShardScheduler::~BoundedThreadShardScheduler() {
  {
    const std::scoped_lock lock(mu_);
    shutting_down_ = true;
  }
  not_empty_.notify_all();
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
}

void BoundedThreadShardScheduler::Submit(core::ShardId shard, std::function<void()> work) {
  {
    const std::scoped_lock lock(mu_);
    tasks_.push(Task{.shard = shard, .work = std::move(work)});
    ++in_flight_;
  }
  not_empty_.notify_one();
}

void BoundedThreadShardScheduler::WaitAll() {
  std::unique_lock lock(mu_);
  idle_.wait(lock, [this] ABYSS_REQUIRES(mu_) { return in_flight_ == 0; });
}

void BoundedThreadShardScheduler::RunWorker() {
  while (true) {
    Task task;
    {
      std::unique_lock lock(mu_);
      not_empty_.wait(lock,
                      [this] ABYSS_REQUIRES(mu_) { return shutting_down_ || !tasks_.empty(); });
      if (tasks_.empty()) {
        // shutting down with no work
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
    }

    if (task.work) task.work();

    {
      const std::scoped_lock lock(mu_);
      --in_flight_;
      if (in_flight_ == 0) idle_.notify_all();
    }
  }
}

}  // namespace abyss::engine
