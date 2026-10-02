#include "abyss/queue/group_commit.h"

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.queue.group_commit")

namespace abyss::queue {

namespace {

constexpr std::chrono::milliseconds kSlowFlushThreshold{50};

DurabilityFuture MakeReadyFuture(core::Result<void> value) {
  std::promise<core::Result<void>> p;
  p.set_value(std::move(value));
  return p.get_future();
}

core::Error StoppedError() {
  return core::Error{core::ErrorCode::kUnavailable, "WAL group committer stopped"};
}

}  // namespace

GroupCommitter::GroupCommitter(Extent durable, FlushFn flush, FlushedFn on_flushed)
    : flush_(std::move(flush)),
      on_flushed_(std::move(on_flushed)),
      flush_duration_(
          metrics::Registry::Instance().Histogram(metrics::names::kWalFlushDurationSeconds)),
      flush_batch_entries_(
          metrics::Registry::Instance().Histogram(metrics::names::kWalFlushBatchEntries)),
      published_end_(durable.end),
      durable_end_(durable.end),
      durable_(durable),
      thread_([this] { Run(); }) {}

// Only a std::system_error from a lock or the join can escape, and
// terminating on that at teardown is the right outcome.
// NOLINTNEXTLINE(bugprone-exception-escape)
GroupCommitter::~GroupCommitter() { Stop(/*final_flush=*/true); }

void GroupCommitter::Published(core::SequenceId end) noexcept {
  core::SequenceId current = published_end_.load(std::memory_order_relaxed);
  while (end > current && !published_end_.compare_exchange_weak(
                              current, end, std::memory_order_seq_cst, std::memory_order_relaxed)) {
  }
  if (idle_.load(std::memory_order_seq_cst)) {
    const std::scoped_lock lock(wake_mu_);
    wake_cv_.notify_one();
  }
}

DurabilityFuture GroupCommitter::WhenDurable(core::SequenceId seq) {
  const std::scoped_lock lock(durable_mu_);
  if (seq < durable_end_.load(std::memory_order_relaxed)) return MakeReadyFuture({});
  if (stopped_) return MakeReadyFuture(std::unexpected(StoppedError()));
  std::promise<core::Result<void>> promise;
  auto future = promise.get_future();
  waiters_.emplace_back(seq, std::move(promise));
  return future;
}

bool GroupCommitter::AwaitDurable(core::SequenceId seq, std::chrono::nanoseconds timeout) const {
  if (seq < DurableEnd()) return true;
  std::unique_lock lock(durable_mu_);
  durable_cv_.wait_for(lock, timeout, [this, seq] ABYSS_REQUIRES(durable_mu_) {
    return stopped_ || seq < DurableEnd();
  });
  return seq < DurableEnd();
}

void GroupCommitter::Stop(bool final_flush) {
  const std::scoped_lock stop_lock(stop_mu_);
  if (thread_.joinable()) {
    {
      const std::scoped_lock lock(wake_mu_);
      stop_ = true;
      final_flush_ = final_flush;
    }
    wake_cv_.notify_one();
    thread_.join();
  }

  std::deque<std::pair<core::SequenceId, std::promise<core::Result<void>>>> orphaned;
  {
    const std::scoped_lock lock(durable_mu_);
    stopped_ = true;
    orphaned.swap(waiters_);
  }
  durable_cv_.notify_all();
  for (auto& [seq, promise] : orphaned) promise.set_value(std::unexpected(StoppedError()));
}

void GroupCommitter::Run() {
  while (true) {
    {
      std::unique_lock lock(wake_mu_);
      if (!stop_ && published_end_.load(std::memory_order_seq_cst) <= durable_.end) {
        idle_.store(true, std::memory_order_seq_cst);
        wake_cv_.wait(lock, [this] ABYSS_REQUIRES(wake_mu_) {
          return stop_ || published_end_.load(std::memory_order_seq_cst) > durable_.end;
        });
        idle_.store(false, std::memory_order_relaxed);
      }
      if (stop_) {
        const bool final_flush = final_flush_;
        lock.unlock();
        if (final_flush && published_end_.load(std::memory_order_acquire) > durable_.end) {
          FlushOnce();
        }
        return;
      }
    }
    FlushOnce();
  }
}

void GroupCommitter::FlushOnce() {
  const auto start = std::chrono::steady_clock::now();
  auto flushed = flush_();
  const auto elapsed = std::chrono::steady_clock::now() - start;
  if (!flushed.has_value()) {
    core::Fatal("WAL flush failed: " + flushed.error().message());
  }
  const uint64_t entries = flushed->end > durable_.end ? flushed->end - durable_.end : 0;
  RecordFlush(elapsed, entries);
  if (elapsed > kSlowFlushThreshold) {
    const uint64_t bytes = flushed->bytes > durable_.bytes ? flushed->bytes - durable_.bytes : 0;
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    ABYSS_LOG_WARN("slow WAL flush", {"entries", entries}, {"bytes", bytes},
                   {"duration_ms", static_cast<int64_t>(ms)});
  }
  Advance(*flushed);
}

void GroupCommitter::Advance(Extent flushed) {
  const Extent previous = durable_;
  if (flushed.end <= previous.end) return;
  durable_ = flushed;
  // Before the new end is visible, so its observers see the shard's
  // accounting already settled.
  if (on_flushed_) on_flushed_(previous, flushed);

  std::vector<std::promise<core::Result<void>>> ready;
  {
    const std::scoped_lock lock(durable_mu_);
    durable_end_.store(flushed.end, std::memory_order_release);
    while (!waiters_.empty() && waiters_.front().first < flushed.end) {
      ready.push_back(std::move(waiters_.front().second));
      waiters_.pop_front();
    }
  }
  durable_cv_.notify_all();
  for (auto& promise : ready) promise.set_value({});
}

void GroupCommitter::RecordFlush(std::chrono::steady_clock::duration elapsed,
                                 uint64_t entries) noexcept {
  flush_duration_.Observe(std::chrono::duration<double>(elapsed).count());
  if (entries > 0) flush_batch_entries_.Observe(static_cast<double>(entries));
}

}  // namespace abyss::queue
