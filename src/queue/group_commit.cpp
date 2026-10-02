#include "abyss/queue/group_commit.h"

#include <chrono>
#include <string>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.queue.group_commit")

namespace abyss::queue {

namespace {

constexpr std::chrono::milliseconds kSlowFlushThreshold{50};

}  // namespace

GroupCommitter::GroupCommitter(LogPosition durable, FlushFn flush, FlushedFn on_flushed)
    : flush_(std::move(flush)),
      on_flushed_(std::move(on_flushed)),
      flush_duration_(
          metrics::Registry::Instance().Histogram(metrics::names::kWalFlushDurationSeconds)),
      flush_batch_entries_(
          metrics::Registry::Instance().Histogram(metrics::names::kWalFlushBatchEntries)),
      published_end_(durable),
      durable_end_(durable),
      durable_(durable),
      thread_([this] { Run(); }) {}

// Only a std::system_error from a lock or the join can escape, and
// terminating on that at teardown is the right outcome.
// NOLINTNEXTLINE(bugprone-exception-escape)
GroupCommitter::~GroupCommitter() { Stop(/*final_flush=*/true); }

void GroupCommitter::Published(LogPosition end) noexcept {
  LogPosition current = published_end_.load(std::memory_order_relaxed);
  while (end > current && !published_end_.compare_exchange_weak(
                              current, end, std::memory_order_seq_cst, std::memory_order_relaxed)) {
  }
  if (idle_.load(std::memory_order_seq_cst)) {
    const std::scoped_lock lock(wake_mu_);
    wake_cv_.notify_one();
  }
}

void GroupCommitter::Stop(bool final_flush) {
  const std::scoped_lock stop_lock(stop_mu_);
  if (!thread_.joinable()) return;
  {
    const std::scoped_lock lock(wake_mu_);
    stop_ = true;
    final_flush_ = final_flush;
  }
  wake_cv_.notify_one();
  thread_.join();
}

void GroupCommitter::Run() {
  while (true) {
    {
      std::unique_lock lock(wake_mu_);
      if (!stop_ && published_end_.load(std::memory_order_seq_cst) <= durable_) {
        idle_.store(true, std::memory_order_seq_cst);
        wake_cv_.wait(lock, [this] ABYSS_REQUIRES(wake_mu_) {
          return stop_ || published_end_.load(std::memory_order_seq_cst) > durable_;
        });
        idle_.store(false, std::memory_order_relaxed);
      }
      if (stop_) {
        const bool final_flush = final_flush_;
        lock.unlock();
        if (final_flush && published_end_.load(std::memory_order_acquire) > durable_) {
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
  const LogPosition previous = durable_;
  RecordFlush(elapsed, flushed->entries);
  if (elapsed > kSlowFlushThreshold) {
    const uint64_t bytes = flushed->end > previous ? flushed->end - previous : 0;
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    ABYSS_LOG_WARN("slow WAL flush", {"entries", flushed->entries}, {"bytes", bytes},
                   {"duration_ms", static_cast<int64_t>(ms)});
  }
  if (flushed->end <= previous) return;
  durable_ = flushed->end;
  if (on_flushed_) on_flushed_(previous, *flushed);
  durable_end_.store(flushed->end, std::memory_order_release);
}

void GroupCommitter::RecordFlush(std::chrono::steady_clock::duration elapsed,
                                 uint64_t entries) noexcept {
  flush_duration_.Observe(std::chrono::duration<double>(elapsed).count());
  if (entries > 0) flush_batch_entries_.Observe(static_cast<double>(entries));
}

}  // namespace abyss::queue
