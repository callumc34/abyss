#include "abyss/queue/group_commit.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.queue.group_commit")

namespace abyss::queue {

namespace {

constexpr std::chrono::milliseconds kSlowFsyncThreshold{50};

DurabilityFuture MakeReadyFuture(core::Result<void> value) {
  std::promise<core::Result<void>> p;
  p.set_value(std::move(value));
  return p.get_future();
}

}  // namespace

GroupCommitter::GroupCommitter(GroupCommitConfig config, FsyncFn fsync_fn)
    : config_(config), fsync_fn_(std::move(fsync_fn)) {
  auto& reg = metrics::Registry::Instance();
  flush_duration_ = reg.Histogram(metrics::names::kWalFlushDurationSeconds);
  flush_batch_entries_ = reg.Histogram(metrics::names::kWalFlushBatchEntries);
  if (config_.policy == FsyncPolicy::kGroupCommit) {
    thread_ = std::thread([this] { Run(); });
  }
}

GroupCommitter::~GroupCommitter() { Stop(); }

DurabilityFuture GroupCommitter::Submit(size_t bytes, size_t entries,
                                        core::SequenceId batch_last_seq) {
  switch (config_.policy) {
    case FsyncPolicy::kNone:
      // No durability barrier, but advance the watermark to the published seq
      // so the retention-commit gate is a correct no-op (Decision 1). The
      // operator is warned at startup that durability is disabled.
      PublishDurable(batch_last_seq);
      return MakeReadyFuture({});

    case FsyncPolicy::kPerWrite: {
      const auto start = std::chrono::steady_clock::now();
      auto result = fsync_fn_();
      RecordFlush(std::chrono::steady_clock::now() - start, entries);
      if (result.has_value()) {
        PublishDurable(batch_last_seq);
      }
      return MakeReadyFuture(std::move(result));
    }

    case FsyncPolicy::kGroupCommit: {
      std::promise<core::Result<void>> promise;
      auto future = promise.get_future();
      {
        const std::scoped_lock lock(mu_);
        if (stopped_) {
          return MakeReadyFuture(std::unexpected(
              core::Error{core::ErrorCode::kUnavailable, "group committer stopped"}));
        }
        pending_.push_back(
            {.promise = std::move(promise), .bytes = bytes, .batch_last_seq = batch_last_seq});
        pending_bytes_ += bytes;
        pending_entries_ += entries;
        batch_high_seq_ = std::max(batch_high_seq_, batch_last_seq);
        if (pending_bytes_ >= config_.max_bytes) {
          flush_requested_ = true;
        }
      }
      cv_.notify_one();
      return future;
    }
  }
  return MakeReadyFuture(
      std::unexpected(core::Error{core::ErrorCode::kInternal, "unknown fsync policy"}));
}

bool GroupCommitter::AwaitDurable(core::SequenceId seq, std::chrono::nanoseconds timeout) const {
  const auto ready = [this, seq] { return HasDurable() && DurableSeq() >= seq; };
  if (ready()) return true;
  std::unique_lock lock(durable_mu_);
  return durable_cv_.wait_for(lock, timeout, ready);
}

void GroupCommitter::PublishDurable(core::SequenceId seq) {
  // The first published seq makes the watermark meaningful even at value 0.
  has_durable_.store(true, std::memory_order_release);
  // Monotonic CAS-max: never regress, and wake awaiters only when we advance.
  core::SequenceId current = durable_seq_.load(std::memory_order_relaxed);
  while (seq > current) {
    if (durable_seq_.compare_exchange_weak(current, seq, std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
      const std::scoped_lock lock(durable_mu_);
      durable_cv_.notify_all();
      return;
    }
  }
  // seq did not advance the watermark (e.g. a smaller seq, or seq 0 when the
  // watermark is already 0). Still wake awaiters: HasDurable just became true.
  const std::scoped_lock lock(durable_mu_);
  durable_cv_.notify_all();
}

void GroupCommitter::RecordFlush(std::chrono::steady_clock::duration elapsed,
                                 size_t entries) noexcept {
  flush_duration_.Observe(std::chrono::duration<double>(elapsed).count());
  // A Drain-only flush covers no entries; 0 would blur the le=1 bucket.
  if (entries > 0) flush_batch_entries_.Observe(static_cast<double>(entries));
}

core::Result<void> GroupCommitter::Drain() {
  if (config_.policy == FsyncPolicy::kNone) {
    return {};
  }
  if (config_.policy == FsyncPolicy::kPerWrite) {
    const auto start = std::chrono::steady_clock::now();
    auto result = fsync_fn_();
    RecordFlush(std::chrono::steady_clock::now() - start, 0);
    return result;
  }

  std::promise<core::Result<void>> promise;
  auto future = promise.get_future();
  {
    const std::scoped_lock lock(mu_);
    if (stopped_) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "group committer stopped"});
    }
    pending_.push_back({.promise = std::move(promise), .bytes = 0});
    flush_requested_ = true;
  }
  cv_.notify_one();
  return future.get();
}

void GroupCommitter::Stop() {
  {
    const std::scoped_lock lock(mu_);
    if (stopped_) return;
    stopped_ = true;
    flush_requested_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void GroupCommitter::Run() {
  std::unique_lock lock(mu_);
  while (true) {
    cv_.wait(lock, [this] { return !pending_.empty() || stopped_; });

    if (pending_.empty()) {
      // stopped_ must be true here.
      return;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const bool should_coalesce =
        !stopped_ && config_.interval.count() > 0 && pending_bytes_ < config_.max_bytes;
    if (should_coalesce) {
      cv_.wait_for(lock, config_.interval, [this] {
        return stopped_ || flush_requested_ || pending_bytes_ >= config_.max_bytes;
      });
    }

    auto batch = std::exchange(pending_, {});
    const size_t batch_bytes = pending_bytes_;
    const size_t batch_entries = std::exchange(pending_entries_, 0);
    const core::SequenceId flushed_high_seq = batch_high_seq_;
    pending_bytes_ = 0;
    batch_high_seq_ = 0;
    flush_requested_ = false;
    auto fsync_fn = fsync_fn_;
    lock.unlock();

    const auto start = std::chrono::steady_clock::now();
    core::Result<void> result = fsync_fn();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    // Before the promises resolve, so a woken writer sees this flush.
    RecordFlush(elapsed, batch_entries);

    if (!result.has_value()) {
      ABYSS_LOG_ERROR("fsync failed", {"batch", static_cast<uint64_t>(batch.size())},
                      {"bytes", static_cast<uint64_t>(batch_bytes)},
                      {"err", std::string_view{result.error().message()}});
    } else if (elapsed > kSlowFsyncThreshold) {
      const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
      ABYSS_LOG_WARN("slow fsync", {"batch", static_cast<uint64_t>(batch.size())},
                     {"bytes", static_cast<uint64_t>(batch_bytes)},
                     {"duration_ms", static_cast<int64_t>(ms)});
    }

    // Advance the durable watermark only after the fsync that covers these
    // bytes has landed (invariant: durable_seq never leads stable media).
    if (result.has_value()) {
      PublishDurable(flushed_high_seq);
    }

    for (auto& entry : batch) {
      entry.promise.set_value(result);
    }

    lock.lock();
  }
}

}  // namespace abyss::queue
