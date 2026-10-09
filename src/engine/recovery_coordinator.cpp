#include "abyss/engine/recovery_coordinator.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/fatal.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.engine.recovery")

namespace abyss::engine {

namespace {

constexpr uint32_t kMinParallelism = 1;

uint64_t SumDelta(const std::vector<core::SequenceId>& target,
                  const std::vector<core::SequenceId>& starting) noexcept {
  uint64_t total = 0;
  const auto n = std::min(target.size(), starting.size());
  for (size_t i = 0; i < n; ++i) {
    if (target[i] > starting[i]) total += target[i] - starting[i];
  }
  return total;
}

}  // namespace

RecoveryCoordinator::RecoveryCoordinator(core::Queue& queue, consumer::ColdConsumerPool& cold_pool,
                                         hot::ShardedHotStore& hot, ShardScheduler& scheduler,
                                         RecoveryConfig config)
    : queue_(queue),
      cold_pool_(cold_pool),
      scheduler_(scheduler),
      config_(std::move(config)),
      replayer_(
          hot,
          [this](core::ShardId shard, core::SequenceId through) {
            return DrainCold(shard, through);
          },
          config_.replayer),
      shard_count_(hot.shard_count()) {
  config_.replay_parallelism = std::max(config_.replay_parallelism, kMinParallelism);

  auto& reg = metrics::Registry::Instance();
  phase_gauge_ = reg.Gauge(metrics::names::kRecoveryPhase);
  cold_replayed_gauge_ = reg.Gauge(metrics::names::kRecoveryColdEntriesReplayed);
  cold_target_gauge_ = reg.Gauge(metrics::names::kRecoveryColdEntriesTarget);
  hot_replayed_gauge_ = reg.Gauge(metrics::names::kRecoveryHotEntriesReplayed);
  hot_target_gauge_ = reg.Gauge(metrics::names::kRecoveryHotEntriesTarget);
  duration_gauge_ = reg.Gauge(metrics::names::kRecoveryDurationSeconds);

  phase_gauge_.Set(static_cast<double>(RecoverySnapshot::Phase::kQueueOpen));
}

core::Result<void> RecoveryCoordinator::Run(const std::atomic<bool>& cancel) {
  {
    const std::scoped_lock lock(progress_mu_);
    started_at_ = std::chrono::steady_clock::now();
  }
  ABYSS_LOG_INFO("recovery starting", {"shard_count", static_cast<int64_t>(shard_count_)},
                 {"replay_parallelism", static_cast<int64_t>(config_.replay_parallelism)});

  // Queue self-recovery is synchronous: WalQueue::Open drained any torn tail
  // before this method was called. The phase is observable for operator
  // tooling and for future Queue backends that might need an async handshake;
  // today it is a transient marker.
  TransitionPhase(RecoverySnapshot::Phase::kQueueOpen);
  if (cancel.load(std::memory_order_acquire)) {
    TransitionPhase(RecoverySnapshot::Phase::kComplete);
    return std::unexpected(
        core::Error{core::ErrorCode::kUnavailable, "recovery cancelled before start"});
  }

  if (auto r = RunColdHotPhase(cancel); !r.has_value()) {
    TransitionPhase(RecoverySnapshot::Phase::kComplete);
    return r;
  }

  TransitionPhase(RecoverySnapshot::Phase::kComplete);
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started_at_)
                              .count();
  duration_gauge_.Set(static_cast<double>(elapsed_ms) / 1000.0);
  ABYSS_LOG_INFO("recovery complete", {"elapsed_ms", static_cast<int64_t>(elapsed_ms)});
  return {};
}

void RecoveryCoordinator::TransitionPhase(RecoverySnapshot::Phase next) {
  phase_.store(next, std::memory_order_release);
  phase_gauge_.Set(static_cast<double>(next));
}

core::Result<void> RecoveryCoordinator::RunColdHotPhase(const std::atomic<bool>& cancel) {
  const uint32_t shards = shard_count_;
  std::vector<core::SequenceId> end(shards, 0);
  // Hot commits nothing; it rebuilds from the first retained seq.
  std::vector<core::SequenceId> first(shards, 0);
  std::vector<core::SequenceId> cold_target(shards, 0);
  for (uint32_t s = 0; s < shards; ++s) {
    auto durable = queue_.DurableEnd(s, core::Durability::kProcessCrash);
    if (!durable.has_value()) return std::unexpected(durable.error());
    end[s] = *durable;
    ABYSS_DCHECK(end[s] >= core::kFirstSeq, "a durable end below the first seq");
    // Cold's last seq to drain, against its last committed one.
    cold_target[s] = end[s] - 1;
    auto retained = queue_.FirstSeq(s);
    if (!retained.has_value()) return std::unexpected(retained.error());
    first[s] = *retained;
  }
  auto cold_starting = CaptureColdOffsets();
  {
    const std::scoped_lock lock(progress_mu_);
    cold_starting_ = std::move(cold_starting);
    cold_target_ = std::move(cold_target);
    hot_target_ = SumDelta(end, first);
  }
  cold_target_gauge_.Set(static_cast<double>(SumDelta(cold_target_, cold_starting_)));
  hot_target_gauge_.Set(static_cast<double>(hot_target_));
  TransitionPhase(RecoverySnapshot::Phase::kColdHotReplay);

  const auto cancelled = [] {
    return std::unexpected(
        core::Error{core::ErrorCode::kUnavailable, "recovery cancelled in cold/hot phase"});
  };
  if (auto scanned = ScanColdHot(first, end, cancel); !scanned.has_value()) {
    if (cancel.load(std::memory_order_acquire)) return cancelled();
    return scanned;
  }

  std::atomic<bool> failed{false};
  core::Error first_error{core::ErrorCode::kInternal, "no error"};
  std::mutex error_mu;
  for (uint32_t s = 0; s < shards; ++s) {
    scheduler_.Submit(s, [this, s, &end, &cancel, &failed, &first_error, &error_mu] {
      if (failed.load(std::memory_order_acquire) || cancel.load(std::memory_order_acquire)) {
        return;
      }
      auto finished = cold_pool_.ConsumerFor(s).FinishReplay(end[s], cancel);
      if (!finished.has_value()) {
        const std::scoped_lock lock(error_mu);
        if (!failed.exchange(true, std::memory_order_acq_rel)) first_error = finished.error();
      }
    });
  }
  scheduler_.WaitAll();
  if (cancel.load(std::memory_order_acquire)) return cancelled();
  if (failed.load(std::memory_order_acquire)) return std::unexpected(first_error);

  // After cold flushed everything, so the sweep evicts what it finds due.
  if (auto swept = replayer_.Finish(); !swept.has_value()) return swept;
  cold_replayed_gauge_.Set(static_cast<double>(SumDelta(cold_target_, cold_starting_)));
  hot_replayed_gauge_.Set(static_cast<double>(replayer_.Replayed()));
  return {};
}

core::Result<void> RecoveryCoordinator::ScanColdHot(const std::vector<core::SequenceId>& first,
                                                    const std::vector<core::SequenceId>& end,
                                                    const std::atomic<bool>& cancel) {
  const uint32_t shards = shard_count_;
  std::vector<core::SequenceId> from_cold(shards, 0);
  std::vector<core::SequenceId> from(shards, 0);
  for (uint32_t s = 0; s < shards; ++s) {
    auto begun = cold_pool_.ConsumerFor(s).BeginReplay();
    if (!begun.has_value()) return std::unexpected(begun.error());
    from_cold[s] = *begun;
    from[s] = std::min(first[s], from_cold[s]);
  }
  replayer_.Begin(first, end);
  const auto by_seq = [](const core::QueueEntry& entry) { return entry.seq; };
  const core::Queue::ScanSink sink = [&](core::ShardId shard,
                                         std::vector<core::QueueEntry>& batch) {
    // Cold first: its drained seq then covers the batch, so hot may
    // evict what the batch writes, and the replayer moves payloads out.
    const std::span<const core::QueueEntry> entries(batch);
    const auto cold_at = std::ranges::lower_bound(entries, from_cold[shard], {}, by_seq);
    if (auto applied = cold_pool_.ConsumerFor(shard).ApplyReplayBatch(
            entries.subspan(static_cast<std::size_t>(cold_at - entries.begin())), cancel);
        !applied.has_value()) {
      return applied;
    }
    batch.erase(batch.begin(), std::ranges::lower_bound(batch, first[shard], {}, by_seq));
    return replayer_.Apply(shard, batch);
  };
  return queue_.Scan(from, end, config_.replay_parallelism, sink, cancel);
}

core::Result<void> RecoveryCoordinator::DrainCold(core::ShardId shard, core::SequenceId through) {
  // Recovery synced the log it read, so this cap only guards the gate.
  auto power = queue_.DurableEnd(shard, core::Durability::kPowerLoss);
  if (!power.has_value()) return std::unexpected(power.error());
  if (*power == core::kFirstSeq) return {};
  return cold_pool_.ConsumerFor(shard).FlushThrough(std::min(through, *power - 1));
}

std::vector<core::SequenceId> RecoveryCoordinator::CaptureColdOffsets() const {
  std::vector<core::SequenceId> out(shard_count_, 0);
  for (uint32_t s = 0; s < shard_count_; ++s) {
    auto committed = queue_.CommittedOffset(core::kColdConsumer, s);
    if (committed.has_value()) out[s] = committed->value_or(0);
  }
  return out;
}

RecoverySnapshot RecoveryCoordinator::Snapshot() const {
  RecoverySnapshot snap;
  snap.phase = phase_.load(std::memory_order_acquire);
  const std::scoped_lock lock(progress_mu_);

  if (started_at_.time_since_epoch().count() != 0) {
    snap.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started_at_);
  }

  snap.cold_entries_target = SumDelta(cold_target_, cold_starting_);
  snap.hot_entries_target = hot_target_;

  // Live progress: poll each consumer's drain marker. Bounded by shard_count;
  // /status callers tolerate this.
  for (uint32_t s = 0; s < cold_pool_.ShardCount() && s < cold_starting_.size(); ++s) {
    const auto drained = cold_pool_.ConsumerFor(s).Snapshot().latest_drained_seq;
    if (drained > cold_starting_[s]) {
      snap.cold_entries_replayed += drained - cold_starting_[s];
    }
  }
  snap.hot_entries_replayed = replayer_.Replayed();
  return snap;
}

}  // namespace abyss::engine
