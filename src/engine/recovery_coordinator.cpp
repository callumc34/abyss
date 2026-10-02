#include "abyss/engine/recovery_coordinator.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.engine.recovery")

namespace abyss::engine {

namespace {

constexpr uint32_t kMinParallelism = 1;

uint64_t SafeDelta(core::SequenceId target, core::SequenceId starting) noexcept {
  return target > starting ? target - starting : 0;
}

}  // namespace

RecoveryCoordinator::RecoveryCoordinator(core::Queue& queue, consumer::ResolverPool& resolver_pool,
                                         consumer::ColdConsumerPool& cold_pool,
                                         consumer::HotConsumerPool& hot_pool,
                                         ShardScheduler& scheduler, RecoveryConfig config)
    : queue_(queue),
      resolver_pool_(resolver_pool),
      cold_pool_(cold_pool),
      hot_pool_(hot_pool),
      scheduler_(scheduler),
      config_(config) {
  config_.replay_parallelism = std::max(config_.replay_parallelism, kMinParallelism);

  auto& reg = metrics::Registry::Instance();
  phase_gauge_ = reg.Gauge(metrics::names::kRecoveryPhase);
  resolver_replayed_gauge_ = reg.Gauge(metrics::names::kRecoveryResolverEntriesReplayed);
  resolver_target_gauge_ = reg.Gauge(metrics::names::kRecoveryResolverEntriesTarget);
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
  ABYSS_LOG_INFO("recovery starting", {"shard_count", static_cast<int64_t>(hot_pool_.ShardCount())},
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

  if (auto r = RunResolverPhase(cancel); !r.has_value()) {
    TransitionPhase(RecoverySnapshot::Phase::kComplete);
    return r;
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

core::Result<void> RecoveryCoordinator::RunResolverPhase(const std::atomic<bool>& cancel) {
  const uint32_t shards = resolver_pool_.ShardCount();
  auto starting = CaptureCommittedOffsets(core::kResolverConsumer);
  auto targets = CaptureTargets();
  {
    const std::scoped_lock lock(progress_mu_);
    resolver_starting_ = std::move(starting);
    resolver_target_ = std::move(targets);
  }
  resolver_target_gauge_.Set(static_cast<double>(SumDelta(resolver_target_, resolver_starting_)));
  TransitionPhase(RecoverySnapshot::Phase::kResolverReplay);

  std::atomic<bool> failed{false};
  core::Error first_error{core::ErrorCode::kInternal, "no error"};
  std::mutex error_mu;

  for (uint32_t s = 0; s < shards; ++s) {
    scheduler_.Submit(s, [this, s, &cancel, &failed, &first_error, &error_mu] {
      if (failed.load(std::memory_order_acquire) || cancel.load(std::memory_order_acquire)) {
        return;
      }
      auto r = resolver_pool_.ConsumerFor(s).ReplayForRecovery(cancel);
      if (!r.has_value()) {
        const std::scoped_lock lock(error_mu);
        if (!failed.exchange(true, std::memory_order_acq_rel)) {
          first_error = r.error();
        }
      }
    });
  }
  scheduler_.WaitAll();

  if (cancel.load(std::memory_order_acquire)) {
    return std::unexpected(
        core::Error{core::ErrorCode::kUnavailable, "recovery cancelled in resolver phase"});
  }
  if (failed.load(std::memory_order_acquire)) {
    return std::unexpected(first_error);
  }
  resolver_replayed_gauge_.Set(static_cast<double>(SumDelta(resolver_target_, resolver_starting_)));
  return {};
}

core::Result<void> RecoveryCoordinator::RunColdHotPhase(const std::atomic<bool>& cancel) {
  const uint32_t shards = hot_pool_.ShardCount();

  // Captured after the resolver phase, so the Resolveds it emitted lie
  // below the ends and cold and hot drain through them.
  std::vector<core::SequenceId> end(shards, 0);
  std::vector<core::SequenceId> target(shards, 0);
  // Hot commits nothing; it rebuilds from the first retained seq.
  std::vector<core::SequenceId> first(shards, 0);
  for (uint32_t s = 0; s < shards; ++s) {
    auto durable = queue_.DurableEnd(s, core::Durability::kProcessCrash);
    if (!durable.has_value()) return std::unexpected(durable.error());
    end[s] = *durable;
    target[s] = end[s] > 0 ? end[s] - 1 : 0;
    auto retained = queue_.FirstSeq(s);
    if (!retained.has_value()) return std::unexpected(retained.error());
    first[s] = *retained;
  }
  auto cold_starting = CaptureCommittedOffsets(core::kColdConsumer);
  {
    const std::scoped_lock lock(progress_mu_);
    cold_starting_ = std::move(cold_starting);
    hot_starting_ = std::move(first);
    cold_target_ = target;
    hot_target_ = std::move(target);
  }
  cold_target_gauge_.Set(static_cast<double>(SumDelta(cold_target_, cold_starting_)));
  hot_target_gauge_.Set(static_cast<double>(SumDelta(hot_target_, hot_starting_)));
  TransitionPhase(RecoverySnapshot::Phase::kColdHotReplay);

  if (auto scanned = ScanColdHot(end, cancel); !scanned.has_value()) {
    if (cancel.load(std::memory_order_acquire)) {
      return std::unexpected(
          core::Error{core::ErrorCode::kUnavailable, "recovery cancelled in cold/hot phase"});
    }
    return scanned;
  }

  std::atomic<bool> failed{false};
  core::Error first_error{core::ErrorCode::kInternal, "no error"};
  std::mutex error_mu;

  auto submit_replay = [&](core::ShardId shard, auto replay_fn) {
    scheduler_.Submit(
        shard, [&failed, &first_error, &error_mu, &cancel, fn = std::move(replay_fn)] {
          if (failed.load(std::memory_order_acquire) || cancel.load(std::memory_order_acquire)) {
            return;
          }
          auto r = fn();
          if (!r.has_value()) {
            const std::scoped_lock lock(error_mu);
            if (!failed.exchange(true, std::memory_order_acq_rel)) {
              first_error = r.error();
            }
          }
        });
  };
  // The scan delivered everything below the ends, so ReplayUntil must
  // find nothing; a regression shows up here instead of being absorbed.
  const auto expect_none = [](std::string_view tier, core::ShardId shard,
                              const core::Result<uint64_t>& replayed) -> core::Result<void> {
    if (!replayed.has_value()) return std::unexpected(replayed.error());
    if (*replayed > 0) {
      ABYSS_LOG_WARN("replay after the recovery scan found entries it missed", {"tier", tier},
                     {"shard", static_cast<int64_t>(shard)}, {"entries", *replayed});
    }
    return {};
  };

  for (uint32_t s = 0; s < shards; ++s) {
    const bool empty = end[s] == 0;
    submit_replay(s, [this, s, empty, &cancel, &expect_none]() -> core::Result<void> {
      consumer::ColdConsumer& cold = cold_pool_.ConsumerFor(s);
      if (auto finished = cold.FinishReplay(cancel); !finished.has_value()) return finished;
      if (empty) return {};
      return expect_none("cold", s, cold.ReplayUntil(cold_target_[s], cancel));
    });
    if (empty) continue;
    submit_replay(s, [this, s, &cancel, &expect_none] {
      return expect_none("hot", s, hot_pool_.ConsumerFor(s).ReplayUntil(hot_target_[s], cancel));
    });
  }
  scheduler_.WaitAll();

  if (cancel.load(std::memory_order_acquire)) {
    return std::unexpected(
        core::Error{core::ErrorCode::kUnavailable, "recovery cancelled in cold/hot phase"});
  }
  if (failed.load(std::memory_order_acquire)) {
    return std::unexpected(first_error);
  }
  cold_replayed_gauge_.Set(static_cast<double>(SumDelta(cold_target_, cold_starting_)));
  hot_replayed_gauge_.Set(static_cast<double>(SumDelta(hot_target_, hot_starting_)));
  return {};
}

core::Result<void> RecoveryCoordinator::ScanColdHot(const std::vector<core::SequenceId>& end,
                                                    const std::atomic<bool>& cancel) {
  const uint32_t shards = hot_pool_.ShardCount();
  std::vector<core::SequenceId> from_cold(shards, 0);
  std::vector<core::SequenceId> from(shards, 0);
  for (uint32_t s = 0; s < shards; ++s) {
    auto begun = cold_pool_.ConsumerFor(s).BeginReplay();
    if (!begun.has_value()) return std::unexpected(begun.error());
    from_cold[s] = *begun;
    from[s] = std::min(hot_starting_[s], from_cold[s]);
  }

  // The scan moves each hot cursor from its start to the end; one it
  // left short is what the trailing ReplayUntil reports.
  for (uint32_t s = 0; s < shards; ++s) {
    consumer::HotConsumer& hot = hot_pool_.ConsumerFor(s);
    hot.SetReplayCursor(hot_starting_[s]);
    hot.BeginReplay();
  }
  const auto by_seq = [](const core::QueueEntry& entry) { return entry.seq; };
  const core::Queue::ScanSink sink = [&](core::ShardId shard,
                                         std::vector<core::QueueEntry>& batch) {
    // Cold first: hot moves entries out of the batch it applies.
    const std::span<const core::QueueEntry> entries(batch);
    const auto cold_at = std::ranges::lower_bound(entries, from_cold[shard], {}, by_seq);
    if (auto applied = cold_pool_.ConsumerFor(shard).ApplyReplayBatch(
            entries.subspan(static_cast<std::size_t>(cold_at - entries.begin())), cancel);
        !applied.has_value()) {
      return applied;
    }
    batch.erase(batch.begin(), std::ranges::lower_bound(batch, hot_starting_[shard], {}, by_seq));
    hot_pool_.ConsumerFor(shard).ApplyReplayBatch(batch);
    return core::Result<void>{};
  };
  auto scanned = queue_.Scan(from, end, config_.replay_parallelism, sink, cancel);
  for (uint32_t s = 0; s < shards; ++s) hot_pool_.ConsumerFor(s).EndReplay();
  return scanned;
}

std::vector<core::SequenceId> RecoveryCoordinator::CaptureTargets() const {
  const uint32_t shards = hot_pool_.ShardCount();
  std::vector<core::SequenceId> out(shards, 0);
  for (uint32_t s = 0; s < shards; ++s) {
    auto t = queue_.TailSeq(s);
    if (t.has_value()) out[s] = *t;
  }
  return out;
}

std::vector<core::SequenceId> RecoveryCoordinator::CaptureCommittedOffsets(
    core::ConsumerId consumer) const {
  const uint32_t shards = hot_pool_.ShardCount();
  std::vector<core::SequenceId> out(shards, 0);
  for (uint32_t s = 0; s < shards; ++s) {
    auto committed = queue_.CommittedOffset(consumer, s);
    if (committed.has_value()) out[s] = committed->value_or(0);
  }
  return out;
}

uint64_t RecoveryCoordinator::SumDelta(const std::vector<core::SequenceId>& target,
                                       const std::vector<core::SequenceId>& starting) noexcept {
  uint64_t total = 0;
  const auto n = std::min(target.size(), starting.size());
  for (size_t i = 0; i < n; ++i) total += SafeDelta(target[i], starting[i]);
  return total;
}

RecoverySnapshot RecoveryCoordinator::Snapshot() const {
  RecoverySnapshot snap;
  snap.phase = phase_.load(std::memory_order_acquire);
  const std::scoped_lock lock(progress_mu_);

  if (started_at_.time_since_epoch().count() != 0) {
    snap.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started_at_);
  }

  snap.resolver_entries_target = SumDelta(resolver_target_, resolver_starting_);
  snap.cold_entries_target = SumDelta(cold_target_, cold_starting_);
  snap.hot_entries_target = SumDelta(hot_target_, hot_starting_);

  // Live progress: poll each consumer's drain marker. Bounded by shard_count;
  // /status callers tolerate this.
  for (uint32_t s = 0; s < resolver_pool_.ShardCount() && s < resolver_starting_.size(); ++s) {
    const auto drained = resolver_pool_.ConsumerFor(s).GetSnapshot().latest_drained_seq;
    if (drained > resolver_starting_[s]) {
      snap.resolver_entries_replayed += drained - resolver_starting_[s];
    }
  }
  for (uint32_t s = 0; s < cold_pool_.ShardCount() && s < cold_starting_.size(); ++s) {
    const auto drained = cold_pool_.ConsumerFor(s).Snapshot().latest_drained_seq;
    if (drained > cold_starting_[s]) {
      snap.cold_entries_replayed += drained - cold_starting_[s];
    }
  }
  for (uint32_t s = 0; s < hot_pool_.ShardCount() && s < hot_starting_.size(); ++s) {
    const auto settled = hot_pool_.ConsumerFor(s).HighestSettledSeq();
    if (settled > hot_starting_[s]) {
      snap.hot_entries_replayed += settled - hot_starting_[s];
    }
  }
  return snap;
}

}  // namespace abyss::engine
