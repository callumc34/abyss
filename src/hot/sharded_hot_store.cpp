#include "abyss/hot/sharded_hot_store.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <utility>
#include <variant>

#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/log/log.h"
#include "abyss/queue/reservation.h"

ABYSS_LOG_COMPONENT("abyss.hot.store")

namespace abyss::hot {

namespace {

// A blocking caller's waiter, on its stack. Wake notifies under mu_,
// and the caller destroys it only after relocking mu_ and seeing done_,
// so no ender is still inside cv_ by then.
class BlockingLoadWaiter final : public LoadWaiter {
 public:
  void Wake(core::Result<void> result) noexcept override {
    const std::scoped_lock lock(mu_);
    result_ = std::move(result);
    done_ = true;
    cv_.notify_one();
  }

  bool WaitUntil(core::SteadyTime deadline) {
    std::unique_lock lock(mu_);
    return cv_.wait_until(lock, deadline, [this] ABYSS_REQUIRES(mu_) { return done_; });
  }

  core::Result<void> Take() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] ABYSS_REQUIRES(mu_) { return done_; });
    return result_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  core::Result<void> result_ ABYSS_GUARDED_BY(mu_);
  bool done_ ABYSS_GUARDED_BY(mu_) = false;
};

// Wakes the load waits a hold ended, however the hold is left, a throw
// included: taken while `lock` holds the shard, woken once it is not.
class WakeEnded {
 public:
  WakeEnded(SingleShardStore& store, std::unique_lock<std::shared_mutex>& lock)
      : store_(&store), lock_(&lock) {}
  WakeEnded(const WakeEnded&) = delete;
  WakeEnded& operator=(const WakeEnded&) = delete;
  WakeEnded(WakeEnded&&) = delete;
  WakeEnded& operator=(WakeEnded&&) = delete;
  ~WakeEnded() {
    LoadWaiter* const woken = store_->TakeWoken();
    lock_->unlock();
    SingleShardStore::Wake(woken, core::Result<void>{});
  }

 private:
  SingleShardStore* store_;
  std::unique_lock<std::shared_mutex>* lock_;
};

// Points the store's graveyard at `graveyard` while it lives, so no
// exit, a throw's included, leaves the store pointing at a freed one.
// Within the shard's lock.
class Burial {
 public:
  Burial(SingleShardStore& store, Graveyard& graveyard) : store_(&store) {
    store.SetGraveyard(&graveyard);
  }
  Burial(const Burial&) = delete;
  Burial& operator=(const Burial&) = delete;
  Burial(Burial&&) = delete;
  Burial& operator=(Burial&&) = delete;
  ~Burial() { store_->SetGraveyard(nullptr); }

 private:
  SingleShardStore* store_;
};

}  // namespace

ShardedHotStore::ShardedHotStore(ShardedHotStoreConfig config) : config_(std::move(config)) {
  const auto stub_budget = static_cast<double>(config_.max_memory_bytes) *
                           config_.stub_memory_fraction / static_cast<double>(kStubBytes);
  SingleShardConfig shard_config{
      .max_memory_bytes = config_.max_memory_bytes / config_.shard_count,
      .stub_max_entries = static_cast<size_t>(stub_budget) / config_.shard_count,
      .negative_max_entries = config_.negative_max_entries / config_.shard_count,
      .backpressure_ratio = config_.backpressure_ratio,
      .steady_clock = config_.steady_clock,
      .wall_clock = config_.wall_clock,
      .reserve_keys =
          config_.reserve_bytes_per_key == 0
              ? 0
              : config_.max_memory_bytes / config_.shard_count / config_.reserve_bytes_per_key,
      .on_rehash = [this](double seconds) { rehash_seconds_.Observe(seconds); },
  };
  shards_.reserve(config_.shard_count);
  for (uint32_t i = 0; i < config_.shard_count; ++i) {
    shards_.push_back(std::make_unique<Shard>(shard_config));
  }
  auto& reg = metrics::Registry::Instance();
  using metrics::MaintenancePass;
  for (const MaintenancePass pass :
       {MaintenancePass::kTombstones, MaintenancePass::kParked, MaintenancePass::kTtl,
        MaintenancePass::kDeadline, MaintenancePass::kMemory}) {
    hold_seconds_.at(static_cast<size_t>(pass)) =
        reg.Histogram(metrics::names::kHotMaintenanceHoldSeconds, pass);
  }
  expiry_sweep_seconds_ = reg.Histogram(metrics::names::kHotExpirySweepSeconds);
  rehash_seconds_ = reg.Histogram(metrics::names::kHotRehashSeconds);
  ABYSS_LOG_INFO("hot store opened", {"shards", static_cast<int64_t>(config_.shard_count)},
                 {"max_memory_bytes", static_cast<uint64_t>(config_.max_memory_bytes)});
}

// NOLINTNEXTLINE(bugprone-exception-escape): as FailLoadWaits.
ShardedHotStore::~ShardedHotStore() { FailLoadWaits(); }

// Only a lock or the error's allocation can throw here, and either at
// teardown may terminate.
// NOLINTNEXTLINE(bugprone-exception-escape)
void ShardedHotStore::FailLoadWaits() {
  const core::Result<void> gone =
      std::unexpected(core::Error{core::ErrorCode::kUnavailable, "hot store shutting down"});
  for (auto& shard : shards_) {
    LoadWaiter* woken = nullptr;
    {
      const std::unique_lock lock(shard->mutex);
      shard->store.FailLoadWaits(gone.error());
      woken = shard->store.TakeWoken();
    }
    SingleShardStore::Wake(woken, gone);
  }
}

core::ShardId ShardedHotStore::ShardIndex(std::string_view key) const {
  return core::ComputeShard(key, config_.shard_count);
}

ShardedHotStore::Shard& ShardedHotStore::ShardFor(std::string_view key) {
  return *shards_[ShardIndex(key)];
}

core::SequenceId ShardedHotStore::Horizon(core::ShardId shard) const {
  return config_.drained ? config_.drained(shard) : kAllDrained;
}

const core::EvictionPolicy& ShardedHotStore::Policy() const {
  return config_.eviction_policy != nullptr ? *config_.eviction_policy : default_policy_;
}

core::EvictionTTL ShardedHotStore::ResolveEviction(std::string_view key) const {
  return Policy().Resolve(key);
}

core::Result<core::RespValue> ShardedHotStore::Exec(const core::ops::ReadOp& op,
                                                    std::optional<core::Duration> /*deadline*/)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return ExecExists(o);
        } else {
          auto& shard = ShardFor(o.key);
          const std::shared_lock lock(shard.mutex);
          return shard.store.Exec(op);
        }
      },
      op);
}

ShardedHotStore::HotRead ShardedHotStore::Read(const core::ops::ReadOp& op)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const auto* exists = std::get_if<core::ops::Exists>(&op);
  const std::string_view key = exists != nullptr ? exists->keys.front() : core::ops::PrimaryKey(op);
  const auto index = ShardIndex(key);
  auto& shard = *shards_[index];
  const std::shared_lock lock(shard.mutex);
  // After locking, so it is no older than any eviction behind a miss.
  auto answer = shard.store.Read(op, Horizon(index));
  return {.result = std::move(answer.result), .fence = answer.fence};
}

// Engine fan-out always issues Exists{single key}. The vector shape and the
// cross-shard grouping survive only to keep Exists itself a stable internal
// probe — a single-element keys vector executes one iteration.
core::Result<core::RespValue> ShardedHotStore::ExecExists(const core::ops::Exists& op) {
  int64_t total = 0;
  for (auto key : op.keys) {
    auto& shard = ShardFor(key);
    const std::shared_lock lock(shard.mutex);
    core::ops::Exists shard_op{.keys = {key}};
    auto result = shard.store.Exec(core::ops::ReadOp{shard_op});
    if (result.has_value()) total += result->AsInteger();
  }
  return core::RespValue::Integer(total);
}

core::Result<core::RespValue> ShardedHotStore::Apply(
    const core::ops::WriteOp& op, core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  return std::visit(
      [this, &op, seq](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o, seq);
        } else {
          const auto key = core::ops::PrimaryKey(core::ops::WriteOp{o});
          return ApplyToShard(ShardIndex(key), op, ResolveEviction(key), seq);
        }
      },
      op);
}

core::Result<core::RespValue> ShardedHotStore::ApplyToShard(
    core::ShardId index, const core::ops::WriteOp& op, core::EvictionTTL eviction,
    core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  // Read before the lock: never a cold call under a shard lock.
  const auto horizon = Horizon(index);
  auto& shard = *shards_[index];
  std::unique_lock lock(shard.mutex);
  const WakeEnded wake(shard.store, lock);
  return shard.store.Apply(op, eviction, seq, horizon);
}

core::HotKeyPresence ShardedHotStore::Probe(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  std::shared_lock lock(shard.mutex);
  return shard.store.Probe(key);
}

// One Del per key, on its own shard: a multi-key Del is never logged.
core::Result<core::RespValue> ShardedHotStore::ApplyDel(const core::ops::Del& op,
                                                        core::SequenceId seq) {
  int64_t total_removed = 0;
  for (auto key : op.keys) {
    core::ops::Del shard_op{.keys = {key}};
    auto result =
        ApplyToShard(ShardIndex(key), core::ops::WriteOp{shard_op}, core::EvictionTTL{0}, seq);
    if (!result.has_value()) return std::unexpected(result.error());
    total_removed += result->AsInteger();
  }
  return core::RespValue::Integer(total_removed);
}

core::Result<void> ShardedHotStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                               core::SequenceId seq) {
  for (const auto& op : ops) {
    auto result = Apply(op, seq);
    if (!result.has_value()) return std::unexpected(result.error());
  }
  return {};
}

core::Result<core::MemoryStats> ShardedHotStore::Stats() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  core::MemoryStats total{};
  for (const auto& shard : shards_) {
    std::shared_lock lock(shard->mutex);
    auto stats = shard->store.Stats();
    total.used_bytes += stats.used_bytes;
    total.key_count += stats.key_count;
    total.eviction_count += stats.eviction_count;
    total.expired_count += stats.expired_count;
    total.max_bytes += stats.max_bytes;
    total.stub_entries += stats.stub_entries;
    total.negative_entries += stats.negative_entries;
    total.stub_bytes += stats.stub_bytes;
    total.stub_drops += stats.stub_drops;
    total.load_discards += stats.load_discards;
    total.unevictable_bytes += stats.unevictable_bytes;
    total.backpressured = total.backpressured || stats.backpressured;
  }
  return total;
}

core::Result<void> ShardedHotStore::Wipe(core::ShardId shard,
                                         core::SequenceId seq) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  if (shard >= shards_.size()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument,
                                       "hot shard " + std::to_string(shard) + " out of range"});
  }
  Shard& target = *shards_[shard];
  FreshMaps fresh = target.store.ReservedMaps();
  Graveyard wiped;
  {
    std::unique_lock lock(target.mutex);
    const WakeEnded wake(target.store, lock);
    const Burial burial(target.store, wiped);
    target.store.Wipe(seq, std::move(fresh));
  }
  return {};
}

std::optional<core::RespValue> ShardedHotStore::ApplyLogged(core::ShardId shard,
                                                            core::QueueEntry& entry) {
  std::optional<core::RespValue> reply;
  if (auto replayed = ReplayFrame(shard, entry, reply); !replayed.has_value()) {
    core::Fatal(replayed.error().message());
  }
  return reply;
}

core::Result<ShardedHotStore::Replayed> ShardedHotStore::Replay(core::ShardId shard,
                                                                core::QueueEntry& entry) {
  std::optional<core::RespValue> reply;
  return ReplayFrame(shard, entry, reply);
}

core::Result<ShardedHotStore::Replayed> ShardedHotStore::ReplayFrame(
    core::ShardId shard, core::QueueEntry& entry,
    std::optional<core::RespValue>& reply) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto* write = std::get_if<core::entry::Write>(&entry.payload);
  // A Write is logged on its key's stream; elsewhere it is corrupt, and
  // replaying it would give the key another stream's seq.
  if (write != nullptr && write->cmd.args.size() > 1) {
    if (const core::ShardId owner = ShardIndex(write->cmd.args[1]); owner != shard) {
      return std::unexpected(core::Error{
          core::ErrorCode::kCorruption,
          "replayed frame at seq " + std::to_string(entry.seq) + " on shard " +
              std::to_string(shard) + " writes a key of shard " + std::to_string(owner) +
              " (key_hash " + std::to_string(log::KeyHash(write->cmd.args[1])) + ")"});
    }
  }
  // Read before the lock: never a cold call under a shard lock.
  const auto horizon = Horizon(shard);
  const core::SteadyTime linked_at(std::chrono::duration_cast<core::SteadyClock::duration>(
      entry.appended_at.time_since_epoch()));
  Shard& target = *shards_.at(shard);
  std::unique_lock lock(target.mutex);
  const WakeEnded wake(target.store, lock);
  target.store.RaiseAppendedAt(entry.appended_at);
  reply.reset();
  if (write != nullptr && !entry.replaces_state &&
      (write->cmd.args.size() < 2 || !target.store.HasEntry(write->cmd.args[1]))) {
    // Its key's earlier history was evicted or reclaimed: buffer and
    // cold hold the key whole, and a stub no longer describes it.
    if (write->cmd.args.size() > 1) target.store.DropStub(write->cmd.args[1]);
  } else if (write != nullptr) {
    core::Effect effect{
        .key = write->cmd.args.size() > 1 ? write->cmd.args[1] : std::string{},
        .cmd = std::move(write->cmd),
        .replaces_state = entry.replaces_state,
    };
    auto replies = target.store.ApplyEffects(std::span(&effect, 1), entry.seq, entry.appended_at,
                                             Policy(), horizon, linked_at);
    reply = std::move(replies.front());
  } else if (std::holds_alternative<core::entry::Flush>(entry.payload)) {
    target.store.Wipe(entry.seq);
    reply = core::RespValue::SimpleString("OK");
  } else {
    core::Fatal("only Write and Flush entries apply to hot as logged");
  }
  return Replayed{
      .applied = reply.has_value(),
      .over_backpressure = target.store.OverBackpressure(),
      .memory = {.used_bytes = target.store.UsedBytes(),
                 .limit_bytes = target.store.BackpressureLimit()},
  };
}

ShardedHotStore::ShardMemory ShardedHotStore::Memory(core::ShardId shard) const
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const Shard& target = *shards_.at(shard);
  const std::shared_lock lock(target.mutex);
  return {.used_bytes = target.store.UsedBytes(), .limit_bytes = target.store.BackpressureLimit()};
}

void ShardedHotStore::ShiftReplayedLinks(core::SteadyTime steady_now,
                                         core::WallTime wall_now) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  // Replay linked at wall ticks; one shift maps them all to steady.
  const auto shift =
      steady_now.time_since_epoch() -
      std::chrono::duration_cast<core::SteadyClock::duration>(wall_now.time_since_epoch());
  for (auto& shard : shards_) {
    const std::unique_lock lock(shard->mutex);
    shard->store.ShiftLinks(shift, steady_now);
  }
}

void ShardedHotStore::RaiseAppendedAt(core::ShardId shard,
                                      core::WallTime at) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  Shard& target = *shards_.at(shard);
  const std::unique_lock lock(target.mutex);
  target.store.RaiseAppendedAt(at);
}

ShardLocks ShardedHotStore::LockExclusive(std::span<const core::ShardId> shards) {
  return {*this, shards};
}

bool ShardedHotStore::EvictShardToTarget(core::ShardId shard) {
  bool over = false;
  size_t evicted = 0;
  RunHolds(shard, metrics::MaintenancePass::kMemory,
           [this, &over, &evicted](SingleShardStore& store, core::SequenceId horizon,
                                   HoldBudget& budget) {
             const bool done = config_.max_memory_bytes == 0 ||
                               store.EvictLru(config_.max_memory_bytes / config_.shard_count,
                                              horizon, budget, evicted);
             over = store.OverBackpressure();
             return done;
           });
  return !over;
}

LoadStart ShardedHotStore::BeginLoad(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const auto index = ShardIndex(key);
  auto& shard = *shards_[index];
  const std::unique_lock lock(shard.mutex);
  // An atomic read. Taken under the lock, it is no older than any
  // eviction behind the key's absence, as in ShardLocks.
  return shard.store.StartLoad(key, Horizon(index));
}

bool ShardedHotStore::CompleteLoad(std::string_view key, LoadToken token, LoadResult&& result) {
  return Install(key, token, std::move(result), /*fill=*/false) == FillResult::kInstalled;
}

ShardedHotStore::FillResult ShardedHotStore::Fill(std::string_view key, LoadToken token,
                                                  LoadResult&& result) {
  return Install(key, token, std::move(result), /*fill=*/true);
}

ShardedHotStore::FillResult ShardedHotStore::Install(std::string_view key, LoadToken token,
                                                     LoadResult&& result,
                                                     bool fill) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const auto index = ShardIndex(key);
  const auto eviction = ResolveEviction(key);
  auto& shard = *shards_[index];
  const auto* full = std::get_if<LoadedFull>(&result);
  const double budget =
      static_cast<double>(config_.max_memory_bytes) / static_cast<double>(config_.shard_count);
  const bool too_large = fill && full != nullptr && budget > 0 &&
                         static_cast<double>(full->bytes) > budget * config_.fill_max_fraction;
  Graveyard evicted;
  FillResult outcome = FillResult::kDiscarded;
  {
    std::unique_lock lock(shard.mutex);
    const WakeEnded wake(shard.store, lock);
    const auto horizon = Horizon(index);
    if (too_large || (fill && shard.store.OverBackpressure())) {
      shard.store.AbortLoad(key, token);
      outcome = too_large ? FillResult::kTooLarge : FillResult::kOverBackpressure;
    } else {
      const Burial burial(shard.store, evicted);
      // A fill is optional: it evicts one hold's worth at most, and is
      // skipped if that is not enough.
      if (fill && full != nullptr && shard.store.LoadPending(key) &&
          !shard.store.MakeRoom(key, full->bytes, horizon)) {
        shard.store.AbortLoad(key, token);
        outcome = FillResult::kNoRoom;
      } else if (shard.store.CompleteLoad(key, token, std::move(result), eviction, horizon)) {
        outcome = FillResult::kInstalled;
      }
    }
  }
  return outcome;
}

size_t ShardedHotStore::CompleteLoads(core::ShardId shard, std::span<LoadCompletion> loads)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  for (const auto& load : loads) {
    if (ShardIndex(load.key) != shard) core::Fatal("a load completes on another key's shard");
  }
  auto& target = *shards_.at(shard);
  std::unique_lock lock(target.mutex);
  const WakeEnded wake(target.store, lock);
  return target.store.CompleteLoads(loads, Policy());
}

void ShardedHotStore::AbortLoad(std::string_view key,
                                LoadToken token) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  std::unique_lock lock(shard.mutex);
  const WakeEnded wake(shard.store, lock);
  shard.store.AbortLoad(key, token);
}

std::optional<core::Result<void>> ShardedHotStore::WhenLoaded(
    std::string_view key, LoadWaiter& waiter) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::unique_lock lock(shard.mutex);
  return shard.store.AddLoadWaiter(key, waiter);
}

bool ShardedHotStore::CancelLoadWait(std::string_view key,
                                     LoadWaiter& waiter) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::unique_lock lock(shard.mutex);
  return shard.store.CancelLoadWaiter(key, waiter);
}

bool ShardedHotStore::AwaitLoad(std::string_view key, core::SteadyTime deadline) {
  if (load_wait_hook_) load_wait_hook_();
  BlockingLoadWaiter waiter;
  if (const auto now = WhenLoaded(key, waiter); now.has_value()) return now->has_value();
  // A lost Cancel means the placeholder ended and its wake is coming,
  // which the waiter must outlive.
  if (!waiter.WaitUntil(deadline) && CancelLoadWait(key, waiter)) return false;
  return waiter.Take().has_value();
}

size_t ShardedHotStore::LoadWaitersForTesting(std::string_view key)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::shared_lock lock(shard.mutex);
  return shard.store.LoadWaitersForTesting(key);
}

bool ShardedHotStore::LoadPending(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::shared_lock lock(shard.mutex);
  return shard.store.LoadPending(key);
}

std::optional<Stub> ShardedHotStore::FindStub(std::string_view key)
    ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::shared_lock lock(shard.mutex);
  const Stub* stub = shard.store.FindStub(key);
  if (stub == nullptr) return std::nullopt;
  return *stub;
}

bool ShardedHotStore::DropStub(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  auto& shard = ShardFor(key);
  const std::unique_lock lock(shard.mutex);
  return shard.store.DropStub(key);
}

bool ShardedHotStore::KnownAbsentAfterFlush(std::string_view key) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  const auto index = ShardIndex(key);
  const auto horizon = Horizon(index);
  auto& shard = *shards_[index];
  const std::shared_lock lock(shard.mutex);
  return shard.store.KnownAbsentAfterFlush(horizon);
}

void ShardedHotStore::SetAccessTime(core::SteadyTime now) {
  for (auto& shard : shards_) shard->store.SetAccessTime(now);
}

template <typename Step, typename More>
void ShardedHotStore::RunHolds(core::ShardId index, metrics::MaintenancePass pass, Step step,
                               More more) ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  Shard& shard = *shards_.at(index);
  for (;;) {
    // Read before the lock: never a cold call under a shard lock, and
    // an older horizon only holds back more.
    const auto horizon = Horizon(index);
    HoldBudget budget = HoldBudget::Capped();
    // Freed after the lock.
    Graveyard removed;
    bool done = false;
    core::SteadyClock::duration held{};
    {
      const std::unique_lock lock(shard.mutex);
      const auto start = core::SteadyClock::now();
      const Burial burial(shard.store, removed);
      done = step(shard.store, horizon, budget);
      held = core::SteadyClock::now() - start;
    }
    hold_seconds_.at(static_cast<size_t>(pass))
        .Observe(std::chrono::duration<double>(held).count());
    if (hold_observer_) hold_observer_(pass, held, budget.examined());
    if (done || !more(budget)) return;
  }
}

template <typename Step>
void ShardedHotStore::RunHolds(core::ShardId index, metrics::MaintenancePass pass, Step step) {
  RunHolds(index, pass, std::move(step), [](const HoldBudget& /*budget*/) { return true; });
}

SingleShardStore::EvictExpiredReport ShardedHotStore::EvictExpired(
    core::SteadyTime now, std::optional<core::Duration> ttl_budget) {
  EvictExpiredReport total;
  for (core::ShardId index = 0; index < config_.shard_count; ++index) {
    RunHolds(index, metrics::MaintenancePass::kParked,
             [now, &total](SingleShardStore& store, core::SequenceId horizon, HoldBudget& budget) {
               return store.ReleaseParked(now, horizon, budget, total);
             });
  }
  ExpireByTtl(now, ttl_budget, total);
  for (core::ShardId index = 0; index < config_.shard_count; ++index) {
    RunHolds(index, metrics::MaintenancePass::kDeadline,
             [now, &total](SingleShardStore& store, core::SequenceId horizon, HoldBudget& budget) {
               return store.EvictPastDeadline(now, horizon, budget, total);
             });
  }
  return total;
}

void ShardedHotStore::ExpireByTtl(core::SteadyTime now, std::optional<core::Duration> budget,
                                  EvictExpiredReport& total) {
  const std::scoped_lock sweep(ttl_mu_);
  const auto started = core::SteadyClock::now();
  if (!sweep_started_.has_value()) sweep_started_ = started;
  const auto out_of_time = [budget, started] {
    return budget.has_value() && core::SteadyClock::now() - started >= *budget;
  };
  while (ttl_swept_ < config_.shard_count) {
    if (out_of_time()) return;
    size_t expired = 0;
    bool stopped_for_time = false;
    RunHolds(
        ttl_next_shard_, metrics::MaintenancePass::kTtl,
        [now, &total, &expired](SingleShardStore& store, core::SequenceId horizon,
                                HoldBudget& hold) {
          const size_t before = total.Total() + total.parked;
          const bool done = store.ExpireTtl(now, horizon, hold, total);
          expired = total.Total() + total.parked - before;
          return done;
        },
        [&expired, &stopped_for_time, &out_of_time](const HoldBudget& hold) {
          // Valkey's rule: another hold only while more than a quarter
          // of what one examined had expired.
          if (expired * 4 <= hold.examined()) return false;
          stopped_for_time = out_of_time();
          return !stopped_for_time;
        });
    // Resumed next time.
    if (stopped_for_time) return;
    ++ttl_swept_;
    ttl_next_shard_ = (ttl_next_shard_ + 1) % config_.shard_count;
  }
  expiry_sweep_seconds_.Observe(
      std::chrono::duration<double>(core::SteadyClock::now() - sweep_started_.value_or(started))
          .count());
  sweep_started_.reset();
  ttl_swept_ = 0;
}

size_t ShardedHotStore::EvictToMemoryTarget() {
  // A budget of 0 means unlimited — never evict for memory pressure. (Guarded
  // here too because EvictLru(0) would otherwise evict every key.)
  if (config_.max_memory_bytes == 0) return 0;
  size_t evicted = 0;
  // Each shard owns max_memory_bytes / shard_count of the budget (set at
  // construction). Evict LRU down to that per-shard ceiling; data is safe in
  // queue/cold (invariant 2).
  const size_t per_shard = config_.max_memory_bytes / config_.shard_count;
  for (core::ShardId index = 0; index < config_.shard_count; ++index) {
    RunHolds(index, metrics::MaintenancePass::kMemory,
             [per_shard, &evicted](SingleShardStore& store, core::SequenceId horizon,
                                   HoldBudget& budget) {
               return store.EvictLru(per_shard, horizon, budget, evicted);
             });
  }
  return evicted;
}

size_t ShardedHotStore::GcTombstones() {
  size_t reclaimed = 0;
  for (core::ShardId index = 0; index < config_.shard_count; ++index) {
    reclaimed += GcTombstones(index);
  }
  return reclaimed;
}

size_t ShardedHotStore::GcTombstones(core::ShardId shard) {
  size_t reclaimed = 0;
  RunHolds(shard, metrics::MaintenancePass::kTombstones,
           [&reclaimed](SingleShardStore& store, core::SequenceId horizon, HoldBudget& budget) {
             return store.GcTombstones(horizon, budget, reclaimed);
           });
  return reclaimed;
}

ShardLocks::ShardLocks(ShardedHotStore& hot, std::span<const core::ShardId> shards)
    : hot_(&hot), shards_(shards.begin(), shards.end()) {
  // A reservation's owner may not block until it completes, and a
  // lock holder may be waiting on its fill.
  ABYSS_DCHECK(queue::ReservationsHeld() == 0,
               "hot shards locked by a thread holding a reservation");
  ABYSS_DCHECK(std::ranges::adjacent_find(shards_, std::greater_equal<>{}) == shards_.end(),
               "hot shards must be locked sorted and distinct");
  horizons_.reserve(shards_.size());
  Lock();
  // No destructor runs for a constructor that throws, as the drained
  // supplier or a test's observer may: what Lock took is released here.
  try {
    if (hot_->write_hold_observer_) hot_->write_hold_observer_(true);
    for (const core::ShardId shard : shards_) {
      // An atomic read, not a cold call. Taken under the locks, it is no
      // older than any eviction a view's miss reflects.
      horizons_.push_back(hot_->Horizon(shard));
      hot_->shards_[shard]->store.SetGraveyard(&graveyard_);
    }
  } catch (...) {
    for (const core::ShardId shard : shards_) hot_->shards_[shard]->store.SetGraveyard(nullptr);
    for (const core::ShardId shard : std::views::reverse(shards_)) {
      hot_->shards_[shard]->mutex.unlock();
    }
    held_ = false;
    throw;
  }
}

ShardLocks::ShardLocks(ShardLocks&& other) noexcept
    : hot_(other.hot_),
      shards_(std::move(other.shards_)),
      horizons_(std::move(other.horizons_)),
      graveyard_(std::move(other.graveyard_)),
      held_(std::exchange(other.held_, false)) {
  if (!held_) return;
  for (const core::ShardId shard : shards_) {
    hot_->shards_[shard]->store.SetGraveyard(&graveyard_);
  }
}

// NOLINTNEXTLINE(bugprone-exception-escape): only a test's hold observer can throw.
ShardLocks::~ShardLocks() { Unlock(); }

// A lock that throws leaves the ones before it released, newest first.
void ShardLocks::Lock() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  std::size_t taken = 0;
  try {
    for (; taken < shards_.size(); ++taken) hot_->shards_.at(shards_[taken])->mutex.lock();
  } catch (...) {
    while (taken > 0) hot_->shards_[shards_[--taken]]->mutex.unlock();
    throw;
  }
  held_ = true;
}

Graveyard ShardLocks::Unlock() ABYSS_NO_THREAD_SAFETY_ANALYSIS {
  if (!held_) return {};
  if (hot_->write_hold_observer_) hot_->write_hold_observer_(false);
  // One chain of every held shard's ended waits, woken once unlocked.
  LoadWaiter* woken = nullptr;
  for (const core::ShardId shard : shards_) {
    SingleShardStore& store = hot_->shards_[shard]->store;
    store.SetGraveyard(nullptr);
    woken = SingleShardStore::Chain(store.TakeWoken(), woken);
  }
  for (const core::ShardId shard : std::views::reverse(shards_)) {
    hot_->shards_[shard]->mutex.unlock();
  }
  held_ = false;
  SingleShardStore::Wake(woken, core::Result<void>{});
  return std::exchange(graveyard_, Graveyard{});
}

size_t ShardLocks::Slot(core::ShardId shard) const {
  const auto it = std::ranges::lower_bound(shards_, shard);
  if (!held_ || it == shards_.end() || *it != shard) {
    core::Fatal("hot shard " + std::to_string(shard) + " is not held");
  }
  return static_cast<size_t>(it - shards_.begin());
}

SingleShardStore& ShardLocks::Store(core::ShardId shard) const {
  return hot_->shards_[shards_[Slot(shard)]]->store;
}

KeyView ShardLocks::View(std::string_view key, uint64_t now_ms) const {
  const core::ShardId shard = hot_->ShardIndex(key);
  KeyView view = Store(shard).View(key, horizons_[Slot(shard)], now_ms);
  view.shard = shard;
  return view;
}

LoadStart ShardLocks::BeginLoad(std::string_view key) {
  const core::ShardId shard = hot_->ShardIndex(key);
  return Store(shard).StartLoad(key, horizons_[Slot(shard)]);
}

// Registered in the hold that saw BeginLoad's kPending, and every end
// of a placeholder holds the shard exclusively: no wake can fall
// between that sight and the link.
std::optional<core::Result<void>> ShardLocks::WhenLoaded(std::string_view key, LoadWaiter& waiter) {
  return Store(hot_->ShardIndex(key)).AddLoadWaiter(key, waiter);
}

size_t ShardLocks::CompleteLoads(core::ShardId shard, std::span<LoadCompletion> loads) {
  for (const auto& load : loads) {
    if (hot_->ShardIndex(load.key) != shard) core::Fatal("a load completes on another key's shard");
  }
  return Store(shard).CompleteLoads(loads, hot_->Policy());
}

void ShardLocks::AbortLoad(std::string_view key, LoadToken token) {
  Store(hot_->ShardIndex(key)).AbortLoad(key, token);
}

std::vector<core::RespValue> ShardLocks::ApplyEffects(core::ShardId shard,
                                                      std::span<core::Effect> effects,
                                                      core::SequenceId first_seq,
                                                      core::WallTime appended_at) {
  return Store(shard).ApplyEffects(effects, first_seq, appended_at, hot_->Policy(),
                                   horizons_[Slot(shard)]);
}

void ShardLocks::Wipe(core::ShardId shard, core::SequenceId seq, FreshMaps fresh) {
  Store(shard).Wipe(seq, std::move(fresh));
}

bool ShardLocks::OverBackpressure(core::ShardId shard) const {
  return Store(shard).OverBackpressure();
}

core::WallTime ShardLocks::LastAppendedAt(core::ShardId shard) const {
  return Store(shard).LastAppendedAt();
}

void ShardLocks::RaiseAppendedAt(core::ShardId shard, core::WallTime at) {
  Store(shard).RaiseAppendedAt(at);
}

}  // namespace abyss::hot
