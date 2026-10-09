#pragma once

#include <array>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::hot {

struct ShardedHotStoreConfig {
  size_t max_memory_bytes = 4294967296;
  uint32_t shard_count = 64;
  // Share of max_memory_bytes for stubs, at kStubBytes each.
  double stub_memory_fraction = 0.02;
  // Keys held as loaded absent, across shards.
  size_t negative_max_entries = 65536;
  // A cache fill larger than this share of a shard's budget is never
  // installed, however often it is read.
  double fill_max_fraction = 0.0625;
  // Over max_memory_bytes times this, the store reports backpressure.
  double backpressure_ratio = 1.25;
  // A shard's cold drained seq: nothing above it is removed from hot.
  // Null means everything has drained.
  std::function<core::SequenceId(core::ShardId)> drained;
  // Borrowed from the server's single EvictionPolicy. Must outlive the store.
  // Nullable for tests that don't exercise eviction (DefaultPolicy is used).
  const core::EvictionPolicy* eviction_policy = nullptr;
  core::SteadyClockFn steady_clock = core::DefaultSteadyClock;
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

class ShardedHotStore;

// An exclusive hold on a set of hot shards, and everything the
// sequencer may do under it. Ending the hold (Unlock or destruction)
// releases the locks, then wakes load waiters; what its applies
// replaced is freed after the locks too, by Unlock's caller.
class ShardLocks {
 public:
  ShardLocks(ShardLocks&& other) noexcept;
  ShardLocks& operator=(ShardLocks&&) = delete;
  ShardLocks(const ShardLocks&) = delete;
  ShardLocks& operator=(const ShardLocks&) = delete;
  ~ShardLocks();

  // `key`'s shard must be held. Expiry is judged at `now_ms`.
  KeyView View(std::string_view key, uint64_t now_ms) const;
  LoadStart BeginLoad(std::string_view key);
  // SingleShardStore::CompleteLoads: no eviction, no stub drop.
  size_t CompleteLoads(core::ShardId shard, std::span<LoadCompletion> loads);
  void AbortLoad(std::string_view key, LoadToken token);
  std::vector<core::RespValue> ApplyEffects(core::ShardId shard, std::span<core::Effect> effects,
                                            core::SequenceId first_seq, core::WallTime appended_at);
  void Wipe(core::ShardId shard, core::SequenceId seq);
  bool OverBackpressure(core::ShardId shard) const;
  core::WallTime LastAppendedAt(core::ShardId shard) const;
  void RaiseAppendedAt(core::ShardId shard, core::WallTime at);

  // Ends the hold, handing back what its applies replaced so the caller
  // can free it once it has published; dropped, it is freed at once.
  Graveyard Unlock();
  bool held() const { return held_; }

 private:
  friend class ShardedHotStore;
  ShardLocks(ShardedHotStore& hot, std::span<const core::ShardId> shards);
  void Lock();

  // The held shard's position in shards_.
  size_t Slot(core::ShardId shard) const;
  SingleShardStore& Store(core::ShardId shard) const;
  // Marks `shard` for a load-cv wake if its loads changed.
  void NoteLoads(core::ShardId shard, size_t before);

  ShardedHotStore* hot_;
  std::vector<core::ShardId> shards_;
  // Each shard's cold drained seq, read once the locks were held.
  std::vector<core::SequenceId> horizons_;
  std::vector<bool> wake_;
  Graveyard graveyard_;
  bool held_ = false;
};

class ShardedHotStore : public core::HotStore {
 public:
  explicit ShardedHotStore(ShardedHotStoreConfig config);
  ~ShardedHotStore() override;
  ShardedHotStore(const ShardedHotStore&) = delete;
  ShardedHotStore& operator=(const ShardedHotStore&) = delete;
  ShardedHotStore(ShardedHotStore&&) = delete;
  ShardedHotStore& operator=(ShardedHotStore&&) = delete;

  core::Result<core::RespValue> Exec(
      const core::ops::ReadOp& op, std::optional<core::Duration> deadline = std::nullopt) override;
  core::Result<core::RespValue> Apply(const core::ops::WriteOp& op, core::SequenceId seq) override;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId seq) override;
  core::HotKeyPresence Probe(std::string_view key) override;
  void SetReplayMode(bool replaying) override;
  core::Result<core::MemoryStats> Stats() override;
  core::Result<void> Wipe(core::ShardId shard, core::SequenceId seq) override;
  std::optional<core::RespValue> ApplyLogged(core::ShardId shard, core::QueueEntry& entry) override;
  void RaiseAppendedAt(core::ShardId shard, core::WallTime at) override;

  // Takes each of `shards`, sorted and distinct, exclusively and in
  // ascending order. The thread must hold no queue reservation.
  ShardLocks LockExclusive(std::span<const core::ShardId> shards);

  struct HotRead {
    core::Result<core::RespValue> result;
    // What the reply must be durable through; nullopt on a miss. Loaded
    // state's 0 is durable already.
    std::optional<core::SequenceId> fence;
  };
  // SingleShardStore::Read under one shared hold: the result is a copy,
  // so the caller can wait on the fence after the lock is released.
  HotRead Read(const core::ops::ReadOp& op);

  // Evicts `shard`'s drained keys down to its budget; true if it is
  // then no longer over its backpressure limit.
  bool EvictShardToTarget(core::ShardId shard);
  core::SequenceId Drained(core::ShardId shard) const { return Horizon(shard); }
  core::ShardId ShardOf(std::string_view key) const { return ShardIndex(key); }

  // Refused, as well, while the key's shard is under its flush floor:
  // cold may still hold what the Flush removed.
  LoadStart BeginLoad(std::string_view key);
  // Installs `result` unless a write overtook it, evicting to make
  // room; what it evicts is freed after the lock.
  bool CompleteLoad(std::string_view key, LoadToken token, LoadResult&& result);
  enum class FillResult : uint8_t {
    kInstalled,
    kDiscarded,
    kOverBackpressure,
    kTooLarge,
    // One hold's eviction did not make room.
    kNoRoom,
  };
  // A read's cache fill: CompleteLoad, unless the shard is over its
  // backpressure limit, `result` is over fill_max_fraction of its
  // budget, or evicting one hold's worth leaves no room for it, when
  // the load is aborted. Only kInstalled moves `result`.
  FillResult Fill(std::string_view key, LoadToken token, LoadResult&& result);
  // SingleShardStore::CompleteLoads on `shard`, in one exclusive hold.
  size_t CompleteLoads(core::ShardId shard, std::span<LoadCompletion> loads);
  void AbortLoad(std::string_view key, LoadToken token);
  // Waits until `key` has no load in flight; false at the deadline.
  bool AwaitLoad(std::string_view key, core::SteadyTime deadline);
  bool LoadPending(std::string_view key);
  bool RetainsStubs() const { return shards_.front()->store.RetainsStubs(); }

  std::optional<Stub> FindStub(std::string_view key);
  bool DropStub(std::string_view key);

  // True while cold has not drained the key's shard's last Flush. Ask
  // only after seeing the miss, so the drain read is no older than it.
  bool KnownAbsentAfterFlush(std::string_view key);

  // What read hits stamp until the next call. See ADP-002 §Eviction.
  void SetAccessTime(core::SteadyTime now);

  // Every maintenance pass below runs in exclusive holds of at most
  // HoldBudget's cap, resuming where the last hold stopped.
  //
  // Parked keys cold has drained, keys past their TTL, then keys idle
  // past their eviction. The TTL pass goes round the shards from where
  // it last stopped, taking another hold while more than a quarter of
  // a hold's keys had expired, for at most `ttl_budget` of real time.
  using EvictExpiredReport = SingleShardStore::EvictExpiredReport;
  EvictExpiredReport EvictExpired(core::SteadyTime now,
                                  std::optional<core::Duration> ttl_budget = std::nullopt);

  // Evicts each shard down to its per-shard memory budget by LRU. Eviction is a
  // tier transition (data stays durable in queue/cold). Returns the number of
  // keys evicted across all shards (HOT-1 memory-pressure pass).
  size_t EvictToMemoryTarget();

  // Reclaims each shard's tombstones cold has drained.
  size_t GcTombstones();

  // Called after each maintenance hold, with what it examined.
  using HoldObserver = std::function<void(metrics::MaintenancePass,
                                          core::SteadyClock::duration held, size_t examined)>;
  void SetHoldObserverForTesting(HoldObserver observer) { hold_observer_ = std::move(observer); }
  // A shared hold on `shard`, as a read takes.
  std::shared_lock<std::shared_mutex> LockSharedForTesting(core::ShardId shard) {
    return std::shared_lock(shards_.at(shard)->mutex);
  }

  uint32_t shard_count() const { return config_.shard_count; }

 private:
  friend class ShardLocks;

  struct Shard {
    mutable std::shared_mutex mutex;
    SingleShardStore store;
    // Signalled when a load placeholder may have gone.
    std::condition_variable_any load_cv;

    explicit Shard(SingleShardConfig config) : store(std::move(config)) {}
  };

  core::ShardId ShardIndex(std::string_view key) const;
  Shard& ShardFor(std::string_view key);
  core::SequenceId Horizon(core::ShardId shard) const;
  const core::EvictionPolicy& Policy() const;
  core::EvictionTTL ResolveEviction(std::string_view key) const;
  core::Result<core::RespValue> ApplyToShard(core::ShardId index, const core::ops::WriteOp& op,
                                             core::EvictionTTL eviction, core::SequenceId seq);

  // CompleteLoad, and as a fill, refused past the limits Fill names.
  FillResult Install(std::string_view key, LoadToken token, LoadResult&& result, bool fill);
  core::Result<core::RespValue> ExecExists(const core::ops::Exists& op);
  core::Result<core::RespValue> ApplyDel(const core::ops::Del& op, core::SequenceId seq);
  // Runs `step(store, horizon, budget)` on shard `index` in capped
  // exclusive holds until it is done or `more(budget)` declines another.
  template <typename Step, typename More>
  void RunHolds(core::ShardId index, metrics::MaintenancePass pass, Step step, More more);
  template <typename Step>
  void RunHolds(core::ShardId index, metrics::MaintenancePass pass, Step step);
  void ExpireByTtl(core::SteadyTime now, std::optional<core::Duration> budget,
                   EvictExpiredReport& total);

  ShardedHotStoreConfig config_;
  // Fallback when config_.eviction_policy is null; keeps Resolve() infallible.
  core::EvictionPolicy default_policy_;
  std::vector<std::unique_ptr<Shard>> shards_;
  // By metrics::MaintenancePass.
  std::array<metrics::HistogramHandle, 5> hold_seconds_;
  metrics::HistogramHandle expiry_sweep_seconds_;
  HoldObserver hold_observer_;
  // The TTL pass's place in its sweep of the shards.
  std::mutex ttl_mu_;
  core::ShardId ttl_next_shard_ ABYSS_GUARDED_BY(ttl_mu_) = 0;
  uint32_t ttl_swept_ ABYSS_GUARDED_BY(ttl_mu_) = 0;
  std::optional<core::SteadyTime> sweep_started_ ABYSS_GUARDED_BY(ttl_mu_);
};

}  // namespace abyss::hot
