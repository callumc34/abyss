#pragma once

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
#include <unordered_set>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::hot {

struct ShardedHotStoreConfig {
  size_t max_memory_bytes = 4294967296;
  uint32_t shard_count = 64;
  // Per-shard cap on the deferred read-access refresh buffer (XRES-2). Past the
  // cap, refreshes are dropped (a dropped refresh only shortens a key's
  // deadline — safe, the key is still in queue/cold). 0 means unbounded.
  size_t access_buffer_high_water = 65536;
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
  enum class FillResult : uint8_t { kInstalled, kDiscarded, kOverBackpressure, kTooLarge };
  // A read's cache fill: CompleteLoad, unless the shard is over its
  // backpressure limit or `result` is over fill_max_fraction of its
  // budget, when the load is aborted. Only kInstalled moves `result`.
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

  // Refreshes the deadline for every buffered access, using the per-key
  // eviction cached on each Entry at Apply time. See ADP-002 §Eviction.
  void DrainAccessBuffers(core::SteadyTime now);
  using EvictExpiredReport = SingleShardStore::EvictExpiredReport;
  EvictExpiredReport EvictExpired(core::SteadyTime now);

  // Evicts each shard down to its per-shard memory budget by LRU. Eviction is a
  // tier transition (data stays durable in queue/cold). Returns the number of
  // keys evicted across all shards (HOT-1 memory-pressure pass).
  size_t EvictToMemoryTarget();

  // Current total depth of the per-shard access buffers and the count of
  // refreshes dropped past the high-water cap since construction (XRES-2).
  struct AccessBufferStats {
    size_t depth = 0;
    uint64_t dropped = 0;
  };
  AccessBufferStats AccessBufferSnapshot() const;

  // Reclaims each shard's tombstones cold has drained.
  size_t GcTombstones();

  uint32_t shard_count() const { return config_.shard_count; }

 private:
  friend class ShardLocks;

  struct Shard {
    mutable std::shared_mutex mutex;
    SingleShardStore store;
    mutable std::mutex access_mutex;
    // Deferred read-access refresh queue, bounded at access_buffer_high_water.
    // access_seen de-dups within a drain interval so a hot key is buffered
    // once per tick rather than once per read (XRES-2).
    std::vector<std::string> access_buffer ABYSS_GUARDED_BY(access_mutex);
    std::unordered_set<std::string> access_seen ABYSS_GUARDED_BY(access_mutex);
    uint64_t access_dropped ABYSS_GUARDED_BY(access_mutex) = 0;
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
  // Queues `key` for a deferred LRU refresh after a read hit.
  void RecordAccess(Shard& shard, std::string_view key) const;

  ShardedHotStoreConfig config_;
  // Fallback when config_.eviction_policy is null; keeps Resolve() infallible.
  core::EvictionPolicy default_policy_;
  std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace abyss::hot
