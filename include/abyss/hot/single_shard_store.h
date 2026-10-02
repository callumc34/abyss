#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "abyss/core/hot_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/memory_governor.h"

namespace abyss::hot {

// A drain horizon past every seq: nothing is held back for cold.
inline constexpr core::SequenceId kAllDrained = std::numeric_limits<core::SequenceId>::max();

struct SingleShardConfig {
  size_t max_memory_bytes = 0;
  // 0 disables stubs.
  size_t stub_max_entries = 0;
  double backpressure_ratio = 1.25;
  core::SteadyClockFn steady_clock = core::DefaultSteadyClock;
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

struct SetValue {
  std::unordered_set<std::string> members;
};

struct HashValue {
  std::unordered_map<std::string, std::string> fields;
};

struct ZsetValue {
  std::unordered_map<std::string, double> member_scores;
  std::map<double, std::set<std::string>> score_members;
};

using Value = std::variant<std::string, SetValue, HashValue, ZsetValue>;

struct Entry {
  enum class Type : uint8_t { kString, kSet, kHash, kZset };

  Type type;
  Value value;
  core::SteadyTime eviction_deadline;
  // Resolved per-prefix eviction for this key.
  core::EvictionTTL eviction{};
  int64_t abs_ttl_ms = 0;

  // Marks a deleted key whose delete cold may not have absorbed yet.
  bool tombstoned = false;
  // Seq of the last write applied to the key; loaded state carries 0.
  core::SequenceId latest_seq = 0;
  // ApproximateBytes(), kept current as the value changes.
  size_t bytes = 0;
  // Live entries form the shard's LRU list; tombstones are not on it.
  Entry* lru_newer = nullptr;
  Entry* lru_older = nullptr;
  const std::string* lru_key = nullptr;

  size_t ApproximateBytes() const;
};

// What an evicted live key leaves behind.
struct Stub {
  Entry::Type type = Entry::Type::kString;
  int64_t abs_ttl_ms = 0;
  core::SequenceId latest_seq = 0;
};

// Accounted size of one stub beyond its key; also sizes the stub cap.
inline constexpr size_t kStubBytes = 80;

// Bounded stub cache that drops its oldest stub.
class StubCache {
 public:
  explicit StubCache(size_t max_entries) : max_entries_(max_entries) {}
  StubCache(const StubCache&) = delete;
  StubCache& operator=(const StubCache&) = delete;
  StubCache(StubCache&&) = default;
  StubCache& operator=(StubCache&&) = default;
  ~StubCache() = default;

  const Stub* Find(std::string_view key) const;
  void Put(std::string_view key, const Stub& stub);
  bool Erase(std::string_view key);
  void Clear();

  size_t size() const { return index_.size(); }
  uint64_t bytes() const { return bytes_; }
  uint64_t drops() const { return drops_; }

 private:
  struct Node {
    std::string key;
    Stub stub;
  };
  using Order = std::list<Node>;

  void EraseNode(Order::iterator node);

  size_t max_entries_;
  // Most recent first. Index keys view the nodes' own strings.
  Order order_;
  std::unordered_map<std::string_view, Order::iterator> index_;
  uint64_t bytes_ = 0;
  uint64_t drops_ = 0;
};

struct LoadToken {
  uint64_t id = 0;
  bool operator==(const LoadToken&) const = default;
};

// A non-resident key's state as read from buffer and cold.
struct LoadedState {
  bool exists = false;
  Entry::Type type = Entry::Type::kString;
  Value value;
  int64_t abs_ttl_ms = 0;
};

class SingleShardStore {
 public:
  explicit SingleShardStore(SingleShardConfig config);

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const;

  // `seq` is the op's queue seq, stamped as the key's latest_seq. Only
  // keys cold has drained (latest_seq <= `horizon`) are evicted to make
  // room.
  core::Result<core::RespValue> Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction,
                                      core::SequenceId seq = 0,
                                      core::SequenceId horizon = kAllDrained);
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops, core::EvictionTTL eviction,
                                core::SequenceId seq = 0, core::SequenceId horizon = kAllDrained);

  // Existence verdict distinguishing a delete-tombstone (authoritatively
  // absent) from a true miss (consult the next tier). See core::HotKeyPresence.
  core::HotKeyPresence Probe(std::string_view key) const;

  // Reclaims tombstones whose delete seq is <= `horizon` — cold has absorbed
  // those deletes, so the buffer/cold view now reflects them. Live keys remain.
  size_t GcTombstones(core::SequenceId horizon);

  // Extends the eviction deadline using the per-key cached eviction recorded
  // at Apply time, and makes the key most recently used. No-op if the key
  // is absent.
  void RefreshAccess(std::string_view key, core::SteadyTime now);
  // by_deadline is a tier transition (data still in queue/cold) counted as an
  // eviction; by_ttl is a true deletion counted separately (HOT-7).
  struct EvictExpiredReport {
    size_t by_deadline = 0;
    size_t by_ttl = 0;
    size_t Total() const { return by_deadline + by_ttl; }
  };
  // Every eviction below skips a key whose latest_seq is above
  // `horizon`, the shard's cold drained seq, TTL-expired keys included.
  EvictExpiredReport EvictExpired(core::SteadyTime now, core::SequenceId horizon);
  // Evicts least-recently-accessed live keys until used bytes <= target_bytes.
  // Eviction is a tier transition: the key stays durable in queue/cold.
  size_t EvictLru(size_t target_bytes, core::SequenceId horizon);
  // Evicts down to the governor's budget, protecting `protect_key` (the entry a
  // write just created — it must survive so the write is applied, not evicted),
  // then reports whether the store now fits. Returns false (the caller surfaces
  // kResourceExhausted) only when, after evicting every other eligible key, the
  // store still exceeds the budget. A no-op while replaying so recovery stays a
  // deterministic queue replay (invariant 4).
  // Other keys' undrained bytes are left out of that check.
  bool EnsureCapacityFor(std::string_view protect_key, core::SequenceId horizon);

  // Suppresses memory-pressure eviction during replay. Set by the hot consumer
  // around ReplayUntil so the rebuilt hot view does not depend on memory timing.
  void SetReplayMode(bool replaying) { replay_mode_ = replaying; }

  const Stub* FindStub(std::string_view key) const { return stubs_.Find(key); }
  bool DropStub(std::string_view key);

  // A placeholder for a load of a non-resident key, taken only when the
  // key has no entry and no load in flight. Readers see a miss.
  std::optional<LoadToken> BeginLoad(std::string_view key);
  // Installs `state` only if `token` is still the key's placeholder and
  // the key has no entry; otherwise discards it and returns false.
  bool CompleteLoad(std::string_view key, LoadToken token, LoadedState state,
                    core::EvictionTTL eviction, core::SequenceId horizon);
  void AbortLoad(std::string_view key, LoadToken token);
  bool LoadPending(std::string_view key) const;
  size_t PendingLoads() const { return loading_.size(); }

  core::MemoryStats Stats() const;
  // Clears entries, stubs and placeholders for a Flush at `seq`.
  void Wipe(core::SequenceId seq);
  // A miss is absent until cold drains the last Flush.
  bool KnownAbsentAfterFlush(core::SequenceId horizon) const { return horizon < flush_seq_; }

  // Entries the LRU walk has examined.
  uint64_t LruVisitsForTesting() const { return lru_visits_; }

 private:
  core::Result<core::RespValue> ExecStringGet(const core::ops::StringGet& op) const;
  core::Result<core::RespValue> ExecSetIsMember(const core::ops::SetIsMember& op) const;
  core::Result<core::RespValue> ExecSetMembers(const core::ops::SetMembers& op) const;
  core::Result<core::RespValue> ExecSetCard(const core::ops::SetCard& op) const;
  core::Result<core::RespValue> ExecZsetScore(const core::ops::ZsetScore& op) const;
  core::Result<core::RespValue> ExecZsetCard(const core::ops::ZsetCard& op) const;
  core::Result<core::RespValue> ExecZsetRange(const core::ops::ZsetRange& op) const;
  core::Result<core::RespValue> ExecHashGet(const core::ops::HashGet& op) const;
  core::Result<core::RespValue> ExecHashGetAll(const core::ops::HashGetAll& op) const;
  core::Result<core::RespValue> ExecHashMultiGet(const core::ops::HashMultiGet& op) const;
  core::Result<core::RespValue> ExecHashFieldExists(const core::ops::HashFieldExists& op) const;
  core::Result<core::RespValue> ExecHashKeys(const core::ops::HashKeys& op) const;
  core::Result<core::RespValue> ExecHashVals(const core::ops::HashVals& op) const;
  core::Result<core::RespValue> ExecHashLen(const core::ops::HashLen& op) const;
  core::Result<core::RespValue> ExecExists(const core::ops::Exists& op) const;

  core::Result<core::RespValue> ApplyStringSet(const core::ops::StringSet& op,
                                               core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyDel(const core::ops::Del& op, core::SequenceId seq);
  core::Result<core::RespValue> ApplySetAdd(const core::ops::SetAdd& op,
                                            core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplySetRem(const core::ops::SetRem& op, core::SequenceId seq);
  core::Result<core::RespValue> ApplyZsetAdd(const core::ops::ZsetAdd& op,
                                             core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyZsetRem(const core::ops::ZsetRem& op, core::SequenceId seq);
  core::Result<core::RespValue> ApplyHashSet(const core::ops::HashSet& op,
                                             core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyHashMSet(const core::ops::HashMSet& op,
                                              core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyHashDel(const core::ops::HashDel& op, core::SequenceId seq);
  core::Result<core::RespValue> ApplyExpire(const core::ops::Expire& op);
  core::Result<core::RespValue> ApplyPersist(const core::ops::Persist& op);

  using EntryMap = std::unordered_map<std::string, Entry>;

  const Entry* FindEntry(std::string_view key) const;
  const Entry* FindLiveEntry(std::string_view key) const;
  Entry& GetOrCreateEntry(std::string_view key, Entry::Type type, core::EvictionTTL eviction);
  core::Result<const Entry*> FindTypedEntry(std::string_view key, Entry::Type expected) const;
  void RemoveEntry(const std::string& key);
  // Converts a live entry into a tombstone: releases the value, drops it from
  // key_count, and stamps the delete seq. Idempotent on an existing tombstone.
  void TombstoneEntry(Entry& entry, std::string_view key, core::SequenceId seq);
  // Stamps the key's entry with `seq` and drops its stub and any load in
  // flight, which the write has made stale.
  void MarkWritten(std::string_view key, core::SequenceId seq);
  // Removes a live entry, leaving a stub unless it expired by TTL.
  EntryMap::iterator Evict(EntryMap::iterator it, bool leave_stub);
  // A DEL of a key with no entry leaves it resident as absent.
  void InsertTombstone(std::string_view key, core::SequenceId seq);
  void LruLink(EntryMap::iterator it);
  void LruUnlink(Entry& entry);
  void LruTouch(Entry& entry);
  // An entry cold has drained may be evictable again.
  void NoteEvictable(core::SequenceId latest_seq);
  void TrackInsert(const Entry& entry, std::string_view key);
  void TrackRemove(const Entry& entry, std::string_view key);
  uint64_t UsedBytes() const { return entry_bytes_ + stubs_.bytes(); }
  void UpdateBackpressure();
  // LRU make-room primitive: evicts least-recently-accessed live keys (never
  // `protect_key`, empty to protect none) until UsedBytes() <= target_bytes.
  size_t EvictLru(size_t target_bytes, std::string_view protect_key, core::SequenceId horizon);

  SingleShardConfig config_;
  MemoryGovernor governor_;
  EntryMap entries_;
  StubCache stubs_;
  Entry* lru_newest_ = nullptr;
  Entry* lru_oldest_ = nullptr;
  // Set when an LRU walk at lru_dry_horizon_ found nothing more to evict,
  // so later writes skip the walk until something can be.
  bool lru_dry_ = false;
  core::SequenceId lru_dry_horizon_ = 0;
  uint64_t lru_visits_ = 0;
  std::unordered_map<std::string, LoadToken> loading_;
  uint64_t next_load_id_ = 0;
  uint64_t load_discards_ = 0;
  core::SequenceId flush_seq_ = 0;
  uint64_t entry_bytes_ = 0;
  // The part of entry_bytes_ that is on the LRU list.
  uint64_t live_bytes_ = 0;
  uint64_t key_count_ = 0;
  // Tier transitions: deadline eviction + memory-pressure eviction (HOT-7).
  uint64_t eviction_count_ = 0;
  // TTL-expiry deletions, distinct from tier evictions (HOT-7).
  uint64_t expired_count_ = 0;
  // Both as of the last full eviction pass that took a horizon.
  uint64_t unevictable_bytes_ = 0;
  bool backpressured_ = false;
  bool replay_mode_ = false;
};

}  // namespace abyss::hot
