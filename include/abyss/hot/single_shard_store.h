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
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/core/eviction_policy.h"
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

// What an entry holding `value` counts, beyond its key.
size_t ApproximateBytes(const Value& value);

// What an evicted live key leaves behind.
struct Stub {
  Entry::Type type = Entry::Type::kString;
  int64_t abs_ttl_ms = 0;
  core::SequenceId latest_seq = 0;
};

// Accounted size of one stub beyond its key; also sizes the stub cap.
inline constexpr size_t kStubBytes = 80;

// One key's state, valid while the shard lock it was taken under is
// held.
struct KeyView {
  enum class Presence : uint8_t {
    kLive,
    kTombstoned,
    // Past its TTL: absent, but its entry or stub is still held.
    kExpired,
    // Not resident; its stub holds type, TTL and latest_seq.
    kStub,
    // Not resident and unknown: no entry or stub, or a load in flight.
    kNonResident,
  };

  Presence presence = Presence::kNonResident;
  // Absent by the shard's flush floor: kTombstoned, at the Flush's seq.
  bool flush_floor = false;
  Entry::Type type = Entry::Type::kString;
  int64_t abs_ttl_ms = 0;
  core::SequenceId latest_seq = 0;
  // The whole value of a live or expired entry.
  const Value* value = nullptr;
  core::ShardId shard = 0;

  // Each is empty, false or 0 when the view holds no such value.
  std::string_view string_value() const;
  bool set_has(std::string_view member) const;
  std::optional<double> zset_score(std::string_view member) const;
  bool hash_has(std::string_view field) const;
  std::optional<std::string_view> hash_get(std::string_view field) const;
  // Members or fields.
  size_t collection_size() const;
};

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
  // Drops the oldest stubs past the cap, unless `trim` is false: a load
  // batch keeps its own stubs, and the next Put trims.
  void Put(std::string_view key, const Stub& stub, bool trim = true);
  bool Erase(std::string_view key);
  void Clear();
  // Moves every stub out, keeping the cap and the drop count.
  StubCache Release();

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

// What applies under an exclusive hold replaced or removed, freed once
// the hold ends, so no large free runs under the lock.
struct Graveyard {
  std::vector<Value> values;
  std::vector<std::unordered_map<std::string, Entry>> entries;
  std::vector<StubCache> stubs;
};

struct LoadToken {
  uint64_t id = 0;
  bool operator==(const LoadToken&) const = default;
};

// An attempt to start a load, judged in one hold.
struct LoadStart {
  enum class Status : uint8_t {
    kStarted,
    // The key has an entry or tombstone: hot answers.
    kResident,
    // Another load is in flight: await it.
    kPending,
    // The flush floor makes the key absent.
    kFlushed,
  };
  Status status = Status::kResident;
  LoadToken token;

  bool started() const { return status == Status::kStarted; }
  std::optional<LoadToken> token_if_started() const {
    return started() ? std::optional<LoadToken>(token) : std::nullopt;
  }
};

// What a load of a non-resident key found in buffer and cold. Each is
// installable, so a decide that asked for it never asks again.
struct LoadedAbsent {
  bool operator==(const LoadedAbsent&) const = default;
};

// An existence probe's answer.
struct LoadedExists {
  Entry::Type type = Entry::Type::kString;
  int64_t abs_ttl_ms = 0;

  bool operator==(const LoadedExists&) const = default;
};

struct LoadedFull {
  Value value;
  int64_t abs_ttl_ms = 0;
  // ApproximateBytes(value), measured off the lock.
  size_t bytes = 0;

  Entry::Type type() const { return static_cast<Entry::Type>(value.index()); }
};

// Measures `value`; call it off the lock.
LoadedFull MakeLoadedFull(Value value, int64_t abs_ttl_ms);

using LoadResult = std::variant<LoadedAbsent, LoadedExists, LoadedFull>;

struct LoadCompletion {
  std::string key;
  LoadToken token;
  // Moved from only when installed.
  LoadResult result;
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

  // `horizon` is the shard's cold drained seq, read under the lock;
  // TTL expiry is judged at `now_ms`.
  KeyView View(std::string_view key, core::SequenceId horizon, uint64_t now_ms) const;

  // Applies decided effects in order, effect i at `first_seq + i`, and
  // returns each one's reply. It judges no TTL: decide logged a DEL for
  // every expired key it read. `appended_at` is only for the DCHECK
  // that no effect reading state meets a key expired at that time. A
  // SET's value is moved out of its effect. An unparsable effect is
  // fatal.
  std::vector<core::RespValue> ApplyEffects(std::span<core::Effect> effects,
                                            core::SequenceId first_seq, core::WallTime appended_at,
                                            const core::EvictionPolicy& policy,
                                            core::SequenceId horizon);

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
  // Installs `result` only if `token` is still the key's placeholder
  // and the key has no entry; otherwise discards it, leaving `result`
  // intact, and returns false. Absent installs a drained tombstone and
  // Exists a stub, replacing any stub.
  bool CompleteLoad(std::string_view key, LoadToken token, LoadResult&& result,
                    core::EvictionTTL eviction, core::SequenceId horizon);
  // CompleteLoad of each, in one hold; returns how many installed. It
  // neither evicts nor drops stubs, so the batch keeps every install
  // even past the budget or the stub cap; the next eviction trims.
  size_t CompleteLoads(std::span<LoadCompletion> loads, const core::EvictionPolicy& policy);
  void AbortLoad(std::string_view key, LoadToken token);
  bool LoadPending(std::string_view key) const;
  size_t PendingLoads() const { return loading_.size(); }
  // Whether a stub, and so an existence load, can be held.
  bool RetainsStubs() const { return config_.stub_max_entries > 0; }

  core::MemoryStats Stats() const;
  // Clears entries, stubs and placeholders for a Flush at `seq`.
  void Wipe(core::SequenceId seq);
  // A miss is absent until cold drains the last Flush.
  bool KnownAbsentAfterFlush(core::SequenceId horizon) const { return horizon < flush_seq_; }

  struct ReadAnswer {
    core::Result<core::RespValue> result;
    // What the reply must be durable through; nullopt on a miss.
    std::optional<core::SequenceId> fence;
  };
  // Exec of a single-key read, fenced on the answering entry's
  // latest_seq. An entry deleted or past its TTL answers absent for the
  // op's shape, as does a miss under the flush floor, fenced on the
  // Flush. Only a key with no entry misses.
  ReadAnswer Read(const core::ops::ReadOp& op, core::SequenceId horizon) const;
  // What a reply that observed `seq` waits for: loaded state carries 0
  // too, so a 0 needs no wait unless a write here was at seq 0.
  std::optional<core::SequenceId> FenceFor(core::SequenceId seq) const;

  // While set, applies move what they replace or remove into
  // `graveyard` instead of freeing it.
  void SetGraveyard(Graveyard* graveyard) { graveyard_ = graveyard; }
  // The highest appended_at given to the shard's writes.
  core::WallTime LastAppendedAt() const { return last_appended_at_; }
  // An entry, live, expired or a tombstone.
  bool HasEntry(std::string_view key) const { return FindEntry(key) != nullptr; }
  void RaiseAppendedAt(core::WallTime at);
  // Over max_memory_bytes times the backpressure ratio, as of now.
  bool OverBackpressure() const;

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

  // A SET's value to move in rather than copy, and whether the SET
  // replies with the string it replaces.
  struct SetMove {
    std::string* value = nullptr;
    bool reply_old_value = false;
  };

  // Apply without the capacity check.
  core::Result<core::RespValue> Mutate(const core::ops::WriteOp& op, core::EvictionTTL eviction,
                                       core::SequenceId seq, SetMove move);
  core::Result<core::RespValue> ApplyStringSet(const core::ops::StringSet& op,
                                               core::EvictionTTL eviction, SetMove move);
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

  bool InstallLoad(std::string_view key, LoadToken token, LoadResult&& result,
                   core::EvictionTTL eviction, core::SequenceId horizon, bool in_batch);
  // Moves `value` to the graveyard, if one is set; it is then reassigned
  // or erased by the caller.
  void Bury(Value& value);
  const Entry* FindEntry(std::string_view key) const;
  const Entry* FindLiveEntry(std::string_view key) const;
  // TTL expiry as writes see it: never while applying effects.
  bool ExpiredForApply(const Entry& entry) const;
  // False when `effect` reads `key` past its TTL at `at_ms`: decide
  // logs such an expiry as a DEL before the read.
  bool ExpiryIsLogged(const core::Effect& effect, std::string_view key, int64_t at_ms) const;
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
  // Set while ApplyEffects runs.
  bool applying_effects_ = false;
  bool applied_seq_zero_ = false;
  core::WallTime last_appended_at_{};
  Graveyard* graveyard_ = nullptr;
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
