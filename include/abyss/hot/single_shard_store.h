#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
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

namespace abyss::hot {

struct SingleShardConfig {
  size_t max_memory_bytes = 0;
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
  core::SteadyTime last_access;
  // Resolved per-prefix eviction for this key.
  core::EvictionTTL eviction{};
  int64_t abs_ttl_ms = 0;

  // Marks a deleted key whose delete cold may not have absorbed yet.
  bool tombstoned = false;
  core::SequenceId tombstone_seq = 0;

  size_t ApproximateBytes() const;
};

class SingleShardStore {
 public:
  explicit SingleShardStore(SingleShardConfig config);

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const;

  // `seq` is the queue seq of the op; it is recorded on the tombstone a delete
  // leaves behind so the GC can reclaim it once cold catches up. Defaults to 0
  // (the earliest position) for callers that do not exercise tombstone GC.
  core::Result<core::RespValue> Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction,
                                      core::SequenceId seq = 0);
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops, core::EvictionTTL eviction,
                                core::SequenceId seq = 0);

  // Existence verdict distinguishing a delete-tombstone (authoritatively
  // absent) from a true miss (consult the next tier). See core::HotKeyPresence.
  core::HotKeyPresence Probe(std::string_view key) const;

  // Reclaims tombstones whose delete seq is <= `horizon` — cold has absorbed
  // those deletes, so the buffer/cold view now reflects them. Live keys remain.
  size_t GcTombstones(core::SequenceId horizon);

  // Extends the eviction deadline using the per-key cached eviction recorded
  // at Apply time. No-op if the key is absent.
  void RefreshAccess(std::string_view key, core::SteadyTime now);
  struct EvictExpiredReport {
    size_t by_deadline = 0;
    size_t by_ttl = 0;
    size_t Total() const { return by_deadline + by_ttl; }
  };
  EvictExpiredReport EvictExpired(core::SteadyTime now);
  size_t EvictLru(size_t target_bytes);

  core::MemoryStats Stats() const;
  void Wipe();

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

  const Entry* FindEntry(std::string_view key) const;
  const Entry* FindLiveEntry(std::string_view key) const;
  Entry& GetOrCreateEntry(std::string_view key, Entry::Type type, core::EvictionTTL eviction);
  core::Result<const Entry*> FindTypedEntry(std::string_view key, Entry::Type expected) const;
  void RemoveEntry(const std::string& key);
  // Converts a live entry into a tombstone: releases the value, drops it from
  // key_count, and stamps the delete seq. Idempotent on an existing tombstone.
  void TombstoneEntry(Entry& entry, std::string_view key, core::SequenceId seq);
  void TrackInsert(const Entry& entry, std::string_view key);
  void TrackRemove(const Entry& entry, std::string_view key);

  SingleShardConfig config_;
  std::unordered_map<std::string, Entry> entries_;
  uint64_t used_bytes_ = 0;
  uint64_t key_count_ = 0;
  uint64_t eviction_count_ = 0;
};

}  // namespace abyss::hot
