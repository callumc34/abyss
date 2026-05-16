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

  size_t ApproximateBytes() const;
};

class SingleShardStore {
 public:
  explicit SingleShardStore(SingleShardConfig config);

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const;

  core::Result<core::RespValue> Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction);
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::EvictionTTL eviction);

  // Extends the eviction deadline using the per-key cached eviction recorded
  // at Apply time. No-op if the key is absent.
  void RefreshAccess(std::string_view key, core::SteadyTime now);
  size_t EvictExpired(core::SteadyTime now);
  size_t EvictLru(size_t target_bytes);

  core::MemoryStats Stats() const;
  void Flush();

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
  core::Result<core::RespValue> ApplyDel(const core::ops::Del& op);
  core::Result<core::RespValue> ApplySetAdd(const core::ops::SetAdd& op,
                                            core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplySetRem(const core::ops::SetRem& op);
  core::Result<core::RespValue> ApplyZsetAdd(const core::ops::ZsetAdd& op,
                                             core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyZsetRem(const core::ops::ZsetRem& op);
  core::Result<core::RespValue> ApplyHashSet(const core::ops::HashSet& op,
                                             core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyHashMSet(const core::ops::HashMSet& op,
                                              core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyHashDel(const core::ops::HashDel& op);
  core::Result<core::RespValue> ApplyMultiStringSet(const core::ops::MultiStringSet& op,
                                                    core::EvictionTTL eviction);
  core::Result<core::RespValue> ApplyExpire(const core::ops::Expire& op);
  core::Result<core::RespValue> ApplyPersist(const core::ops::Persist& op);

  const Entry* FindEntry(std::string_view key) const;
  const Entry* FindLiveEntry(std::string_view key) const;
  Entry& GetOrCreateEntry(std::string_view key, Entry::Type type, core::EvictionTTL eviction);
  core::Result<const Entry*> FindTypedEntry(std::string_view key, Entry::Type expected) const;
  void RemoveEntry(const std::string& key);
  void TrackInsert(const Entry& entry, std::string_view key);
  void TrackRemove(const Entry& entry, std::string_view key);

  SingleShardConfig config_;
  std::unordered_map<std::string, Entry> entries_;
  uint64_t used_bytes_ = 0;
  uint64_t key_count_ = 0;
  uint64_t eviction_count_ = 0;
};

}  // namespace abyss::hot
