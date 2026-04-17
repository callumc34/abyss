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
  int64_t abs_ttl_ms = 0;

  size_t ApproximateBytes() const;
};

class SingleShardStore {
 public:
  explicit SingleShardStore(SingleShardConfig config);

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op) const;

  core::Result<void> Apply(const core::ops::WriteOp& op, core::EvictionTTL eviction);
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::EvictionTTL eviction);

  void RefreshAccess(std::string_view key, core::SteadyTime now, core::EvictionTTL eviction);
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
  core::Result<core::RespValue> ExecExists(const core::ops::Exists& op) const;

  core::Result<void> ApplyStringSet(const core::ops::StringSet& op, core::EvictionTTL eviction);
  core::Result<void> ApplyDel(const core::ops::Del& op);
  core::Result<void> ApplySetAdd(const core::ops::SetAdd& op, core::EvictionTTL eviction);
  core::Result<void> ApplySetRem(const core::ops::SetRem& op);
  core::Result<void> ApplyZsetAdd(const core::ops::ZsetAdd& op, core::EvictionTTL eviction);
  core::Result<void> ApplyZsetRem(const core::ops::ZsetRem& op);
  core::Result<void> ApplyHashSet(const core::ops::HashSet& op, core::EvictionTTL eviction);
  core::Result<void> ApplyHashDel(const core::ops::HashDel& op);
  core::Result<void> ApplyMultiStringSet(const core::ops::MultiStringSet& op,
                                         core::EvictionTTL eviction);

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
