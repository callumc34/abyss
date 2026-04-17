#include "abyss/hot/single_shard_store.h"

#include <algorithm>
#include <chrono>
#include <ranges>
#include <vector>

namespace abyss::hot {

namespace {

int64_t WallMs(const core::WallClockFn& clock) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(clock().time_since_epoch()).count();
}

bool IsExpiredByTtl(const Entry& entry, const core::WallClockFn& clock) {
  if (entry.abs_ttl_ms == 0) return false;
  return WallMs(clock) >= entry.abs_ttl_ms;
}

}  // namespace

size_t Entry::ApproximateBytes() const {
  size_t bytes = sizeof(Entry);
  std::visit(
      [&bytes](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string>) {
          bytes += v.capacity();
        } else if constexpr (std::is_same_v<T, SetValue>) {
          for (const auto& m : v.members) {
            bytes += sizeof(m) + m.capacity();
          }
        } else if constexpr (std::is_same_v<T, HashValue>) {
          for (const auto& [k, val] : v.fields) {
            bytes += sizeof(k) + k.capacity() + sizeof(val) + val.capacity();
          }
        } else if constexpr (std::is_same_v<T, ZsetValue>) {
          for (const auto& [m, s] : v.member_scores) {
            bytes += sizeof(m) + m.capacity() + sizeof(s);
          }
        }
      },
      value);
  return bytes;
}

SingleShardStore::SingleShardStore(SingleShardConfig config) : config_(config) {}

// --- Read operations (const) ---

core::Result<core::RespValue> SingleShardStore::Exec(const core::ops::ReadOp& op) const {
  return std::visit(
      [this](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringGet>) {
          return ExecStringGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetIsMember>) {
          return ExecSetIsMember(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetMembers>) {
          return ExecSetMembers(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetCard>) {
          return ExecSetCard(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetScore>) {
          return ExecZsetScore(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetCard>) {
          return ExecZsetCard(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRange>) {
          return ExecZsetRange(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashGet>) {
          return ExecHashGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashGetAll>) {
          return ExecHashGetAll(o);
        } else if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return ExecExists(o);
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported read operation"));
        }
      },
      op);
}

core::Result<core::RespValue> SingleShardStore::ExecStringGet(
    const core::ops::StringGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kString);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  return core::RespValue::BulkString(std::get<std::string>((*result)->value));
}

core::Result<core::RespValue> SingleShardStore::ExecSetIsMember(
    const core::ops::SetIsMember& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  return core::RespValue::Integer(members.contains(std::string(op.member)) ? 1 : 0);
}

core::Result<core::RespValue> SingleShardStore::ExecSetMembers(
    const core::ops::SetMembers& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  std::vector<core::RespValue> elements;
  elements.reserve(members.size());
  for (const auto& m : members) {
    elements.push_back(core::RespValue::BulkString(m));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecSetCard(const core::ops::SetCard& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  return core::RespValue::Integer(static_cast<int64_t>(members.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetScore(
    const core::ops::ZsetScore& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  auto it = zset.member_scores.find(std::string(op.member));
  if (it == zset.member_scores.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(std::to_string(it->second));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetCard(const core::ops::ZsetCard& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  return core::RespValue::Integer(static_cast<int64_t>(zset.member_scores.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetRange(
    const core::ops::ZsetRange& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  std::vector<core::RespValue> elements;

  if (op.by_score) {
    double min_score = -std::numeric_limits<double>::infinity();
    double max_score = std::numeric_limits<double>::infinity();
    if (!op.min.empty() && op.min != "-inf") {
      min_score = std::stod(std::string(op.min));
    }
    if (!op.max.empty() && op.max != "+inf") {
      max_score = std::stod(std::string(op.max));
    }

    if (op.rev) std::swap(min_score, max_score);

    auto lo = zset.score_members.lower_bound(std::min(min_score, max_score));
    auto hi = zset.score_members.upper_bound(std::max(min_score, max_score));

    std::vector<std::pair<std::string, double>> collected;
    for (auto it = lo; it != hi; ++it) {
      for (const auto& member : it->second) {
        collected.emplace_back(member, it->first);
      }
    }

    if (op.rev) {
      std::ranges::reverse(collected);
    }

    int64_t start = op.offset;
    int64_t count = op.count < 0 ? static_cast<int64_t>(collected.size()) : op.count;
    if (start < static_cast<int64_t>(collected.size())) {
      auto end = std::min(start + count, static_cast<int64_t>(collected.size()));
      for (int64_t i = start; i < end; ++i) {
        elements.push_back(core::RespValue::BulkString(collected[static_cast<size_t>(i)].first));
        if (op.with_scores) {
          elements.push_back(core::RespValue::BulkString(
              std::to_string(collected[static_cast<size_t>(i)].second)));
        }
      }
    }
  } else {
    // Index-based range — convert to score iteration
    const auto& sm = zset.score_members;
    std::vector<std::pair<std::string, double>> all;
    for (const auto& [score, members] : sm) {
      for (const auto& member : members) {
        all.emplace_back(member, score);
      }
    }
    if (op.rev) {
      std::ranges::reverse(all);
    }

    int64_t min_idx = 0;
    int64_t max_idx = static_cast<int64_t>(all.size()) - 1;
    if (!op.min.empty()) min_idx = std::stoll(std::string(op.min));
    if (!op.max.empty()) max_idx = std::stoll(std::string(op.max));

    if (min_idx < 0) min_idx += static_cast<int64_t>(all.size());
    if (max_idx < 0) max_idx += static_cast<int64_t>(all.size());
    min_idx = std::max(min_idx, int64_t{0});
    max_idx = std::min(max_idx, static_cast<int64_t>(all.size()) - 1);

    for (int64_t i = min_idx; i <= max_idx; ++i) {
      elements.push_back(core::RespValue::BulkString(all[static_cast<size_t>(i)].first));
      if (op.with_scores) {
        elements.push_back(
            core::RespValue::BulkString(std::to_string(all[static_cast<size_t>(i)].second)));
      }
    }
  }

  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashGet(const core::ops::HashGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  auto it = fields.find(std::string(op.field));
  if (it == fields.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(it->second);
}

core::Result<core::RespValue> SingleShardStore::ExecHashGetAll(
    const core::ops::HashGetAll& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size() * 2);
  for (const auto& [k, v] : fields) {
    elements.push_back(core::RespValue::BulkString(k));
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecExists(const core::ops::Exists& op) const {
  int64_t count = 0;
  for (auto key : op.keys) {
    if (FindLiveEntry(key) != nullptr) ++count;
  }
  return core::RespValue::Integer(count);
}

// --- Write operations ---

core::Result<void> SingleShardStore::Apply(const core::ops::WriteOp& op,
                                           core::EvictionTTL eviction) {
  return std::visit(
      [this, eviction](const auto& o) -> core::Result<void> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringSet>) {
          return ApplyStringSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetAdd>) {
          return ApplySetAdd(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::SetRem>) {
          return ApplySetRem(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetAdd>) {
          return ApplyZsetAdd(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRem>) {
          return ApplyZsetRem(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashSet>) {
          return ApplyHashSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::HashDel>) {
          return ApplyHashDel(o);
        } else if constexpr (std::is_same_v<T, core::ops::MultiStringSet>) {
          return ApplyMultiStringSet(o, eviction);
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported write operation"));
        }
      },
      op);
}

core::Result<void> SingleShardStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                                core::EvictionTTL eviction) {
  for (const auto& op : ops) {
    auto result = Apply(op, eviction);
    if (!result.has_value()) return result;
  }
  return {};
}

core::Result<void> SingleShardStore::ApplyStringSet(const core::ops::StringSet& op,
                                                    core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end()) {
    if (it->second.type != Entry::Type::kString) {
      TrackRemove(it->second, op.key);
      it->second.type = Entry::Type::kString;
      it->second.value = std::string(op.value);
      it->second.eviction_deadline = config_.steady_clock() + eviction;
      it->second.last_access = config_.steady_clock();
      it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
      TrackInsert(it->second, op.key);
      return {};
    }
    TrackRemove(it->second, op.key);
    std::get<std::string>(it->second.value) = std::string(op.value);
    it->second.eviction_deadline = config_.steady_clock() + eviction;
    it->second.last_access = config_.steady_clock();
    it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
    TrackInsert(it->second, op.key);
    return {};
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kString, eviction);
  entry.value = std::string(op.value);
  entry.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
  TrackInsert(entry, op.key);
  return {};
}

core::Result<void> SingleShardStore::ApplyDel(const core::ops::Del& op) {
  for (auto key : op.keys) {
    RemoveEntry(std::string(key));
  }
  return {};
}

core::Result<void> SingleShardStore::ApplySetAdd(const core::ops::SetAdd& op,
                                                 core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && it->second.type != Entry::Type::kSet) {
    if (!IsExpiredByTtl(it->second, config_.wall_clock) && it->second.type != Entry::Type::kSet) {
      return std::unexpected(
          core::Error(core::ErrorCode::kWrongType,
                      "WRONGTYPE Operation against a key holding the wrong kind of value"));
    }
    RemoveEntry(std::string(op.key));
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kSet, eviction);
  TrackRemove(entry, op.key);
  auto& members = std::get<SetValue>(entry.value).members;
  for (auto member : op.members) {
    members.insert(std::string(member));
  }
  entry.eviction_deadline = config_.steady_clock() + eviction;
  entry.last_access = config_.steady_clock();
  TrackInsert(entry, op.key);
  return {};
}

core::Result<void> SingleShardStore::ApplySetRem(const core::ops::SetRem& op) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end() || IsExpiredByTtl(it->second, config_.wall_clock)) return {};
  if (it->second.type != Entry::Type::kSet) {
    return std::unexpected(
        core::Error(core::ErrorCode::kWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& members = std::get<SetValue>(it->second.value).members;
  for (auto member : op.members) {
    members.erase(std::string(member));
  }
  TrackInsert(it->second, op.key);
  if (members.empty()) {
    RemoveEntry(std::string(op.key));
  }
  return {};
}

core::Result<void> SingleShardStore::ApplyZsetAdd(const core::ops::ZsetAdd& op,
                                                  core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !IsExpiredByTtl(it->second, config_.wall_clock) &&
      it->second.type != Entry::Type::kZset) {
    return std::unexpected(
        core::Error(core::ErrorCode::kWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kZset, eviction);
  TrackRemove(entry, op.key);
  auto& zset = std::get<ZsetValue>(entry.value);

  for (const auto& e : op.entries) {
    std::string member(e.member);
    auto existing = zset.member_scores.find(member);
    if (existing != zset.member_scores.end()) {
      double old_score = existing->second;
      zset.score_members[old_score].erase(member);
      if (zset.score_members[old_score].empty()) {
        zset.score_members.erase(old_score);
      }
    }
    zset.member_scores[member] = e.score;
    zset.score_members[e.score].insert(std::move(member));
  }

  entry.eviction_deadline = config_.steady_clock() + eviction;
  entry.last_access = config_.steady_clock();
  TrackInsert(entry, op.key);
  return {};
}

core::Result<void> SingleShardStore::ApplyZsetRem(const core::ops::ZsetRem& op) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end() || IsExpiredByTtl(it->second, config_.wall_clock)) return {};
  if (it->second.type != Entry::Type::kZset) {
    return std::unexpected(
        core::Error(core::ErrorCode::kWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& zset = std::get<ZsetValue>(it->second.value);

  for (auto member : op.members) {
    std::string m(member);
    auto score_it = zset.member_scores.find(m);
    if (score_it != zset.member_scores.end()) {
      double score = score_it->second;
      zset.score_members[score].erase(m);
      if (zset.score_members[score].empty()) {
        zset.score_members.erase(score);
      }
      zset.member_scores.erase(score_it);
    }
  }

  TrackInsert(it->second, op.key);
  if (zset.member_scores.empty()) {
    RemoveEntry(std::string(op.key));
  }
  return {};
}

core::Result<void> SingleShardStore::ApplyHashSet(const core::ops::HashSet& op,
                                                  core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !IsExpiredByTtl(it->second, config_.wall_clock) &&
      it->second.type != Entry::Type::kHash) {
    return std::unexpected(
        core::Error(core::ErrorCode::kWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kHash, eviction);
  TrackRemove(entry, op.key);
  auto& fields = std::get<HashValue>(entry.value).fields;
  for (const auto& fv : op.fields) {
    fields[std::string(fv.field)] = std::string(fv.value);
  }
  entry.eviction_deadline = config_.steady_clock() + eviction;
  entry.last_access = config_.steady_clock();
  TrackInsert(entry, op.key);
  return {};
}

core::Result<void> SingleShardStore::ApplyHashDel(const core::ops::HashDel& op) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end() || IsExpiredByTtl(it->second, config_.wall_clock)) return {};
  if (it->second.type != Entry::Type::kHash) {
    return std::unexpected(
        core::Error(core::ErrorCode::kWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& fields = std::get<HashValue>(it->second.value).fields;
  for (auto field : op.fields) {
    fields.erase(std::string(field));
  }
  TrackInsert(it->second, op.key);
  if (fields.empty()) {
    RemoveEntry(std::string(op.key));
  }
  return {};
}

core::Result<void> SingleShardStore::ApplyMultiStringSet(const core::ops::MultiStringSet& op,
                                                         core::EvictionTTL eviction) {
  for (const auto& e : op.entries) {
    auto result = ApplyStringSet(core::ops::StringSet{.key = e.key, .value = e.value}, eviction);
    if (!result.has_value()) return result;
  }
  return {};
}

// --- Maintenance ---

void SingleShardStore::RefreshAccess(std::string_view key, core::SteadyTime now,
                                     core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return;
  it->second.last_access = now;
  it->second.eviction_deadline = now + eviction;
}

size_t SingleShardStore::EvictExpired(core::SteadyTime now) {
  size_t count = 0;
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (it->second.eviction_deadline <= now || IsExpiredByTtl(it->second, config_.wall_clock)) {
      TrackRemove(it->second, it->first);
      key_count_--;
      eviction_count_++;
      it = entries_.erase(it);
      ++count;
    } else {
      ++it;
    }
  }
  return count;
}

size_t SingleShardStore::EvictLru(size_t target_bytes) {
  if (used_bytes_ <= target_bytes) return 0;

  std::vector<std::pair<core::SteadyTime, std::string>> candidates;
  candidates.reserve(entries_.size());
  for (const auto& [key, entry] : entries_) {
    candidates.emplace_back(entry.last_access, key);
  }

  std::ranges::sort(candidates);

  size_t evicted = 0;
  for (const auto& [access_time, key] : candidates) {
    if (used_bytes_ <= target_bytes) break;
    RemoveEntry(key);
    eviction_count_++;
    ++evicted;
  }
  return evicted;
}

core::MemoryStats SingleShardStore::Stats() const {
  return {.used_bytes = used_bytes_, .key_count = key_count_, .eviction_count = eviction_count_};
}

void SingleShardStore::Flush() {
  entries_.clear();
  used_bytes_ = 0;
  key_count_ = 0;
}

// --- Internal helpers ---

const Entry* SingleShardStore::FindEntry(std::string_view key) const {
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return nullptr;
  return &it->second;
}

const Entry* SingleShardStore::FindLiveEntry(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return nullptr;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return nullptr;
  return entry;
}

Entry& SingleShardStore::GetOrCreateEntry(std::string_view key, Entry::Type type,
                                          core::EvictionTTL eviction) {
  auto [it, inserted] = entries_.try_emplace(std::string(key));
  if (inserted) {
    it->second.type = type;
    it->second.eviction_deadline = config_.steady_clock() + eviction;
    it->second.last_access = config_.steady_clock();
    switch (type) {
      case Entry::Type::kString:
        it->second.value = std::string{};
        break;
      case Entry::Type::kSet:
        it->second.value = SetValue{};
        break;
      case Entry::Type::kHash:
        it->second.value = HashValue{};
        break;
      case Entry::Type::kZset:
        it->second.value = ZsetValue{};
        break;
    }
    key_count_++;
  }
  return it->second;
}

core::Result<const Entry*> SingleShardStore::FindTypedEntry(std::string_view key,
                                                            Entry::Type expected) const {
  const auto* entry = FindLiveEntry(key);
  if (entry == nullptr) return nullptr;
  if (entry->type != expected) {
    return std::unexpected(
        core::Error(core::ErrorCode::kWrongType,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"));
  }
  return entry;
}

void SingleShardStore::RemoveEntry(const std::string& key) {
  auto it = entries_.find(key);
  if (it == entries_.end()) return;
  TrackRemove(it->second, key);
  key_count_--;
  entries_.erase(it);
}

void SingleShardStore::TrackInsert(const Entry& entry, std::string_view key) {
  used_bytes_ += entry.ApproximateBytes() + sizeof(std::string) + key.size();
}

void SingleShardStore::TrackRemove(const Entry& entry, std::string_view key) {
  auto bytes = entry.ApproximateBytes() + sizeof(std::string) + key.size();
  used_bytes_ = (used_bytes_ >= bytes) ? used_bytes_ - bytes : 0;
}

}  // namespace abyss::hot
