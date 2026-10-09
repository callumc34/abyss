#include "abyss/consumer/compacted_state.h"

#include <string>
#include <variant>

namespace abyss::consumer {

const std::string CompactedState::kEmpty;

namespace {

struct AbsorbVisitor {
  CompactedState::DataType& type;
  bool& is_tombstone;
  std::optional<std::string>& string_value;
  uint64_t& abs_ttl_ms;
  std::unordered_set<std::string>& set_members;
  std::unordered_set<std::string>& set_removed_members;
  std::unordered_map<std::string, double>& zset_members;
  std::unordered_set<std::string>& zset_removed_members;
  std::unordered_map<std::string, std::string>& hash_fields;
  std::unordered_set<std::string>& hash_removed_fields;
  CompactedState::BaseInvalidation& base_invalidation;
  CompactedState::TtlIntent& ttl_intent;

  void operator()(const core::ops::StringSet& s) {
    // SET overrides any existing type. A SET over a cold-resident collection or
    // hash must wipe the prior type's slices first (COLDC-3); a SET over a
    // string (or fresh state) overwrites the same key and needs no leading Del.
    if (type == CompactedState::DataType::kSet || type == CompactedState::DataType::kZset ||
        type == CompactedState::DataType::kHash) {
      base_invalidation = CompactedState::BaseInvalidation::kDeleteAll;
    }
    type = CompactedState::DataType::kString;
    is_tombstone = false;
    string_value = std::string(s.value);
    abs_ttl_ms = s.abs_ttl_ms;
    // SET carries its own TTL inline; reset the trailing-op intent so a prior
    // EXPIRE/PERSIST in this window does not double-encode over the new value.
    ttl_intent = CompactedState::TtlIntent::kUnchanged;
    set_members.clear();
    set_removed_members.clear();
    zset_members.clear();
    zset_removed_members.clear();
    hash_fields.clear();
    hash_removed_fields.clear();
  }

  void operator()(const core::ops::Del& /*unused*/) {
    // A DEL resets all accumulated state and pins a destructive base
    // invalidation so a subsequent re-add in the same window emits a leading
    // Del that clears any cold-resident slices of any prior type (ADP-004
    // invariant 3, COLDC-3).
    type = CompactedState::DataType::kNone;
    is_tombstone = true;
    base_invalidation = CompactedState::BaseInvalidation::kDeleteAll;
    ttl_intent = CompactedState::TtlIntent::kUnchanged;
    string_value.reset();
    abs_ttl_ms = 0;
    set_members.clear();
    set_removed_members.clear();
    zset_members.clear();
    zset_removed_members.clear();
    hash_fields.clear();
    hash_removed_fields.clear();
  }

  void operator()(const core::ops::Expire& e) {
    abs_ttl_ms = e.abs_ttl_ms;
    ttl_intent = CompactedState::TtlIntent::kSetTo;
  }

  void operator()(const core::ops::Persist& /*unused*/) {
    abs_ttl_ms = 0;
    ttl_intent = CompactedState::TtlIntent::kCleared;
  }

  void operator()(const core::ops::SetAdd& s) {
    if (type != CompactedState::DataType::kSet && type != CompactedState::DataType::kNone) {
      return;
    }
    type = CompactedState::DataType::kSet;
    is_tombstone = false;
    for (auto m : s.members) {
      auto key = std::string(m);
      set_removed_members.erase(key);
      set_members.insert(std::move(key));
    }
  }

  void operator()(const core::ops::SetRem& s) {
    if (type != CompactedState::DataType::kSet && type != CompactedState::DataType::kNone) {
      return;
    }
    type = CompactedState::DataType::kSet;
    for (auto m : s.members) {
      auto key = std::string(m);
      set_members.erase(key);
      set_removed_members.insert(std::move(key));
    }
  }

  void operator()(const core::ops::ZsetAdd& z) {
    if (type != CompactedState::DataType::kZset && type != CompactedState::DataType::kNone) {
      return;
    }
    type = CompactedState::DataType::kZset;
    is_tombstone = false;
    for (const auto& e : z.entries) {
      auto key = std::string(e.member);
      zset_removed_members.erase(key);
      zset_members[std::move(key)] = e.score;
    }
  }

  void operator()(const core::ops::ZsetRem& z) {
    if (type != CompactedState::DataType::kZset && type != CompactedState::DataType::kNone) {
      return;
    }
    type = CompactedState::DataType::kZset;
    for (auto m : z.members) {
      auto key = std::string(m);
      zset_members.erase(key);
      zset_removed_members.insert(std::move(key));
    }
  }

  // HSET and HMSET differ only at the hot store's reply (count of new fields
  // vs +OK); the cold-side compaction merges them identically.
  void AbsorbHashFields(const std::vector<core::ops::HashSet::FieldValue>& fields) {
    if (type != CompactedState::DataType::kHash && type != CompactedState::DataType::kNone) {
      return;
    }
    type = CompactedState::DataType::kHash;
    is_tombstone = false;
    for (const auto& fv : fields) {
      auto field = std::string(fv.field);
      hash_removed_fields.erase(field);
      hash_fields[std::move(field)] = std::string(fv.value);
    }
  }

  void operator()(const core::ops::HashSet& h) { AbsorbHashFields(h.fields); }

  void operator()(const core::ops::HashMSet& h) { AbsorbHashFields(h.fields); }

  void operator()(const core::ops::HashDel& h) {
    if (type != CompactedState::DataType::kHash && type != CompactedState::DataType::kNone) {
      return;
    }
    type = CompactedState::DataType::kHash;
    for (auto f : h.fields) {
      auto field = std::string(f);
      hash_fields.erase(field);
      hash_removed_fields.insert(std::move(field));
    }
  }
};

}  // namespace

void CompactedState::Absorb(const core::ops::WriteOp& op) {
  AbsorbVisitor visitor{
      .type = type_,
      .is_tombstone = is_tombstone_,
      .string_value = string_value_,
      .abs_ttl_ms = abs_ttl_ms_,
      .set_members = set_members_,
      .set_removed_members = set_removed_members_,
      .zset_members = zset_members_,
      .zset_removed_members = zset_removed_members_,
      .hash_fields = hash_fields_,
      .hash_removed_fields = hash_removed_fields_,
      .base_invalidation = base_invalidation_,
      .ttl_intent = ttl_intent_,
  };
  std::visit(visitor, op);
}

std::vector<core::ops::WriteOp> CompactedState::Emit() const {
  std::vector<core::ops::WriteOp> result;

  if (is_tombstone_) {
    return result;
  }

  // Lossless ordering (A4): leading destructive Del, then type add ops, then
  // rem ops, then exactly one trailing TTL op. Keys are patched by
  // BuildBatchOps. The leading Del is applied before the re-adds within the
  // same WriteBatchWithIndex so cold wipes prior slices of any type first.
  if (base_invalidation_ == BaseInvalidation::kDeleteAll) {
    result.emplace_back(core::ops::Del{.keys = {}});
  }

  switch (type_) {
    case DataType::kNone:
      break;
    case DataType::kString:
      if (string_value_.has_value()) {
        // For strings, the TTL intent folds into StringSet.abs_ttl_ms; the
        // trailing TTL op is suppressed to avoid double-encoding.
        result.emplace_back(core::ops::StringSet{
            .key = {},
            .value = *string_value_,
            .abs_ttl_ms = abs_ttl_ms_,
        });
      }
      break;
    case DataType::kSet:
      if (!set_members_.empty()) {
        std::vector<std::string_view> members;
        members.reserve(set_members_.size());
        for (const auto& m : set_members_) {
          members.emplace_back(m);
        }
        result.emplace_back(core::ops::SetAdd{.key = {}, .members = std::move(members)});
      }
      // Net removals must reach cold.
      if (!set_removed_members_.empty()) {
        std::vector<std::string_view> removed;
        removed.reserve(set_removed_members_.size());
        for (const auto& m : set_removed_members_) {
          removed.emplace_back(m);
        }
        result.emplace_back(core::ops::SetRem{.key = {}, .members = std::move(removed)});
      }
      break;
    case DataType::kZset:
      if (!zset_members_.empty()) {
        std::vector<core::ops::ZsetAdd::Entry> entries;
        entries.reserve(zset_members_.size());
        for (const auto& [member, score] : zset_members_) {
          entries.push_back({.score = score, .member = member});
        }
        result.emplace_back(core::ops::ZsetAdd{.key = {}, .entries = std::move(entries)});
      }
      if (!zset_removed_members_.empty()) {
        std::vector<std::string_view> removed;
        removed.reserve(zset_removed_members_.size());
        for (const auto& m : zset_removed_members_) {
          removed.emplace_back(m);
        }
        result.emplace_back(core::ops::ZsetRem{.key = {}, .members = std::move(removed)});
      }
      break;
    case DataType::kHash:
      if (!hash_fields_.empty()) {
        std::vector<core::ops::HashSet::FieldValue> fields;
        fields.reserve(hash_fields_.size());
        for (const auto& [field, value] : hash_fields_) {
          fields.push_back({.field = field, .value = value});
        }
        result.emplace_back(core::ops::HashSet{.key = {}, .fields = std::move(fields)});
      }
      if (!hash_removed_fields_.empty()) {
        std::vector<std::string_view> removed;
        removed.reserve(hash_removed_fields_.size());
        for (const auto& f : hash_removed_fields_) {
          removed.emplace_back(f);
        }
        result.emplace_back(core::ops::HashDel{.key = {}, .fields = std::move(removed)});
      }
      break;
  }

  // Exactly one trailing TTL op for collections (COLDC-2). Strings fold the TTL
  // into StringSet above, so the trailing op is suppressed for kString. An
  // EXPIRE/PERSIST-only window on a kNone non-tombstone state emits only this.
  if (type_ != DataType::kString && ttl_intent_ != TtlIntent::kUnchanged) {
    if (ttl_intent_ == TtlIntent::kSetTo) {
      result.emplace_back(core::ops::Expire{.key = {}, .abs_ttl_ms = abs_ttl_ms_});
    } else {
      result.emplace_back(core::ops::Persist{.key = {}});
    }
  }

  return result;
}

size_t CompactedState::EstimatedBytes() const {
  switch (type_) {
    case DataType::kNone:
      return 0;
    case DataType::kString:
      return string_value_.has_value() ? string_value_->size() : 0;
    case DataType::kSet: {
      size_t bytes = 0;
      for (const auto& m : set_members_) bytes += m.size();
      for (const auto& m : set_removed_members_) bytes += m.size();
      return bytes;
    }
    case DataType::kZset: {
      size_t bytes = 0;
      for (const auto& [m, _] : zset_members_) bytes += m.size() + sizeof(double);
      for (const auto& m : zset_removed_members_) bytes += m.size();
      return bytes;
    }
    case DataType::kHash: {
      size_t bytes = 0;
      for (const auto& [f, v] : hash_fields_) bytes += f.size() + v.size();
      for (const auto& f : hash_removed_fields_) bytes += f.size();
      return bytes;
    }
  }
  return 0;
}

std::optional<std::string> CompactedState::HashFieldValue(std::string_view field) const {
  if (type_ != DataType::kHash || is_tombstone_) return std::nullopt;
  auto it = hash_fields_.find(std::string(field));
  if (it == hash_fields_.end()) return std::nullopt;
  return it->second;
}

std::optional<double> CompactedState::ZsetMemberScore(std::string_view member) const {
  if (type_ != DataType::kZset || is_tombstone_) return std::nullopt;
  auto it = zset_members_.find(std::string(member));
  if (it == zset_members_.end()) return std::nullopt;
  return it->second;
}

void CompactedState::Reset() {
  type_ = DataType::kNone;
  string_value_.reset();
  abs_ttl_ms_ = 0;
  hash_fields_.clear();
  hash_removed_fields_.clear();
  set_members_.clear();
  set_removed_members_.clear();
  zset_members_.clear();
  zset_removed_members_.clear();
  is_tombstone_ = false;
  base_invalidation_ = BaseInvalidation::kNone;
  ttl_intent_ = TtlIntent::kUnchanged;
}

}  // namespace abyss::consumer
