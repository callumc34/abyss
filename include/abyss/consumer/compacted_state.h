#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "abyss/core/ops.h"

namespace abyss::consumer {

class CompactedState {
 public:
  enum class DataType : uint8_t { kNone, kString, kHash, kSet, kZset };

  // Records that the window contained a destructive reset (DEL, or a type
  // change that invalidates a prior cold-resident type slice) before any
  // surviving additive state. Emit prepends a Del so cold wipes all prior
  // slices for the key before the re-adds land.
  enum class BaseInvalidation : uint8_t { kNone, kDeleteAll };

  // Disambiguates "no TTL touched this window" from "TTL explicitly cleared",
  // so Emit produces a trailing Persist for kCleared and Expire for kSetTo
  // independent of whether any member-bearing op exists.
  enum class TtlIntent : uint8_t { kUnchanged, kSetTo, kCleared };

  void Absorb(const core::ops::WriteOp& op);
  std::vector<core::ops::WriteOp> Emit() const;
  void Reset();

  size_t EstimatedBytes() const;

  bool IsTombstone() const { return is_tombstone_; }
  DataType Type() const { return type_; }
  BaseInvalidation Invalidation() const { return base_invalidation_; }
  TtlIntent Ttl() const { return ttl_intent_; }

  const std::string& StringValue() const {
    return string_value_.has_value() ? *string_value_ : kEmpty;
  }
  uint64_t StringTtlMs() const { return abs_ttl_ms_; }
  uint64_t AbsTtlMs() const { return abs_ttl_ms_; }

  // nullopt when the buffer cannot answer (wrong type, member never seen).
  // Del-tombstones surface via IsTombstone(), not these.
  std::optional<std::string> HashFieldValue(std::string_view field) const;
  // The delta itself, for the loader's merge.
  const std::unordered_map<std::string, std::string>& HashFields() const { return hash_fields_; }
  const std::unordered_set<std::string>& HashRemovedFields() const { return hash_removed_fields_; }
  const std::unordered_set<std::string>& SetMembers() const { return set_members_; }
  const std::unordered_set<std::string>& SetRemovedMembers() const { return set_removed_members_; }
  std::optional<double> ZsetMemberScore(std::string_view member) const;
  const std::unordered_map<std::string, double>& ZsetMembers() const { return zset_members_; }
  const std::unordered_set<std::string>& ZsetRemovedMembers() const {
    return zset_removed_members_;
  }

 private:
  static const std::string kEmpty;

  DataType type_ = DataType::kNone;

  std::optional<std::string> string_value_;
  uint64_t abs_ttl_ms_ = 0;

  std::unordered_map<std::string, std::string> hash_fields_;
  std::unordered_set<std::string> hash_removed_fields_;

  std::unordered_set<std::string> set_members_;
  std::unordered_set<std::string> set_removed_members_;

  std::unordered_map<std::string, double> zset_members_;
  std::unordered_set<std::string> zset_removed_members_;

  bool is_tombstone_ = false;

  // Window-collapse bookkeeping (over already-absolute, parser-derived TTLs).
  // `abs_ttl_ms_` is the kSetTo payload; neither field introduces a TTL clock.
  BaseInvalidation base_invalidation_ = BaseInvalidation::kNone;
  TtlIntent ttl_intent_ = TtlIntent::kUnchanged;
};

}  // namespace abyss::consumer
