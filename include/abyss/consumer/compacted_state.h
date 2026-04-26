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

  void Absorb(const core::ops::WriteOp& op);
  std::vector<core::ops::WriteOp> Emit() const;
  void Reset();

  size_t EstimatedBytes() const;

  bool IsTombstone() const { return is_tombstone_; }
  DataType Type() const { return type_; }

  const std::string& StringValue() const {
    return string_value_.has_value() ? *string_value_ : kEmpty;
  }
  uint64_t StringTtlMs() const { return abs_ttl_ms_; }
  uint64_t AbsTtlMs() const { return abs_ttl_ms_; }

  // nullopt when the buffer cannot answer (wrong type, member never seen).
  // Del-tombstones surface via IsTombstone(), not these.
  bool HashHasField(std::string_view field) const;
  std::optional<std::string> HashFieldValue(std::string_view field) const;
  bool SetHasMember(std::string_view member) const;
  size_t SetCardinality() const { return set_members_.size(); }
  std::optional<double> ZsetMemberScore(std::string_view member) const;
  size_t ZsetCardinality() const { return zset_members_.size(); }

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
};

}  // namespace abyss::consumer
