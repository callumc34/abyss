#pragma once

#include <cstdint>
#include <optional>
#include <string>
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
  uint64_t StringTtlMs() const { return string_ttl_ms_; }

 private:
  static const std::string kEmpty;

  DataType type_ = DataType::kNone;

  std::optional<std::string> string_value_;
  uint64_t string_ttl_ms_ = 0;

  std::unordered_map<std::string, std::string> hash_fields_;
  std::unordered_set<std::string> hash_removed_fields_;

  std::unordered_set<std::string> set_members_;
  std::unordered_set<std::string> set_removed_members_;

  std::unordered_map<std::string, double> zset_members_;
  std::unordered_set<std::string> zset_removed_members_;

  bool is_tombstone_ = false;
};

}  // namespace abyss::consumer
