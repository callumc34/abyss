#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "abyss/core/resp_types.h"

namespace abyss::consumer {

class CompactedState {
 public:
  void Absorb(const core::RespCommand& cmd);
  std::vector<core::RespCommand> Emit() const;
  void Reset();

  bool IsTombstone() const { return is_tombstone_; }

 private:
  std::optional<core::RespCommand> latest_set_;
  std::unordered_map<std::string, std::string> pending_adds_;
  std::unordered_set<std::string> pending_removes_;
  bool is_tombstone_ = false;
};

}  // namespace abyss::consumer
