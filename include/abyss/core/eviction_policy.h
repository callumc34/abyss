#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/types.h"

namespace abyss::core {

struct EvictionRule {
  std::string prefix;
  EvictionTTL eviction;
};

class EvictionPolicy {
 public:
  EvictionPolicy() = default;
  explicit EvictionPolicy(EvictionTTL default_eviction);
  EvictionPolicy(EvictionTTL default_eviction, std::vector<EvictionRule> overrides);

  EvictionTTL Resolve(std::string_view key) const noexcept;

  EvictionTTL DefaultEviction() const noexcept { return default_; }
  const std::vector<EvictionRule>& Overrides() const noexcept { return overrides_; }

 private:
  EvictionTTL default_{86400};
  std::vector<EvictionRule> overrides_;
};

}  // namespace abyss::core
