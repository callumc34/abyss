#include "abyss/core/eviction_policy.h"

#include <algorithm>
#include <utility>

namespace abyss::core {

EvictionPolicy::EvictionPolicy(EvictionTTL default_eviction) : default_(default_eviction) {}

EvictionPolicy::EvictionPolicy(EvictionTTL default_eviction, std::vector<EvictionRule> overrides)
    : default_(default_eviction), overrides_(std::move(overrides)) {
  std::stable_sort(overrides_.begin(), overrides_.end(),
                   [](const EvictionRule& a, const EvictionRule& b) {
                     return a.prefix.size() > b.prefix.size();
                   });
}

EvictionTTL EvictionPolicy::Resolve(std::string_view key) const noexcept {
  for (const auto& rule : overrides_) {
    if (key.size() >= rule.prefix.size() && key.compare(0, rule.prefix.size(), rule.prefix) == 0) {
      return rule.eviction;
    }
  }
  return default_;
}

}  // namespace abyss::core
