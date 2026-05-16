#include "abyss/core/eviction_policy.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace abyss::core {

namespace {

Error InvalidArg(std::string path, std::string_view message) {
  std::string msg = std::move(path);
  msg += ": ";
  msg += message;
  return {ErrorCode::kInvalidArgument, std::move(msg)};
}

}  // namespace

EvictionPolicy::EvictionPolicy(EvictionTTL default_eviction) : default_(default_eviction) {}

EvictionPolicy::EvictionPolicy(EvictionTTL default_eviction, std::vector<EvictionRule> overrides)
    : default_(default_eviction), overrides_(std::move(overrides)) {
  std::ranges::stable_sort(overrides_, [](const EvictionRule& a, const EvictionRule& b) {
    return a.prefix.size() > b.prefix.size();
  });
}

EvictionTTL EvictionPolicy::Resolve(std::string_view key) const noexcept {
  for (const auto& rule : overrides_) {
    if (key.starts_with(rule.prefix)) {
      return rule.eviction;
    }
  }
  return default_;
}

EvictionTTL EvictionPolicy::MaxConfiguredTtl(EvictionTTL default_eviction,
                                             std::span<const EvictionRule> overrides) noexcept {
  EvictionTTL max = default_eviction;
  for (const auto& rule : overrides) {
    if (rule.eviction > max) max = rule.eviction;
  }
  return max;
}

Result<void> EvictionPolicy::Validate(EvictionTTL default_eviction,
                                      std::span<const EvictionRule> overrides,
                                      std::string_view path_prefix) {
  std::string base{path_prefix};
  if (default_eviction.count() <= 0) {
    return std::unexpected(InvalidArg(base + ".default_eviction_seconds", "must be > 0 seconds"));
  }
  std::unordered_set<std::string> seen;
  for (size_t i = 0; i < overrides.size(); ++i) {
    const auto& rule = overrides[i];
    std::string item = base + ".eviction_overrides[";
    item += std::to_string(i);
    item += ']';
    if (rule.prefix.empty()) {
      return std::unexpected(InvalidArg(item + ".prefix", "must not be empty"));
    }
    if (rule.eviction.count() <= 0) {
      return std::unexpected(InvalidArg(item + ".eviction_seconds", "must be > 0 seconds"));
    }
    if (!seen.insert(rule.prefix).second) {
      return std::unexpected(InvalidArg(item + ".prefix", "duplicate prefix"));
    }
  }
  return {};
}

}  // namespace abyss::core
