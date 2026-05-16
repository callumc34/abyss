#pragma once

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/result.h"
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

  // Longest configured TTL across `default_eviction` and `overrides`. Drives
  // the queue.min_retention coupling enforced by the config validator.
  static EvictionTTL MaxConfiguredTtl(EvictionTTL default_eviction,
                                      std::span<const EvictionRule> overrides) noexcept;

  // Structural validation for EvictionPolicy inputs. Rejects:
  //   - non-positive default_eviction
  //   - empty prefix
  //   - non-positive override eviction
  //   - duplicate prefix
  // The path_prefix is prepended to error paths so the validator can surface
  // (for example) "hot.eviction_overrides[1].prefix" without re-implementing
  // the rules outside this header.
  static Result<void> Validate(EvictionTTL default_eviction,
                               std::span<const EvictionRule> overrides,
                               std::string_view path_prefix);

 private:
  EvictionTTL default_{86400};
  std::vector<EvictionRule> overrides_;
};

}  // namespace abyss::core
