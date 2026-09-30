#pragma once

#include <cstddef>
#include <cstdint>

namespace abyss::hot {

// Pure memory-budget policy for a single shard. Centralises the one place
// used_bytes_ is compared to a ceiling so the eviction/admission decision
// cannot drift across call sites (the apply path and the eviction worker both
// route through it). A max of 0 means unlimited (the default config and tests
// that never exercise a ceiling).
class MemoryGovernor {
 public:
  explicit MemoryGovernor(size_t max_bytes) : max_bytes_(max_bytes) {}

  bool Enabled() const noexcept { return max_bytes_ != 0; }

  // True if applying `incoming_bytes` on top of `used_bytes` would exceed the
  // budget. Always false when unlimited.
  bool WouldExceed(uint64_t used_bytes, size_t incoming_bytes) const noexcept {
    if (!Enabled()) return false;
    return used_bytes + incoming_bytes > max_bytes_;
  }

  // The used_bytes target the store should evict down to so that
  // `incoming_bytes` fits under the budget. Clamped at 0 when the incoming
  // entry alone is larger than the whole budget (no room can be made for it).
  size_t Target(size_t incoming_bytes) const noexcept {
    if (!Enabled()) return 0;
    return incoming_bytes >= max_bytes_ ? 0 : max_bytes_ - incoming_bytes;
  }

  size_t max_bytes() const noexcept { return max_bytes_; }

 private:
  size_t max_bytes_;
};

}  // namespace abyss::hot
