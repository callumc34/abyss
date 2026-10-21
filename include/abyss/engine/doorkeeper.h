#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace abyss::engine {

// TinyLFU's doorkeeper: admits a key on its second access within a
// window, so one-hit wonders never reach the cache. A Bloom filter
// sized for `window` keys at about 1% false positives, cleared once it
// has recorded that many. Lock-free; a race only blurs the window.
class Doorkeeper {
 public:
  explicit Doorkeeper(size_t window);

  // True if `key` was seen since the last reset; records it if not.
  bool Admit(std::string_view key);

 private:
  std::vector<std::atomic<uint64_t>> bits_;
  uint64_t bit_mask_;
  size_t window_;
  std::atomic<size_t> recorded_{0};
};

}  // namespace abyss::engine
