#include "abyss/engine/doorkeeper.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

namespace abyss::engine {

namespace {

// Ten bits per key and four probes: about 1.2% false positives.
constexpr size_t kBitsPerKey = 10;
constexpr size_t kProbes = 4;

uint64_t Mix(uint64_t x) {
  // splitmix64's finaliser.
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

}  // namespace

Doorkeeper::Doorkeeper(size_t window)
    : bits_(std::bit_ceil(std::max<size_t>(window * kBitsPerKey, 64)) / 64),
      bit_mask_((bits_.size() * 64) - 1),
      window_(std::max<size_t>(window, 1)) {}

bool Doorkeeper::Admit(std::string_view key) {
  const uint64_t h1 = Mix(std::hash<std::string_view>{}(key));
  const uint64_t h2 = Mix(h1) | 1;
  std::array<uint64_t, kProbes> bits{};
  bool seen = true;
  for (size_t i = 0; i < kProbes; ++i) {
    bits.at(i) = (h1 + (i * h2)) & bit_mask_;
    const uint64_t word = bits_[bits.at(i) / 64].load(std::memory_order_relaxed);
    seen = seen && (word & (uint64_t{1} << (bits.at(i) % 64))) != 0;
  }
  if (seen) return true;
  for (const uint64_t bit : bits) {
    bits_[bit / 64].fetch_or(uint64_t{1} << (bit % 64), std::memory_order_relaxed);
  }
  if (recorded_.fetch_add(1, std::memory_order_relaxed) + 1 >= window_) {
    recorded_.store(0, std::memory_order_relaxed);
    for (auto& word : bits_) word.store(0, std::memory_order_relaxed);
  }
  return false;
}

}  // namespace abyss::engine
