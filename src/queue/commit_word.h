#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>

// A frame's commit word, stored little-endian in its 8-byte-aligned
// first slot. A live mapping's word is only ever touched through these.
namespace abyss::queue::frame {

static_assert(std::atomic_ref<uint64_t>::is_always_lock_free);

inline uint64_t LoadCommitWord(const std::byte* slot,
                               std::memory_order order = std::memory_order_acquire) noexcept {
  // atomic_ref<const T> arrives only in C++26.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  auto& word = *reinterpret_cast<uint64_t*>(const_cast<std::byte*>(slot));
  const uint64_t stored = std::atomic_ref<uint64_t>(word).load(order);
  if constexpr (std::endian::native == std::endian::little) {
    return stored;
  } else {
    return std::byteswap(stored);
  }
}

inline void StoreCommitWord(std::byte* slot, uint64_t word,
                            std::memory_order order = std::memory_order_release) noexcept {
  if constexpr (std::endian::native != std::endian::little) word = std::byteswap(word);
  std::atomic_ref<uint64_t>(*reinterpret_cast<uint64_t*>(slot)).store(word, order);
}

}  // namespace abyss::queue::frame
