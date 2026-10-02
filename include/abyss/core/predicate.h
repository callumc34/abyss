#pragma once

#include <cstdint>

namespace abyss::core {

enum class PredicateFlags : uint16_t {
  kNone = 0,
  kNx = 1 << 0,
  kXx = 1 << 1,
  kGet = 1 << 2,
  kKeepTtl = 1 << 3,
  kZAddGt = 1 << 4,
  kZAddLt = 1 << 5,
  kZAddCh = 1 << 6,
  kMsetNx = 1 << 7,
  kExpireGt = 1 << 8,
  kExpireLt = 1 << 9,
};

constexpr PredicateFlags operator|(PredicateFlags a, PredicateFlags b) {
  // NOLINTNEXTLINE(readability-redundant-casting,clang-analyzer-optin.core.EnumCastOutOfRange)
  return static_cast<PredicateFlags>(static_cast<uint16_t>(a) | static_cast<uint16_t>(b));
}

constexpr PredicateFlags operator&(PredicateFlags a, PredicateFlags b) {
  // NOLINTNEXTLINE(readability-redundant-casting,clang-analyzer-optin.core.EnumCastOutOfRange)
  return static_cast<PredicateFlags>(static_cast<uint16_t>(a) & static_cast<uint16_t>(b));
}

constexpr PredicateFlags& operator|=(PredicateFlags& a, PredicateFlags b) {
  a = a | b;
  return a;
}

constexpr bool HasFlag(PredicateFlags flags, PredicateFlags flag) {
  return (static_cast<uint16_t>(flags) & static_cast<uint16_t>(flag)) != 0;
}

}  // namespace abyss::core
