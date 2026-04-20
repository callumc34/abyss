#pragma once

#include <cstdint>

namespace abyss::core {

// Byte to char pointer conversion helpers.

// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
inline const char* AsChars(const uint8_t* p) noexcept { return reinterpret_cast<const char*>(p); }

inline const uint8_t* AsBytes(const char* p) noexcept {
  return reinterpret_cast<const uint8_t*>(p);
}

// NOLINTNEXTLINE(readability-non-const-parameter)
inline char* AsChars(uint8_t* p) noexcept { return reinterpret_cast<char*>(p); }

// NOLINTNEXTLINE(readability-non-const-parameter)
inline uint8_t* AsBytes(char* p) noexcept { return reinterpret_cast<uint8_t*>(p); }
// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)

}  // namespace abyss::core
