#pragma once

#include <cstddef>

namespace abyss::testing {

// What this binary's global operator new and delete have counted:
// allocations of at least kBigAllocBytes, frees of those, and frees.
inline constexpr std::size_t kBigAllocBytes = std::size_t{512} << 10;

std::size_t Allocs();
std::size_t BigAllocs();
std::size_t BigFrees();
std::size_t Frees();

}  // namespace abyss::testing
