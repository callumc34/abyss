#pragma once

#include <cstddef>

namespace abyss::testing {

// What this binary's global operator new has counted on this thread.
std::size_t ThreadAllocs();

}  // namespace abyss::testing
