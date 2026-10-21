#pragma once

#include <ostream>

#include "abyss/core/durability.h"

namespace abyss::core {

// Lets gtest name parameterized tests by class instead of raw bytes.
inline void PrintTo(Durability durability, std::ostream* os) { *os << DurabilityName(durability); }

}  // namespace abyss::core
