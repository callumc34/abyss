#pragma once

#include <cstddef>
#include <span>

#include "abyss/core/result.h"

namespace abyss::platform {

// Fills `out` from the OS's cryptographically secure generator.
core::Result<void> RandomBytes(std::span<std::byte> out);

}  // namespace abyss::platform
