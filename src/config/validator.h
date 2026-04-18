#pragma once

#include "abyss/config/config.h"
#include "abyss/core/result.h"

namespace abyss::config::internal {

// Pure validation over a fully-decoded Config. Returns the first error.
core::Result<void> Validate(const Config& config);

}  // namespace abyss::config::internal
