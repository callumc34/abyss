#pragma once

#include <cstdint>
#include <string_view>

#include "abyss/core/types.h"

namespace abyss::core {

ShardId ComputeShard(std::string_view key, uint32_t shard_count) noexcept;

}  // namespace abyss::core
