#pragma once

#include <cstdint>
#include <string_view>

#include "abyss/core/types.h"

namespace abyss::hot {

core::ShardId ComputeShard(std::string_view key, uint32_t shard_count) noexcept;

}  // namespace abyss::hot
