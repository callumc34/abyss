#include "abyss/core/shard_router.h"

#include <xxhash.h>

namespace abyss::core {

ShardId ComputeShard(std::string_view key, uint32_t shard_count) noexcept {
  auto hash = XXH3_64bits(key.data(), key.size());
  return static_cast<ShardId>(hash % shard_count);
}

}  // namespace abyss::core
