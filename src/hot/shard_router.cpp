#include "abyss/hot/shard_router.h"

#include <xxhash.h>

namespace abyss::hot {

core::ShardId ComputeShard(std::string_view key, uint32_t shard_count) noexcept {
  auto hash = XXH3_64bits(key.data(), key.size());
  return static_cast<core::ShardId>(hash % shard_count);
}

}  // namespace abyss::hot
