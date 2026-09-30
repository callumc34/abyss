#include "abyss/core/shard_router.h"

#include "abyss/core/slot_shard_map.h"

namespace abyss::core {

// Slot-unified placement (ADP-014): the data shard is derived from the wire
// slot, so wire routing and placement can never diverge. xxHash is gone.
ShardId ComputeShard(std::string_view key, uint32_t shard_count) noexcept {
  return ShardForKey(key, shard_count);
}

}  // namespace abyss::core
