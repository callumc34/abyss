#include "abyss/core/slot_shard_map.h"

#include "abyss/core/slot.h"

namespace abyss::core {

uint16_t SlotForKey(std::string_view key) noexcept { return KeySlot(key); }

ShardId ShardForSlot(uint16_t slot, uint32_t shard_count) noexcept {
  // Contiguous range division: each shard owns a contiguous slot range, so a
  // reshard moves shard ownership without re-encoding any key (ADP-014). The
  // 64-bit product cannot overflow (slot < 16384, shard_count <= 2^16).
  const auto wide = static_cast<uint64_t>(slot) * shard_count / kSlotCount;
  return static_cast<ShardId>(wide);
}

ShardId ShardForKey(std::string_view key, uint32_t shard_count) noexcept {
  return ShardForSlot(SlotForKey(key), shard_count);
}

}  // namespace abyss::core
