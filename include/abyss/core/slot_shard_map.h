#pragma once

#include <cstdint>
#include <string_view>

#include "abyss/core/types.h"

namespace abyss::core {

// The single routing path (ADP-014). The keyspace is partitioned into
// `kSlotCount` (16384) Redis-compatible slots; a data shard owns a contiguous
// range of slots. Placement everywhere — hot striping, the cold key prefix, WAL
// shard directories, resolver stripes, consumer routing — derives the shard
// from the slot via these pure, total, deterministic functions. xxHash is no
// longer in the placement path.

// CRC16(HashtagContent(key)) % kSlotCount — the wire slot a cluster-aware
// client routes by, and the only routing hash. Equals core::KeySlot.
uint16_t SlotForKey(std::string_view key) noexcept;

// Maps a wire slot to its owning data shard by contiguous range division:
// slot * shard_count / kSlotCount. Total over [0, kSlotCount) and onto
// [0, shard_count). `shard_count` must be >= 1.
ShardId ShardForSlot(uint16_t slot, uint32_t shard_count) noexcept;

// The placement authority: ShardForSlot(SlotForKey(key), shard_count). Keys
// sharing a {hashtag} produce the same slot and therefore the same shard.
ShardId ShardForKey(std::string_view key, uint32_t shard_count) noexcept;

}  // namespace abyss::core
