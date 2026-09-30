#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/core/varint.h"

namespace abyss::cold::format {

// The varint codec lives in core::encoding (shared with the existence cache).
// Re-exported here so existing cold callers keep using the unqualified names.
using core::encoding::AppendVarint;
using core::encoding::DecodeVarint;

// Type byte allocations.
inline constexpr uint8_t kTypeString = 0x01;
inline constexpr uint8_t kTypeMeta = 0x02;
inline constexpr uint8_t kTypeHashField = 0x03;
inline constexpr uint8_t kTypeSetMember = 0x04;
inline constexpr uint8_t kTypeZsetMember = 0x05;
inline constexpr uint8_t kTypeZsetScoreIndex = 0x06;
inline constexpr uint8_t kTypeFormatVersion = 0xFF;

// Shard-slot width: a fixed 2-byte big-endian tag after the type byte (ADP-010),
// caps shard count at 65536. RocksdbStore::Create enforces the bound.
inline constexpr size_t kShardBytes = 2;
inline constexpr uint32_t kMaxShardCount = 1U << (8 * kShardBytes);

// v2 added the shard slot; v3 (ADP-014) derives that shard from the wire slot
// (CRC16/16384 -> contiguous range) instead of xxHash, so a v2 store's shard
// prefixes are placed under a different scheme and fail open(). The
// TopologyManifest is the primary epoch gate; this per-open check is defence in
// depth. A store with a different version has an incompatible layout.
inline constexpr uint16_t kFormatVersion = 3;

// Value flag bits.
inline constexpr uint8_t kFlagHasTtl = 0x01;

uint64_t SortableDouble(double d);

// Data-key encoders. The slot is derived from `key` (not passed in) so read,
// write, and wipe can't disagree on a key's slice. `shard_count` in [1, kMaxShardCount].
std::string EncodeStringKey(std::string_view key, uint32_t shard_count);
std::string EncodeMetaKey(uint8_t inner_type, std::string_view key, uint32_t shard_count);
std::string EncodeHashFieldKey(std::string_view key, std::string_view field, uint32_t shard_count);
std::string EncodeSetMemberKey(std::string_view key, std::string_view member, uint32_t shard_count);
std::string EncodeZsetMemberKey(std::string_view key, std::string_view member,
                                uint32_t shard_count);
std::string EncodeZsetScoreIndexKey(std::string_view key, double score, std::string_view member,
                                    uint32_t shard_count);
std::string EncodeFormatVersionKey();

std::string HashFieldPrefix(std::string_view key, uint32_t shard_count);
std::string SetMemberPrefix(std::string_view key, uint32_t shard_count);
std::string ZsetMemberPrefix(std::string_view key, uint32_t shard_count);
std::string ZsetScoreIndexPrefix(std::string_view key, uint32_t shard_count);

// <type><shard:2> — the prefix bounding one shard's slice of one type, used to
// build the wipe range [ShardTypePrefix(t, s), successor).
std::string ShardTypePrefix(uint8_t type, core::ShardId shard);

struct StringValue {
  uint8_t flags = 0;
  uint64_t abs_ttl_ms = 0;
  std::string_view payload;
};

std::string EncodeStringValue(const StringValue& v);

// The returned `payload` view points into `value`.
core::Result<StringValue> DecodeStringValue(std::string_view value);

struct MetaValue {
  uint8_t flags = 0;
  uint64_t abs_ttl_ms = 0;
  uint64_t cardinality = 0;
};

std::string EncodeMetaValue(const MetaValue& v);
core::Result<MetaValue> DecodeMetaValue(std::string_view value);

// Returns true if a TTL is set and `abs_ttl_ms <= now_ms`. `kFlagHasTtl` is
// authoritative, a zero timestamp with the flag clear is "no TTL" regardless
// of `abs_ttl_ms`.
bool IsExpired(uint8_t flags, uint64_t abs_ttl_ms, uint64_t now_ms);

std::string EncodeFormatVersionValue(uint16_t version);
core::Result<uint16_t> DecodeFormatVersionValue(std::string_view value);

}  // namespace abyss::cold::format
