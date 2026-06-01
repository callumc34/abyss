#include "abyss/cold/format/key_codec.h"

#include <bit>
#include <cstring>

#include "abyss/core/shard_router.h"

namespace abyss::cold::format {
namespace {

using core::Error;
using core::ErrorCode;

constexpr uint64_t kSignBit = 0x8000000000000000ULL;

void AppendU8(std::string& out, uint8_t v) { out.push_back(static_cast<char>(v)); }

void AppendU16BE(std::string& out, uint16_t v) {
  out.push_back(static_cast<char>((v >> 8) & 0xFF));
  out.push_back(static_cast<char>(v & 0xFF));
}

// Derives the slot from the key (not the caller) so encode/read/wipe agree.
void AppendShard(std::string& out, std::string_view key, uint32_t shard_count) {
  const core::ShardId shard = core::ComputeShard(key, shard_count);
  AppendU16BE(out, static_cast<uint16_t>(shard));
}

void AppendU64BE(std::string& out, uint64_t v) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out.push_back(static_cast<char>((v >> shift) & 0xFF));
  }
}

uint16_t ReadU16BE(const char* p) {
  const auto hi = static_cast<uint16_t>(static_cast<uint8_t>(p[0]));
  const auto lo = static_cast<uint16_t>(static_cast<uint8_t>(p[1]));
  return static_cast<uint16_t>((hi << 8) | lo);
}

uint64_t ReadU64BE(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  }
  return v;
}

void AppendLengthPrefixedKey(std::string& out, std::string_view key) {
  AppendVarint(out, key.size());
  out.append(key);
}

}  // namespace

// --- Sortable double --------------------------------------------------------

uint64_t SortableDouble(double d) {
  const auto bits = std::bit_cast<uint64_t>(d);
  return (bits & kSignBit) != 0 ? ~bits : (bits | kSignBit);
}

// --- Key encoders -----------------------------------------------------------

std::string EncodeStringKey(std::string_view key, uint32_t shard_count) {
  std::string out;
  out.reserve(1 + kShardBytes + key.size());
  AppendU8(out, kTypeString);
  AppendShard(out, key, shard_count);
  out.append(key);
  return out;
}

std::string EncodeMetaKey(uint8_t inner_type, std::string_view key, uint32_t shard_count) {
  std::string out;
  out.reserve(2 + kShardBytes + key.size());
  AppendU8(out, kTypeMeta);
  AppendShard(out, key, shard_count);
  AppendU8(out, inner_type);
  out.append(key);
  return out;
}

std::string EncodeHashFieldKey(std::string_view key, std::string_view field, uint32_t shard_count) {
  std::string out;
  out.reserve(1 + kShardBytes + 10 + key.size() + field.size());
  AppendU8(out, kTypeHashField);
  AppendShard(out, key, shard_count);
  AppendLengthPrefixedKey(out, key);
  out.append(field);
  return out;
}

std::string EncodeSetMemberKey(std::string_view key, std::string_view member,
                               uint32_t shard_count) {
  std::string out;
  out.reserve(1 + kShardBytes + 10 + key.size() + member.size());
  AppendU8(out, kTypeSetMember);
  AppendShard(out, key, shard_count);
  AppendLengthPrefixedKey(out, key);
  out.append(member);
  return out;
}

std::string EncodeZsetMemberKey(std::string_view key, std::string_view member,
                                uint32_t shard_count) {
  std::string out;
  out.reserve(1 + kShardBytes + 10 + key.size() + member.size());
  AppendU8(out, kTypeZsetMember);
  AppendShard(out, key, shard_count);
  AppendLengthPrefixedKey(out, key);
  out.append(member);
  return out;
}

std::string EncodeZsetScoreIndexKey(std::string_view key, double score, std::string_view member,
                                    uint32_t shard_count) {
  std::string out;
  out.reserve(1 + kShardBytes + 10 + key.size() + 8 + member.size());
  AppendU8(out, kTypeZsetScoreIndex);
  AppendShard(out, key, shard_count);
  AppendLengthPrefixedKey(out, key);
  AppendU64BE(out, SortableDouble(score));
  out.append(member);
  return out;
}

std::string EncodeFormatVersionKey() {
  std::string out;
  out.reserve(8);
  AppendU8(out, kTypeFormatVersion);
  AppendU8(out, 0x00);
  out.append("format", 6);
  return out;
}

// --- Prefix-scan helpers ----------------------------------------------------

namespace {

std::string TypedLengthPrefix(uint8_t type, std::string_view key, uint32_t shard_count) {
  std::string out;
  out.reserve(1 + kShardBytes + 10 + key.size());
  AppendU8(out, type);
  AppendShard(out, key, shard_count);
  AppendLengthPrefixedKey(out, key);
  return out;
}

}  // namespace

std::string HashFieldPrefix(std::string_view key, uint32_t shard_count) {
  return TypedLengthPrefix(kTypeHashField, key, shard_count);
}

std::string SetMemberPrefix(std::string_view key, uint32_t shard_count) {
  return TypedLengthPrefix(kTypeSetMember, key, shard_count);
}

std::string ZsetMemberPrefix(std::string_view key, uint32_t shard_count) {
  return TypedLengthPrefix(kTypeZsetMember, key, shard_count);
}

std::string ZsetScoreIndexPrefix(std::string_view key, uint32_t shard_count) {
  return TypedLengthPrefix(kTypeZsetScoreIndex, key, shard_count);
}

std::string ShardTypePrefix(uint8_t type, core::ShardId shard) {
  std::string out;
  out.reserve(1 + kShardBytes);
  AppendU8(out, type);
  AppendU16BE(out, static_cast<uint16_t>(shard));
  return out;
}

// --- String value codec -----------------------------------------------------

std::string EncodeStringValue(const StringValue& v) {
  std::string out;
  out.reserve(9 + v.payload.size());
  AppendU8(out, v.flags);
  AppendU64BE(out, v.abs_ttl_ms);
  out.append(v.payload);
  return out;
}

core::Result<StringValue> DecodeStringValue(std::string_view value) {
  if (value.size() < 9) {
    return std::unexpected(Error(ErrorCode::kCorruption, "string value shorter than 9 bytes"));
  }
  StringValue out;
  out.flags = static_cast<uint8_t>(value[0]);
  out.abs_ttl_ms = ReadU64BE(value.data() + 1);
  out.payload = value.substr(9);
  return out;
}

// --- Meta value codec -------------------------------------------------------

std::string EncodeMetaValue(const MetaValue& v) {
  std::string out;
  out.reserve(17);
  AppendU8(out, v.flags);
  AppendU64BE(out, v.abs_ttl_ms);
  AppendU64BE(out, v.cardinality);
  return out;
}

core::Result<MetaValue> DecodeMetaValue(std::string_view value) {
  if (value.size() != 17) {
    return std::unexpected(Error(ErrorCode::kCorruption, "meta value must be exactly 17 bytes"));
  }
  MetaValue out;
  out.flags = static_cast<uint8_t>(value[0]);
  out.abs_ttl_ms = ReadU64BE(value.data() + 1);
  out.cardinality = ReadU64BE(value.data() + 9);
  return out;
}

bool IsExpired(uint8_t flags, uint64_t abs_ttl_ms, uint64_t now_ms) {
  return (flags & kFlagHasTtl) != 0 && abs_ttl_ms <= now_ms;
}

// --- Format version record codec --------------------------------------------

std::string EncodeFormatVersionValue(uint16_t version) {
  std::string out;
  out.reserve(2);
  AppendU16BE(out, version);
  return out;
}

core::Result<uint16_t> DecodeFormatVersionValue(std::string_view value) {
  if (value.size() < 2) {
    return std::unexpected(
        Error(ErrorCode::kCorruption, "format version value shorter than 2 bytes"));
  }
  return ReadU16BE(
      value.data());  // NOLINT(bugprone-suspicious-stringview-data-usage) size validated
}

}  // namespace abyss::cold::format
