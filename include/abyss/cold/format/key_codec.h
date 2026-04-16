#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "abyss/core/result.h"

namespace abyss::cold::format {

// Type byte allocations.
inline constexpr uint8_t kTypeString = 0x01;
inline constexpr uint8_t kTypeMeta = 0x02;
inline constexpr uint8_t kTypeHashField = 0x03;
inline constexpr uint8_t kTypeSetMember = 0x04;
inline constexpr uint8_t kTypeZsetMember = 0x05;
inline constexpr uint8_t kTypeZsetScoreIndex = 0x06;
inline constexpr uint8_t kTypeFormatVersion = 0xFF;

// Version stored in the format-version record.
inline constexpr uint16_t kFormatVersion = 1;

// Value flag bits.
inline constexpr uint8_t kFlagHasTtl = 0x01;

void AppendVarint(std::string& out, uint64_t value);

// Consumes the varint from the front of `bytes` on success and advances.
core::Result<uint64_t> DecodeVarint(std::string_view& bytes);

uint64_t SortableDouble(double d);

std::string EncodeStringKey(std::string_view key);
std::string EncodeMetaKey(uint8_t inner_type, std::string_view key);
std::string EncodeHashFieldKey(std::string_view key, std::string_view field);
std::string EncodeSetMemberKey(std::string_view key, std::string_view member);
std::string EncodeZsetMemberKey(std::string_view key, std::string_view member);
std::string EncodeZsetScoreIndexKey(std::string_view key, double score, std::string_view member);
std::string EncodeFormatVersionKey();

std::string HashFieldPrefix(std::string_view key);
std::string SetMemberPrefix(std::string_view key);
std::string ZsetMemberPrefix(std::string_view key);
std::string ZsetScoreIndexPrefix(std::string_view key);

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

std::string EncodeFormatVersionValue(uint16_t version);
core::Result<uint16_t> DecodeFormatVersionValue(std::string_view value);

}  // namespace abyss::cold::format
