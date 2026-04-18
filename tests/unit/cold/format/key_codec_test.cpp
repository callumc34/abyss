#include "abyss/cold/format/key_codec.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace abyss::cold::format {
namespace {

// --- Varint round-trip ------------------------------------------------------

TEST(VarintTest, RoundTripBoundaries) {
  const std::vector<uint64_t> cases = {
      0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, 1ULL << 32, std::numeric_limits<uint64_t>::max(),
  };
  for (auto v : cases) {
    std::string buf;
    AppendVarint(buf, v);
    std::string_view view = buf;
    auto decoded = DecodeVarint(view);
    ASSERT_TRUE(decoded.has_value()) << "value " << v;
    EXPECT_EQ(*decoded, v);
    EXPECT_TRUE(view.empty()) << "decoder should consume exactly one varint for value " << v;
  }
}

TEST(VarintTest, SmallValuesAreSingleByte) {
  for (uint64_t v = 0; v <= 0x7F; ++v) {
    std::string buf;
    AppendVarint(buf, v);
    EXPECT_EQ(buf.size(), 1u) << v;
  }
}

TEST(VarintTest, RejectsTruncated) {
  // A varint with the continuation bit set on its only byte is incomplete.
  std::string buf{'\x80'};
  std::string_view view = buf;
  auto result = DecodeVarint(view);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kCorruption);
}

TEST(VarintTest, RejectsOverflow) {
  // 11 bytes of continuation = overflows uint64.
  std::string buf(11, '\xFF');
  std::string_view view = buf;
  auto result = DecodeVarint(view);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kCorruption);
}

TEST(VarintTest, RejectsTenthByteOverflow) {
  // 9 continuation bytes (0xFF) + final byte with bit 1 set (0x02) at shift=63.
  std::string buf;
  for (int i = 0; i < 9; ++i) buf.push_back(static_cast<char>(0xFF));
  buf.push_back(static_cast<char>(0x02));
  std::string_view view = buf;
  auto result = DecodeVarint(view);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kCorruption);
}

TEST(VarintTest, AcceptsMaxUint64TenthByte) {
  // UINT64_MAX encoded as LEB128: 9 bytes of 0xFF + final byte 0x01.
  std::string buf;
  for (int i = 0; i < 9; ++i) buf.push_back(static_cast<char>(0xFF));
  buf.push_back(static_cast<char>(0x01));
  std::string_view view = buf;
  auto result = DecodeVarint(view);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(*result, std::numeric_limits<uint64_t>::max());
}

// --- Sortable double --------------------------------------------------------

TEST(SortableDoubleTest, PreservesOrderingAcrossSign) {
  const std::vector<double> sorted = {
      -std::numeric_limits<double>::infinity(),
      -1e100,
      -1.0,
      -std::numeric_limits<double>::min(),
      0.0,
      std::numeric_limits<double>::min(),
      1.0,
      1e100,
      std::numeric_limits<double>::infinity(),
  };
  for (size_t i = 1; i < sorted.size(); ++i) {
    EXPECT_LT(SortableDouble(sorted[i - 1]), SortableDouble(sorted[i]))
        << "pair " << sorted[i - 1] << " vs " << sorted[i];
  }
}

TEST(SortableDoubleTest, EncodedBytesAreLexComparable) {
  // Big-endian encoding of SortableDouble must reproduce numeric order under
  // lexicographic byte compare.
  auto as_be_string = [](uint64_t v) {
    std::string s(8, '\0');
    for (int i = 0; i < 8; ++i) {
      s[i] = static_cast<char>((v >> (56 - i * 8)) & 0xFF);
    }
    return s;
  };
  const auto a = as_be_string(SortableDouble(-1.0));
  const auto b = as_be_string(SortableDouble(0.0));
  const auto c = as_be_string(SortableDouble(1.0));
  EXPECT_LT(a, b);
  EXPECT_LT(b, c);
}

// --- Key encoders -----------------------------------------------------------

// A sample of binary-unsafe bytes that may appear in user keys.
const std::string kBinaryUnsafeKey{'u', '\x00', 's', 'e', '\xFF', 'r'};

TEST(KeyEncoderTest, StringKeyIsTypePlusKey) {
  const auto encoded = EncodeStringKey("foo");
  ASSERT_EQ(encoded.size(), 4u);
  EXPECT_EQ(static_cast<uint8_t>(encoded[0]), kTypeString);
  EXPECT_EQ(encoded.substr(1), "foo");
}

TEST(KeyEncoderTest, StringKeyHandlesEmbeddedNullBytes) {
  const auto encoded = EncodeStringKey(kBinaryUnsafeKey);
  EXPECT_EQ(encoded.size(), 1u + kBinaryUnsafeKey.size());
  EXPECT_EQ(static_cast<uint8_t>(encoded[0]), kTypeString);
  EXPECT_EQ(encoded.substr(1), kBinaryUnsafeKey);
}

TEST(KeyEncoderTest, MetaKeyIncludesInnerType) {
  const auto encoded = EncodeMetaKey(kTypeHashField, "h");
  ASSERT_EQ(encoded.size(), 3u);
  EXPECT_EQ(static_cast<uint8_t>(encoded[0]), kTypeMeta);
  EXPECT_EQ(static_cast<uint8_t>(encoded[1]), kTypeHashField);
  EXPECT_EQ(encoded.substr(2), "h");
}

TEST(KeyEncoderTest, HashFieldKeyIsLengthPrefixed) {
  const auto encoded = EncodeHashFieldKey("h:1", "name");
  // 0x03 | varint(3) | "h:1" | "name"
  ASSERT_EQ(encoded.size(), 1u + 1u + 3u + 4u);
  EXPECT_EQ(static_cast<uint8_t>(encoded[0]), kTypeHashField);
  EXPECT_EQ(static_cast<uint8_t>(encoded[1]), 3u);
  EXPECT_EQ(encoded.substr(2, 3), "h:1");
  EXPECT_EQ(encoded.substr(5), "name");
}

TEST(KeyEncoderTest, PrefixScanIsolatesOneKeyFromAnother) {
  // "foo" and "fooX" would collide under naive concatenation; the length prefix
  // must prevent a prefix scan of "foo" from matching "fooX" entries.
  const auto prefix_foo = HashFieldPrefix("foo");
  const auto entry_foox = EncodeHashFieldKey("fooX", "field");
  EXPECT_FALSE(std::string_view{entry_foox}.starts_with(prefix_foo));
}

TEST(KeyEncoderTest, PrefixScanIsolatesBinaryUnsafeKeys) {
  // Keys containing '\x00' must still be disambiguated by length prefix.
  const std::string k1 = std::string{'a', '\x00', 'b'};       // "a\0b" length 3
  const std::string k2 = std::string{'a', '\x00', 'b', 'c'};  // "a\0bc" length 4
  const auto prefix_k1 = HashFieldPrefix(k1);
  const auto entry_k2 = EncodeHashFieldKey(k2, "f");
  EXPECT_FALSE(std::string_view{entry_k2}.starts_with(prefix_k1));
}

TEST(KeyEncoderTest, PrefixMatchesOwnHashFieldEntries) {
  const auto prefix = HashFieldPrefix("h");
  const auto entry = EncodeHashFieldKey("h", "f");
  EXPECT_TRUE(std::string_view{entry}.starts_with(prefix));
}

TEST(KeyEncoderTest, ZsetScoreIndexOrdersByScoreThenMember) {
  const auto lower_score = EncodeZsetScoreIndexKey("z", 1.0, "bbb");
  const auto higher_score_earlier_member = EncodeZsetScoreIndexKey("z", 2.0, "aaa");
  EXPECT_LT(lower_score, higher_score_earlier_member);

  const auto same_score_earlier_member = EncodeZsetScoreIndexKey("z", 1.0, "aaa");
  const auto same_score_later_member = EncodeZsetScoreIndexKey("z", 1.0, "bbb");
  EXPECT_LT(same_score_earlier_member, same_score_later_member);
}

TEST(KeyEncoderTest, FormatVersionKeyIsByteExact) {
  const auto encoded = EncodeFormatVersionKey();
  const std::string expected{
      static_cast<char>(kTypeFormatVersion), '\x00', 'f', 'o', 'r', 'm', 'a', 't'};
  EXPECT_EQ(encoded, expected);
}

// --- Value codecs -----------------------------------------------------------

TEST(StringValueCodecTest, RoundTripWithTtl) {
  const auto encoded = EncodeStringValue({
      .flags = kFlagHasTtl,
      .abs_ttl_ms = 1'700'000'000'000,
      .payload = "hello",
  });
  const auto decoded = DecodeStringValue(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->flags, kFlagHasTtl);
  EXPECT_EQ(decoded->abs_ttl_ms, 1'700'000'000'000u);
  EXPECT_EQ(decoded->payload, "hello");
}

TEST(StringValueCodecTest, RoundTripNoTtlEmptyPayload) {
  const auto encoded = EncodeStringValue({});
  const auto decoded = DecodeStringValue(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->flags, 0u);
  EXPECT_EQ(decoded->abs_ttl_ms, 0u);
  EXPECT_TRUE(decoded->payload.empty());
}

TEST(StringValueCodecTest, RejectsShortValue) {
  const std::string too_short(8, '\0');  // minimum is 9 bytes
  const auto decoded = DecodeStringValue(too_short);
  EXPECT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(StringValueCodecTest, PayloadViewIsBackedByInput) {
  const std::string encoded = EncodeStringValue({.payload = "abc"});
  const auto decoded = DecodeStringValue(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->payload.data(), encoded.data() + 9);
}

TEST(MetaValueCodecTest, RoundTrip) {
  const auto encoded = EncodeMetaValue({
      .flags = kFlagHasTtl,
      .abs_ttl_ms = 99,
      .cardinality = 42,
  });
  const auto decoded = DecodeMetaValue(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->flags, kFlagHasTtl);
  EXPECT_EQ(decoded->abs_ttl_ms, 99u);
  EXPECT_EQ(decoded->cardinality, 42u);
}

TEST(MetaValueCodecTest, RejectsWrongSize) {
  const std::string too_short(16, '\0');
  EXPECT_FALSE(DecodeMetaValue(too_short).has_value());
  const std::string too_long(18, '\0');
  EXPECT_FALSE(DecodeMetaValue(too_long).has_value());
}

TEST(FormatVersionValueCodecTest, RoundTrip) {
  const auto encoded = EncodeFormatVersionValue(kFormatVersion);
  const auto decoded = DecodeFormatVersionValue(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, kFormatVersion);
}

TEST(FormatVersionValueCodecTest, ToleratesTrailingReservedBytes) {
  auto encoded = EncodeFormatVersionValue(kFormatVersion);
  encoded.append(4, '\0');  // reserved bytes per ADP-010
  const auto decoded = DecodeFormatVersionValue(encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, kFormatVersion);
}

TEST(FormatVersionValueCodecTest, RejectsShortValue) {
  const std::string too_short(1, '\0');
  EXPECT_FALSE(DecodeFormatVersionValue(too_short).has_value());
}

// --- IsExpired --------------------------------------------------------------

TEST(IsExpiredTest, FlagClearMeansNoTtl) {
  EXPECT_FALSE(IsExpired(0, 0, 0));
  EXPECT_FALSE(IsExpired(0, 1, 1'000'000));
  // Flag is authoritative: a non-zero timestamp with the flag clear is not a TTL.
  EXPECT_FALSE(IsExpired(0, 1'000'000, 2'000'000));
}

TEST(IsExpiredTest, FlagSetAndExpiredInPast) { EXPECT_TRUE(IsExpired(kFlagHasTtl, 100, 200)); }

TEST(IsExpiredTest, FlagSetAndNotYetExpired) { EXPECT_FALSE(IsExpired(kFlagHasTtl, 500, 200)); }

TEST(IsExpiredTest, ExactlyNowIsExpired) {
  // `abs_ttl_ms <= now_ms` — equality counts as expired.
  EXPECT_TRUE(IsExpired(kFlagHasTtl, 1'000, 1'000));
}

}  // namespace
}  // namespace abyss::cold::format
