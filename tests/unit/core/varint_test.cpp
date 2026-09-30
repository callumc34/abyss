#include "abyss/core/varint.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace abyss::core::encoding {
namespace {

TEST(CoreVarintTest, RoundTripBoundaries) {
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

TEST(CoreVarintTest, RejectsTruncated) {
  std::string buf{'\x80'};
  std::string_view view = buf;
  auto result = DecodeVarint(view);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kCorruption);
}

// The load-bearing property: length-prefixing the first field makes a
// concatenation of two binary-safe blobs self-delimiting even when the data
// contains the byte (0x1F) that the old separator scheme used.
TEST(CoreVarintTest, LengthPrefixIsSelfDelimitingAcrossSeparatorByte) {
  auto encode = [](std::string_view a, std::string_view b) {
    std::string out;
    AppendVarint(out, a.size());
    out.append(a);
    out.append(b);
    return out;
  };
  // ("a", "b\x1Fc") vs ("a\x1Fb", "c") collided under the old single-0x1F join.
  const std::string b_sep_c = std::string("b\x1F") + "c";
  const std::string a_sep_b = std::string("a\x1F") + "b";
  EXPECT_NE(encode("a", b_sep_c), encode(a_sep_b, "c"));
  // And decoding recovers the exact first field length.
  const auto buf = encode(a_sep_b, "c");
  std::string_view view = buf;
  auto len = DecodeVarint(view);
  ASSERT_TRUE(len.has_value());
  EXPECT_EQ(*len, 3U);
  EXPECT_EQ(view.substr(0, *len), a_sep_b);
  EXPECT_EQ(view.substr(*len), "c");
}

}  // namespace
}  // namespace abyss::core::encoding
