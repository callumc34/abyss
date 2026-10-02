#include "abyss/platform/random.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <vector>

namespace abyss::platform {
namespace {

TEST(RandomBytesTest, FillsEveryByteDifferentlyEachTime) {
  std::array<std::byte, 64> a{};
  std::array<std::byte, 64> b{};
  ASSERT_TRUE(RandomBytes(a).has_value());
  ASSERT_TRUE(RandomBytes(b).has_value());
  EXPECT_NE(a, b);
  EXPECT_FALSE(std::ranges::all_of(a, [](std::byte x) { return x == std::byte{0}; }));
}

TEST(RandomBytesTest, HandlesEmptyAndLargeRequests) {
  EXPECT_TRUE(RandomBytes({}).has_value());
  std::vector<std::byte> large(std::size_t{1} << 20);
  ASSERT_TRUE(RandomBytes(large).has_value());
  // 1 MiB of uniform bytes holds every value.
  std::bitset<256> seen;
  for (const std::byte x : large) seen.set(std::to_integer<std::size_t>(x));
  EXPECT_TRUE(seen.all());
}

}  // namespace
}  // namespace abyss::platform
