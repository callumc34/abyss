#include <gtest/gtest.h>

#include <string>

#include "abyss/log/log.h"

namespace abyss::log {
namespace {

TEST(KeyHash, DeterministicForSameInput) { EXPECT_EQ(KeyHash("some-key"), KeyHash("some-key")); }

TEST(KeyHash, DiffersForDifferentInput) {
  EXPECT_NE(KeyHash("session:1234"), KeyHash("session:1235"));
}

TEST(KeyHash, HandlesEmptyInput) {
  const uint64_t h = KeyHash("");
  EXPECT_EQ(h, KeyHash(""));
}

TEST(KeyHash, HandlesBinaryInput) {
  const std::string bin("\x00\x01\x02\x03", 4);
  EXPECT_EQ(KeyHash(bin), KeyHash(bin));
  EXPECT_NE(KeyHash(bin), KeyHash("\x00\x01\x02\x04"));
}

}  // namespace
}  // namespace abyss::log
