#include "abyss/core/slot.h"

#include <gtest/gtest.h>

#include <cstdint>

namespace abyss::core {
namespace {

TEST(Crc16Test, EmptyInputIsZero) { EXPECT_EQ(Crc16(""), 0); }

TEST(Crc16Test, GoldenVectors) {
  // "123456789" is the CCITT canonical test vector; 0x31C3 matches the
  // Redis reference implementation.
  EXPECT_EQ(Crc16("123456789"), 0x31C3);
}

TEST(HashtagTest, NoBraceReturnsKey) { EXPECT_EQ(HashtagContent("foo"), "foo"); }

TEST(HashtagTest, SimpleTag) { EXPECT_EQ(HashtagContent("{user1000}.name"), "user1000"); }

TEST(HashtagTest, TagInMiddle) { EXPECT_EQ(HashtagContent("prefix{abc}suffix"), "abc"); }

TEST(HashtagTest, EmptyTagFallsBackToFullKey) { EXPECT_EQ(HashtagContent("{}foo"), "{}foo"); }

TEST(HashtagTest, UnclosedTagFallsBackToFullKey) {
  EXPECT_EQ(HashtagContent("{noclose"), "{noclose");
}

TEST(HashtagTest, FirstTagWinsEvenWithNestedBraces) {
  // Redis reference: scan for first '{' then first '}', inclusive of any
  // intermediate braces. "{{nested}}" → tag is "{nested".
  EXPECT_EQ(HashtagContent("{{nested}}"), "{nested");
}

TEST(HashtagTest, BraceAfterCloseIgnored) { EXPECT_EQ(HashtagContent("{a}{b}"), "a"); }

TEST(KeySlotTest, KnownRedisSlots) {
  // Slots verified against redis-cli CLUSTER KEYSLOT on Redis 7.x.
  EXPECT_EQ(KeySlot("foo"), 12182);
  EXPECT_EQ(KeySlot("bar"), 5061);
}

TEST(KeySlotTest, HashtagCollocatesKeys) {
  EXPECT_EQ(KeySlot("{foo}bar"), KeySlot("foo"));
  EXPECT_EQ(KeySlot("baz{foo}qux"), KeySlot("foo"));
  EXPECT_EQ(KeySlot("{user1000}.following"), KeySlot("user1000"));
  EXPECT_EQ(KeySlot("{user1000}.following"), KeySlot("{user1000}.followers"));
}

TEST(KeySlotTest, EmptyTagHashesFullKey) {
  EXPECT_NE(KeySlot("{}foo"), KeySlot("foo"));
  EXPECT_EQ(KeySlot("{}foo"), static_cast<uint16_t>(Crc16("{}foo") % kSlotCount));
}

TEST(KeySlotTest, SlotStaysInRange) {
  for (const char* k : {"", "a", "foo", "{a}b", "{}x", "a{b}c", "verylongkeystring"}) {
    EXPECT_LT(KeySlot(k), kSlotCount);
  }
}

}  // namespace
}  // namespace abyss::core
