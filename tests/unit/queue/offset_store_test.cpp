#include <gtest/gtest.h>

#include "abyss/queue/memory_offset_store.h"

namespace abyss::queue {
namespace {

TEST(MemoryOffsetStoreTest, GetBeforeSetReturnsNullopt) {
  const MemoryOffsetStore store;
  EXPECT_FALSE(store.Get(0, 0).has_value());
}

TEST(MemoryOffsetStoreTest, SetGetRoundTrip) {
  MemoryOffsetStore store;
  ASSERT_TRUE(store.Set(0, 3, 42).has_value());
  EXPECT_EQ(store.Get(0, 3), std::optional<core::SequenceId>{42});
}

TEST(MemoryOffsetStoreTest, ConsumersAndShardsAreIndependent) {
  MemoryOffsetStore store;
  ASSERT_TRUE(store.Set(0, 0, 100).has_value());
  ASSERT_TRUE(store.Set(1, 0, 200).has_value());
  ASSERT_TRUE(store.Set(0, 2, 30).has_value());

  EXPECT_EQ(store.Get(0, 0), std::optional<core::SequenceId>{100});
  EXPECT_EQ(store.Get(1, 0), std::optional<core::SequenceId>{200});
  EXPECT_EQ(store.Get(0, 2), std::optional<core::SequenceId>{30});
  EXPECT_FALSE(store.Get(1, 2).has_value());
}

TEST(MemoryOffsetStoreTest, SetOverwritesPrevious) {
  MemoryOffsetStore store;
  ASSERT_TRUE(store.Set(0, 0, 100).has_value());
  ASSERT_TRUE(store.Set(0, 0, 200).has_value());
  EXPECT_EQ(store.Get(0, 0), std::optional<core::SequenceId>{200});
}

}  // namespace
}  // namespace abyss::queue
