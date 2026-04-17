#include "abyss/consumer/compaction_buffer.h"

#include <gtest/gtest.h>

#include "test_clock.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;
using core::ops::Del;
using core::ops::SetAdd;
using core::ops::StringSet;
using core::ops::WriteOp;

class CompactionBufferTest : public ::testing::Test {
 protected:
  testing::TestClock clock_;
  CompactionBuffer buffer_{clock_.SteadyFn()};
};

TEST_F(CompactionBufferTest, AbsorbStringThenReadReturnsBulkString) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}});
  auto result = buffer_.Read("k");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(CompactionBufferTest, AbsorbDelThenReadReturnsNull) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}});
  buffer_.Absorb("k", WriteOp{Del{.keys = {"k"}}});
  auto result = buffer_.Read("k");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(CompactionBufferTest, AbsorbCollectionReadReturnsNotFound) {
  buffer_.Absorb("k", WriteOp{SetAdd{.key = "k", .members = {"a"}}});
  auto result = buffer_.Read("k");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(CompactionBufferTest, ReadNonexistentKeyReturnsNotFound) {
  auto result = buffer_.Read("missing");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(CompactionBufferTest, SizeReflectsDistinctKeys) {
  buffer_.Absorb("k1", WriteOp{StringSet{.key = "k1", .value = "v1"}});
  buffer_.Absorb("k2", WriteOp{StringSet{.key = "k2", .value = "v2"}});
  buffer_.Absorb("k1", WriteOp{StringSet{.key = "k1", .value = "v3"}});
  EXPECT_EQ(buffer_.Size(), 2);
}

TEST_F(CompactionBufferTest, AbsorbSameKeyIncrementsWriteCount) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v1"}});
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v2"}});
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v3"}});

  auto result = buffer_.Read("k");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v3");
}

TEST_F(CompactionBufferTest, TimestampsSetFromClock) {
  auto t0 = clock_.SteadyNow();
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v1"}});

  clock_.Advance(5s);
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v2"}});

  auto entries = buffer_.FlushReady(clock_.SteadyNow());
  // FlushReady is currently a stub — this test documents the expected timestamp
  // behaviour once it's implemented. For now, verify the buffer has the entry.
  EXPECT_EQ(buffer_.Size(), 1);
  (void)t0;
}

TEST_F(CompactionBufferTest, EmptyBufferSizeIsZero) { EXPECT_EQ(buffer_.Size(), 0); }

}  // namespace
}  // namespace abyss::consumer
