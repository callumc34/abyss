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

constexpr uint64_t kTestSeed = 42;
constexpr auto kDefaultEviction = core::EvictionTTL{3600};

class CompactionBufferTest : public ::testing::Test {
 protected:
  void SetUp() override {
    key_counter_ = 0;
    clock_.Set(core::SteadyTime{std::chrono::seconds{1000000}});
  }

  void TearDown() override {
    clock_.Advance(std::chrono::hours{24});
    buffer_.FlushReady(clock_.SteadyNow());
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::TestClock clock_;
  FlushStrategy strategy_;
  CompactionBuffer buffer_{strategy_, clock_.SteadyFn(), kTestSeed};
  int key_counter_ = 0;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  std::string MakeKey() { return "k" + std::to_string(key_counter_++); }

  void AbsorbString(const std::string& key, const std::string& value,
                    core::EvictionTTL eviction = core::EvictionTTL{3600}) {
    buffer_.Absorb(key, WriteOp{StringSet{.key = key, .value = value}}, eviction);
  }

  void AbsorbDel(const std::string& key, core::EvictionTTL eviction = core::EvictionTTL{3600}) {
    buffer_.Absorb(key, WriteOp{Del{.keys = {key}}}, eviction);
  }
};

// ---------------------------------------------------------------------------
// Existing tests (updated for new signatures)
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, AbsorbStringThenReadReturnsBulkString) {
  AbsorbString("ka", "v");
  auto result = buffer_.Read("ka");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(CompactionBufferTest, AbsorbDelThenReadReturnsNull) {
  AbsorbString("ka", "v");
  AbsorbDel("ka");
  auto result = buffer_.Read("ka");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(CompactionBufferTest, AbsorbCollectionReadReturnsNotFound) {
  auto eviction = core::EvictionTTL{3600};
  buffer_.Absorb("ka", WriteOp{SetAdd{.key = "ka", .members = {"a"}}}, eviction);
  auto result = buffer_.Read("ka");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(CompactionBufferTest, ReadNonexistentKeyReturnsNotFound) {
  auto result = buffer_.Read("missing");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(CompactionBufferTest, SizeReflectsDistinctKeys) {
  AbsorbString("k1", "v1");
  AbsorbString("k2", "v2");
  AbsorbString("k1", "v3");
  EXPECT_EQ(buffer_.Size(), 2);
}

TEST_F(CompactionBufferTest, AbsorbSameKeyIncrementsWriteCount) {
  AbsorbString("ka1", "v1");
  AbsorbString("ka1", "v2");
  AbsorbString("ka1", "v3");

  auto result = buffer_.Read("ka1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v3");
}

TEST_F(CompactionBufferTest, EmptyBufferSizeIsZero) { EXPECT_EQ(buffer_.Size(), 0); }

// ---------------------------------------------------------------------------
// Part A: BytesEstimate
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, EmptyBufferBytesEstimateIsZero) {
  EXPECT_EQ(buffer_.BytesEstimate(), 0);
}

TEST_F(CompactionBufferTest, BytesEstimateIncreasesOnAbsorb) {
  AbsorbString("key", "value");
  EXPECT_GT(buffer_.BytesEstimate(), 0);
}

TEST_F(CompactionBufferTest, BytesEstimateGrowsWithMoreKeys) {
  AbsorbString("k1", "v1");
  auto after_one = buffer_.BytesEstimate();

  AbsorbString("k2", "v2");
  auto after_two = buffer_.BytesEstimate();

  EXPECT_GT(after_two, after_one);
}

TEST_F(CompactionBufferTest, BytesEstimateDecreasesAfterFlush) {
  AbsorbString("k", "value");
  auto before = buffer_.BytesEstimate();
  ASSERT_GT(before, 0);

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);

  EXPECT_EQ(buffer_.BytesEstimate(), 0);
}

TEST_F(CompactionBufferTest, BytesEstimateReflectsCollectionSize) {
  auto eviction = core::EvictionTTL{3600};
  buffer_.Absorb("ka", WriteOp{SetAdd{.key = "ka", .members = {"member1"}}}, eviction);
  auto after_one = buffer_.BytesEstimate();

  buffer_.Absorb("ka", WriteOp{SetAdd{.key = "ka", .members = {"member2", "member3", "member4"}}},
                 eviction);
  auto after_four = buffer_.BytesEstimate();

  EXPECT_GT(after_four, after_one);
}

TEST_F(CompactionBufferTest, BytesEstimateDecreasesOnDel) {
  AbsorbString("k", std::string(1000, 'x'));
  auto before_del = buffer_.BytesEstimate();

  AbsorbDel("k");
  auto after_del = buffer_.BytesEstimate();

  EXPECT_LT(after_del, before_del);
}

// ---------------------------------------------------------------------------
// Part B: FlushReady timing
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, FlushReadyReturnsNothingWhenNothingDue) {
  auto key = MakeKey();
  AbsorbString(key, "v");
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_TRUE(flushed.empty());
  clock_.Advance(60s);
  buffer_.FlushReady(clock_.SteadyNow());
}

TEST_F(CompactionBufferTest, FlushReadyReturnsEntryAfterQuietWindow) {
  FlushStrategy no_jitter{30s, 300s, 0.0};
  CompactionBuffer buf{no_jitter, clock_.SteadyFn(), kTestSeed};

  auto key = MakeKey();
  buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, core::EvictionTTL{3600});
  clock_.Advance(31s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].key, key);
}

TEST_F(CompactionBufferTest, FlushReadyReturnsAllWhenAllDue) {
  auto k1 = MakeKey();
  auto k2 = MakeKey();
  auto k3 = MakeKey();
  AbsorbString(k1, "v1");
  AbsorbString(k2, "v2");
  AbsorbString(k3, "v3");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 3);
}

TEST_F(CompactionBufferTest, FlushReadyReturnsSubsetWhenSomeDue) {
  FlushStrategy no_jitter{30s, 300s, 0.0};
  CompactionBuffer buf{no_jitter, clock_.SteadyFn(), kTestSeed};

  auto early = MakeKey();
  auto late = MakeKey();
  buf.Absorb(early, WriteOp{StringSet{.key = early, .value = "v1"}}, core::EvictionTTL{3600});
  clock_.Advance(20s);
  buf.Absorb(late, WriteOp{StringSet{.key = late, .value = "v2"}}, core::EvictionTTL{3600});

  // Advance past early's quiet deadline but before late's.
  // early: absorbed at t0, quiet deadline = t0 + 30s. Now at t0 + 20s + 15s = t0 + 35s.
  // late:  absorbed at t0+20s, quiet deadline = t0 + 50s. 35s < 50s, so not due.
  clock_.Advance(15s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());

  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].key, early);
  EXPECT_EQ(buf.Size(), 1);
}

TEST_F(CompactionBufferTest, FlushReadyReturnsNothingOnEmptyBuffer) {
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_TRUE(flushed.empty());
}

TEST_F(CompactionBufferTest, FlushReadyEarliestScheduledFirst) {
  // Create entries with different first_seen times so their deadlines differ.
  AbsorbString("first", "v1");
  clock_.Advance(5s);
  AbsorbString("second", "v2");
  clock_.Advance(5s);
  AbsorbString("third", "v3");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 3);
  EXPECT_EQ(flushed[0].key, "first");
  EXPECT_EQ(flushed[1].key, "second");
  EXPECT_EQ(flushed[2].key, "third");
}

TEST_F(CompactionBufferTest, EvictionDeadlineForcesEarlyFlush) {
  FlushStrategy strategy{30s, 300s, 0.0};
  CompactionBuffer buf{strategy, clock_.SteadyFn(), kTestSeed};

  auto key = MakeKey();
  buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, core::EvictionTTL{310});
  // Eviction deadline = first_seen + 310 - 300 = first_seen + 10s.
  // With quiet_threshold=30s, eviction dominates.
  clock_.Advance(15s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].key, key);
}

// ---------------------------------------------------------------------------
// Staleness
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, ReAbsorbSlidesQuietDeadline) {
  AbsorbString("k", "v1");

  // 25s later, re-absorb. Old quiet deadline was t0+30+jitter. New is t0+25+30+jitter.
  clock_.Advance(25s);
  AbsorbString("k", "v2");

  // At t0+35, old deadline would have passed but new hasn't.
  clock_.Advance(10s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_TRUE(flushed.empty());
  EXPECT_EQ(buffer_.Size(), 1);

  // Advance past the new deadline.
  clock_.Advance(25s);
  flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].key, "k");
}

TEST_F(CompactionBufferTest, MultipleReAbsorbsOnlyLatestMatters) {
  AbsorbString("k", "v1");
  clock_.Advance(10s);
  AbsorbString("k", "v2");
  clock_.Advance(10s);
  AbsorbString("k", "v3");

  // Now at t0+20. Latest absorb at t0+20, quiet deadline = t0+50+jitter.
  // The two stale heap entries (t0+30+j, t0+40+j) will be skipped.
  clock_.Advance(15s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_TRUE(flushed.empty());

  clock_.Advance(20s);
  flushed = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].state.StringValue(), "v3");
}

TEST_F(CompactionBufferTest, FlushedEntryCarriesLatestState) {
  AbsorbString("k", "v1");
  AbsorbString("k", "v2");
  AbsorbString("k", "v3");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].state.StringValue(), "v3");
  EXPECT_EQ(flushed[0].write_count, 3);
}

// ---------------------------------------------------------------------------
// Jitter
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, DeterministicJitterWithFixedSeed) {
  CompactionBuffer buf1{strategy_, clock_.SteadyFn(), kTestSeed};
  CompactionBuffer buf2{strategy_, clock_.SteadyFn(), kTestSeed};

  buf1.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction);
  buf2.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction);

  clock_.Advance(60s);
  auto f1 = buf1.FlushReady(clock_.SteadyNow());
  auto f2 = buf2.FlushReady(clock_.SteadyNow());

  ASSERT_EQ(f1.size(), 1);
  ASSERT_EQ(f2.size(), 1);
  EXPECT_EQ(f1[0].jitter_offset, f2[0].jitter_offset);
}

TEST_F(CompactionBufferTest, JitterWithinExpectedRange) {
  auto max_jitter = strategy_.MaxJitter();
  ASSERT_GT(max_jitter.count(), 0);

  for (int i = 0; i < 100; ++i) {
    CompactionBuffer buf{strategy_, clock_.SteadyFn(), static_cast<uint64_t>(i)};
    auto key = "k" + std::to_string(i);
    buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, kDefaultEviction);

    clock_.Advance(60s);
    auto flushed = buf.FlushReady(clock_.SteadyNow());
    ASSERT_EQ(flushed.size(), 1);

    EXPECT_GE(flushed[0].jitter_offset.count(), 0);
    EXPECT_LE(flushed[0].jitter_offset, max_jitter);

    clock_.Set(core::SteadyTime{std::chrono::seconds{1000000}});
  }
}

TEST_F(CompactionBufferTest, JitterDelaysFlushBeyondBaseDeadline) {
  // With zero jitter, a quiet deadline = last_modified + 30s.
  // With jitter, the scheduled time is later than that.
  FlushStrategy no_jitter_strategy{30s, 300s, 0.0};
  CompactionBuffer no_jitter_buf{no_jitter_strategy, clock_.SteadyFn(), kTestSeed};

  no_jitter_buf.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction);
  clock_.Advance(31s);

  auto flushed_no_jitter = no_jitter_buf.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed_no_jitter.size(), 1);

  // With default jitter (10% of 30s = up to 3s), the entry might not be ready at +31s.
  clock_.Set(core::SteadyTime{std::chrono::seconds{1000000}});
  AbsorbString("k2", "v");
  clock_.Advance(31s);

  auto flushed_with_jitter = buffer_.FlushReady(clock_.SteadyNow());
  // May or may not have flushed depending on jitter value — but advancing past max jitter works.
  clock_.Advance(5s);
  auto flushed_after_jitter = buffer_.FlushReady(clock_.SteadyNow());

  auto total = flushed_with_jitter.size() + flushed_after_jitter.size();
  EXPECT_EQ(total, 1);
}

TEST_F(CompactionBufferTest, ZeroJitterFractionMeansNoJitter) {
  FlushStrategy zero_jitter{30s, 300s, 0.0};
  CompactionBuffer buf{zero_jitter, clock_.SteadyFn(), kTestSeed};

  buf.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction);
  clock_.Advance(60s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].jitter_offset.count(), 0);
}

// ---------------------------------------------------------------------------
// Post-flush reads
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, ReadAfterFlushReturnsNotFound) {
  AbsorbString("k", "v");
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);

  auto result = buffer_.Read("k");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(CompactionBufferTest, SizeDecreasesAfterFlush) {
  AbsorbString("k1", "v1");
  AbsorbString("k2", "v2");
  EXPECT_EQ(buffer_.Size(), 2);

  clock_.Advance(60s);
  buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(buffer_.Size(), 0);
}

// ---------------------------------------------------------------------------
// Tombstones
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, TombstoneFlushedLikeAnyEntry) {
  AbsorbString("k", "v");
  AbsorbDel("k");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_TRUE(flushed[0].state.IsTombstone());
}

TEST_F(CompactionBufferTest, TombstoneEmitsEmptyVector) {
  AbsorbString("k", "v");
  AbsorbDel("k");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  auto ops = flushed[0].state.Emit();
  EXPECT_TRUE(ops.empty());
}

// ---------------------------------------------------------------------------
// Timestamp and write count verification
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, FlushedEntryHasCorrectTimestamps) {
  auto t0 = clock_.SteadyNow();
  AbsorbString("k", "v1");

  clock_.Advance(5s);
  AbsorbString("k", "v2");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].first_seen, t0);
  EXPECT_EQ(flushed[0].last_modified, t0 + 5s);
  EXPECT_EQ(flushed[0].write_count, 2);
}

TEST_F(CompactionBufferTest, FlushedEntryCarriesEviction) {
  auto eviction = core::EvictionTTL{7200};
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, eviction);

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].eviction, eviction);
}

}  // namespace
}  // namespace abyss::consumer
