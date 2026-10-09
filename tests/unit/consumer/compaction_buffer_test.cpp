#include "abyss/consumer/compaction_buffer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>

#include "test_clock.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;
using core::ops::Del;
using core::ops::HashSet;
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
    buffer_.Absorb(key, WriteOp{StringSet{.key = key, .value = value}}, eviction, 1, 1, 0);
  }

  void AbsorbDel(const std::string& key, core::EvictionTTL eviction = core::EvictionTTL{3600}) {
    buffer_.Absorb(key, WriteOp{Del{.keys = {key}}}, eviction, 1, 1, 0);
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
  buffer_.Absorb("ka", WriteOp{SetAdd{.key = "ka", .members = {"a"}}}, eviction, 1, 1, 0);
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
  buffer_.EraseFlushed(flushed);

  EXPECT_EQ(buffer_.BytesEstimate(), 0);
}

TEST_F(CompactionBufferTest, BytesEstimateReflectsCollectionSize) {
  auto eviction = core::EvictionTTL{3600};
  buffer_.Absorb("ka", WriteOp{SetAdd{.key = "ka", .members = {"member1"}}}, eviction, 1, 1, 0);
  auto after_one = buffer_.BytesEstimate();

  buffer_.Absorb("ka", WriteOp{SetAdd{.key = "ka", .members = {"member2", "member3", "member4"}}},
                 eviction, 1, 1, 0);
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
  buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, core::EvictionTTL{3600}, 1, 1, 0);
  clock_.Advance(31s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().key, key);
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
  buf.Absorb(early, WriteOp{StringSet{.key = early, .value = "v1"}}, core::EvictionTTL{3600}, 1, 1,
             0);
  clock_.Advance(20s);
  buf.Absorb(late, WriteOp{StringSet{.key = late, .value = "v2"}}, core::EvictionTTL{3600}, 1, 1,
             0);

  // Advance past early's quiet deadline but before late's.
  // early: absorbed at t0, quiet deadline = t0 + 30s. Now at t0 + 20s + 15s = t0 + 35s.
  // late:  absorbed at t0+20s, quiet deadline = t0 + 50s. 35s < 50s, so not due.
  clock_.Advance(15s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());

  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().key, early);
  buf.EraseFlushed(flushed);
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
  EXPECT_EQ(flushed[0].get().key, "first");
  EXPECT_EQ(flushed[1].get().key, "second");
  EXPECT_EQ(flushed[2].get().key, "third");
}

TEST_F(CompactionBufferTest, EvictionDeadlineForcesEarlyFlush) {
  FlushStrategy strategy{30s, 300s, 0.0};
  CompactionBuffer buf{strategy, clock_.SteadyFn(), kTestSeed};

  auto key = MakeKey();
  buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, core::EvictionTTL{310}, 1, 1, 0);
  // Eviction deadline = first_seen + 310 - 300 = first_seen + 10s.
  // With quiet_threshold=30s, eviction dominates.
  clock_.Advance(15s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().key, key);
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
  EXPECT_EQ(flushed[0].get().key, "k");
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
  EXPECT_EQ(flushed[0].get().state.StringValue(), "v3");
}

TEST_F(CompactionBufferTest, FlushedEntryCarriesLatestState) {
  AbsorbString("k", "v1");
  AbsorbString("k", "v2");
  AbsorbString("k", "v3");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().state.StringValue(), "v3");
  EXPECT_EQ(flushed[0].get().write_count, 3);
}

// ---------------------------------------------------------------------------
// Jitter
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, DeterministicJitterWithFixedSeed) {
  CompactionBuffer buf1{strategy_, clock_.SteadyFn(), kTestSeed};
  CompactionBuffer buf2{strategy_, clock_.SteadyFn(), kTestSeed};

  buf1.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction, 1, 1, 0);
  buf2.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction, 1, 1, 0);

  clock_.Advance(60s);
  auto f1 = buf1.FlushReady(clock_.SteadyNow());
  auto f2 = buf2.FlushReady(clock_.SteadyNow());

  ASSERT_EQ(f1.size(), 1);
  ASSERT_EQ(f2.size(), 1);
  EXPECT_EQ(f1[0].get().jitter_offset, f2[0].get().jitter_offset);
}

TEST_F(CompactionBufferTest, JitterWithinExpectedRange) {
  auto max_jitter = strategy_.MaxJitter();
  ASSERT_GT(max_jitter.count(), 0);

  for (int i = 0; i < 100; ++i) {
    CompactionBuffer buf{strategy_, clock_.SteadyFn(), static_cast<uint64_t>(i)};
    auto key = "k" + std::to_string(i);
    buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, kDefaultEviction, 1, 1, 0);

    clock_.Advance(60s);
    auto flushed = buf.FlushReady(clock_.SteadyNow());
    ASSERT_EQ(flushed.size(), 1);

    EXPECT_GE(flushed[0].get().jitter_offset.count(), 0);
    EXPECT_LE(flushed[0].get().jitter_offset, max_jitter);

    clock_.Set(core::SteadyTime{std::chrono::seconds{1000000}});
  }
}

TEST_F(CompactionBufferTest, JitterDelaysFlushBeyondBaseDeadline) {
  // With zero jitter, a quiet deadline = last_modified + 30s.
  // With jitter, the scheduled time is later than that.
  FlushStrategy no_jitter_strategy{30s, 300s, 0.0};
  CompactionBuffer no_jitter_buf{no_jitter_strategy, clock_.SteadyFn(), kTestSeed};

  no_jitter_buf.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction, 1, 1,
                       0);
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

  buf.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction, 1, 1, 0);
  clock_.Advance(60s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().jitter_offset.count(), 0);
}

// ---------------------------------------------------------------------------
// Post-flush reads
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, ReadAfterFlushReturnsNotFound) {
  AbsorbString("k", "v");
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  buffer_.EraseFlushed(flushed);

  auto result = buffer_.Read("k");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(CompactionBufferTest, SizeDecreasesAfterFlush) {
  AbsorbString("k1", "v1");
  AbsorbString("k2", "v2");
  EXPECT_EQ(buffer_.Size(), 2);

  clock_.Advance(60s);
  buffer_.EraseFlushed(buffer_.FlushReady(clock_.SteadyNow()));
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
  EXPECT_TRUE(flushed[0].get().state.IsTombstone());
}

TEST_F(CompactionBufferTest, TombstoneEmitsEmptyVector) {
  AbsorbString("k", "v");
  AbsorbDel("k");

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  auto ops = flushed[0].get().state.Emit();
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
  EXPECT_EQ(flushed[0].get().first_seen, t0);
  EXPECT_EQ(flushed[0].get().last_modified, t0 + 5s);
  EXPECT_EQ(flushed[0].get().write_count, 2);
}

TEST_F(CompactionBufferTest, FlushedEntryCarriesEviction) {
  auto eviction = core::EvictionTTL{7200};
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, eviction, 1, 1, 0);

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().eviction, eviction);
}

// ---------------------------------------------------------------------------
// Sequence tracking + low-water commit
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, AbsorbRecordsFirstSeenSeq) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction, 42, 42, 0);
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().first_seen_seq, 42U);
}

TEST_F(CompactionBufferTest, ReAbsorbKeepsFirstSeenSeq) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v1"}}, kDefaultEviction, 10, 10, 0);
  clock_.Advance(5s);
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v2"}}, kDefaultEviction, 11, 11, 0);
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().first_seen_seq, 10U);
}

TEST_F(CompactionBufferTest, OldestPendingSeqEmptyReturnsNullopt) {
  EXPECT_FALSE(buffer_.OldestPendingSeq().has_value());
}

TEST_F(CompactionBufferTest, OldestPendingSeqReturnsMinimum) {
  buffer_.Absorb("a", WriteOp{StringSet{.key = "a", .value = "v"}}, kDefaultEviction, 7, 7, 0);
  buffer_.Absorb("b", WriteOp{StringSet{.key = "b", .value = "v"}}, kDefaultEviction, 3, 3, 0);
  buffer_.Absorb("c", WriteOp{StringSet{.key = "c", .value = "v"}}, kDefaultEviction, 11, 11, 0);
  auto oldest = buffer_.OldestPendingSeq();
  EXPECT_EQ(oldest, std::optional{3U});
}

TEST_F(CompactionBufferTest, OldestPendingSeqAdvancesAfterFlush) {
  buffer_.Absorb("a", WriteOp{StringSet{.key = "a", .value = "v"}}, kDefaultEviction, 5, 5, 0);
  buffer_.Absorb("b", WriteOp{StringSet{.key = "b", .value = "v"}}, kDefaultEviction, 8, 8, 0);

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 2);
  buffer_.EraseFlushed(flushed);
  EXPECT_FALSE(buffer_.OldestPendingSeq().has_value());
}

// ---------------------------------------------------------------------------
// COLDC-5: oldest first_seen (ADP-004 lag signal)
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, OldestFirstSeenEmptyReturnsNullopt) {
  EXPECT_FALSE(buffer_.OldestFirstSeen().has_value());
}

TEST_F(CompactionBufferTest, OldestFirstSeenReturnsMinimumAcrossEntries) {
  const auto t0 = clock_.SteadyNow();
  AbsorbString("a", "v");
  clock_.Advance(10s);
  AbsorbString("b", "v");
  clock_.Advance(10s);
  AbsorbString("c", "v");

  auto oldest = buffer_.OldestFirstSeen();
  EXPECT_EQ(oldest, std::optional{t0});
}

TEST_F(CompactionBufferTest, OldestFirstSeenUnmovedByReabsorbOfSameKey) {
  const auto t0 = clock_.SteadyNow();
  AbsorbString("a", "v1");
  clock_.Advance(10s);
  AbsorbString("a", "v2");

  auto oldest = buffer_.OldestFirstSeen();
  EXPECT_EQ(oldest, std::optional{t0});
}

TEST_F(CompactionBufferTest, OldestFirstSeenAdvancesToNextOldestAfterFlush) {
  AbsorbString("a", "v");
  clock_.Advance(10s);
  const auto t1 = clock_.SteadyNow();
  AbsorbString("b", "v");

  // FlushOldest pops in scheduled order; "a" is scheduled 10s ahead of "b".
  auto flushed = buffer_.FlushOldest(/*target_bytes=*/0, /*max_count=*/1);
  ASSERT_EQ(flushed.size(), 1U);
  EXPECT_EQ(flushed[0].get().key, "a");
  buffer_.EraseFlushed(flushed);

  auto oldest = buffer_.OldestFirstSeen();
  EXPECT_EQ(oldest, std::optional{t1});
}

// ---------------------------------------------------------------------------
// FlushOldest (aggressive mode)
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, FlushOldestEmptyBufferReturnsNothing) {
  auto flushed = buffer_.FlushOldest(0, 10);
  EXPECT_TRUE(flushed.empty());
}

TEST_F(CompactionBufferTest, FlushOldestStopsWhenBelowTargetBytes) {
  AbsorbString("a", "v1");
  AbsorbString("b", "v2");
  AbsorbString("c", "v3");
  auto all_bytes = buffer_.BytesEstimate();

  // Target is half of current: expect to flush some but not all.
  auto flushed = buffer_.FlushOldest(all_bytes / 2, 100);
  EXPECT_GT(flushed.size(), 0);
  EXPECT_LT(flushed.size(), 3);
  buffer_.EraseFlushed(flushed);
  EXPECT_LE(buffer_.BytesEstimate(), (all_bytes / 2) + 1);  // +1 for rounding
}

TEST_F(CompactionBufferTest, FlushOldestRespectsCountCap) {
  for (int i = 0; i < 5; ++i) {
    AbsorbString("k" + std::to_string(i), "v");
  }
  auto flushed = buffer_.FlushOldest(0, 2);
  EXPECT_EQ(flushed.size(), 2);
}

TEST_F(CompactionBufferTest, FlushOldestPopsOldestFirst) {
  AbsorbString("first", "v");
  clock_.Advance(5s);
  AbsorbString("second", "v");
  clock_.Advance(5s);
  AbsorbString("third", "v");

  auto flushed = buffer_.FlushOldest(0, 1);
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].get().key, "first");
}

TEST_F(CompactionBufferTest, FlushOldestBypassesQuietWindow) {
  // Entry isn't due by quiet window; FlushOldest ignores that.
  AbsorbString("k", "v");
  auto flushed_ready = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_TRUE(flushed_ready.empty());

  auto flushed_oldest = buffer_.FlushOldest(0, 1);
  EXPECT_EQ(flushed_oldest.size(), 1);
}

// ---------------------------------------------------------------------------
// In-flight batches: selection keeps entries until erase or reschedule
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, SelectedEntriesStayReadable) {
  buffer_.Absorb("s", WriteOp{StringSet{.key = "s", .value = "v"}}, kDefaultEviction, 4, 4, 0);
  buffer_.Absorb("h", WriteOp{HashSet{.key = "h", .fields = {{.field = "f", .value = "1"}}}},
                 kDefaultEviction, 2, 2, 0);
  clock_.Advance(60s);

  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 2U);

  EXPECT_EQ(buffer_.Size(), 2U);
  auto read = buffer_.Read("s");
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "v");
  const auto hash = buffer_.Snapshot("h");
  ASSERT_TRUE(hash.has_value());
  EXPECT_EQ(hash.value_or(CompactedState{}).HashFields().at("f"), "1");
  EXPECT_EQ(buffer_.OldestPendingSeq(), std::optional<core::SequenceId>{2});
  EXPECT_GT(buffer_.BytesEstimate(), 0U);
}

TEST_F(CompactionBufferTest, EraseFlushedRemovesExactlyTheBatch) {
  AbsorbString("k0", "v");
  clock_.Advance(5s);
  AbsorbString("k1", "v");
  clock_.Advance(5s);
  AbsorbString("k2", "v");
  const auto all_bytes = buffer_.BytesEstimate();

  auto flushed = buffer_.FlushOldest(/*target_bytes=*/0, /*max_count=*/2);
  ASSERT_EQ(flushed.size(), 2U);
  EXPECT_EQ(flushed[0].get().key, "k0");
  EXPECT_EQ(flushed[1].get().key, "k1");
  buffer_.EraseFlushed(flushed);

  EXPECT_EQ(buffer_.Size(), 1U);
  EXPECT_TRUE(buffer_.Read("k2").has_value());
  EXPECT_EQ(buffer_.Read("k0").error().code(), core::ErrorCode::kNotFound);
  EXPECT_EQ(buffer_.Read("k1").error().code(), core::ErrorCode::kNotFound);
  EXPECT_LT(buffer_.BytesEstimate(), all_bytes);
  EXPECT_GT(buffer_.BytesEstimate(), 0U);
}

TEST_F(CompactionBufferTest, RescheduleMakesTheBatchDueAgain) {
  AbsorbString("k", "v");
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1U);
  EXPECT_EQ(buffer_.HeapDepth(), 0U);
  EXPECT_TRUE(buffer_.FlushReady(clock_.SteadyNow()).empty()) << "an in-flight key was reselected";
  EXPECT_TRUE(buffer_.FlushOldest(0, 10).empty()) << "an in-flight key was reselected";

  buffer_.Reschedule(flushed);
  EXPECT_EQ(buffer_.HeapDepth(), 1U);
  EXPECT_EQ(buffer_.Size(), 1U);

  auto again = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(again.size(), 1U);
  EXPECT_EQ(again[0].get().key, "k");
  EXPECT_EQ(again[0].get().state.StringValue(), "v");
}

TEST_F(CompactionBufferTest, LastSeqIsTheMaxCarrierAndFirstSeenKeepsThePosition) {
  // A Resolved's ops absorb at its Conditional's position and its own carrier.
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "a"}}, kDefaultEviction, 5, 9, 0);
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "b"}}, kDefaultEviction, 6, 7, 0);
  EXPECT_EQ(buffer_.OldestPendingSeq(), std::optional<core::SequenceId>{5});
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "c"}}, kDefaultEviction, 12, 12, 0);

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1U);
  EXPECT_EQ(flushed[0].get().first_seen_seq, 5U);
  EXPECT_EQ(flushed[0].get().last_seq, 12U);

  buffer_.Reschedule(flushed);
  auto again = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(again.size(), 1U);
  EXPECT_EQ(again[0].get().last_seq, 12U) << "a reschedule lost last_seq";
}

TEST_F(CompactionBufferTest, OldestPendingSeqUnchangedByInFlightBatch) {
  buffer_.Absorb("a", WriteOp{StringSet{.key = "a", .value = "v"}}, kDefaultEviction, 3, 3, 0);
  buffer_.Absorb("b", WriteOp{StringSet{.key = "b", .value = "v"}}, kDefaultEviction, 8, 8, 0);
  clock_.Advance(60s);

  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 2U);
  EXPECT_EQ(buffer_.OldestPendingSeq(), std::optional<core::SequenceId>{3});
  buffer_.Reschedule(flushed);
  EXPECT_EQ(buffer_.OldestPendingSeq(), std::optional<core::SequenceId>{3});

  buffer_.EraseFlushed(buffer_.FlushReady(clock_.SteadyNow()));
  EXPECT_FALSE(buffer_.OldestPendingSeq().has_value());
}

// ---------------------------------------------------------------------------
// Log clock: the first unflushed appended_at of the oldest entry
// ---------------------------------------------------------------------------

class LogClockTest : public CompactionBufferTest {
 protected:
  void Absorb(const std::string& key, core::SequenceId seq, uint64_t appended_at_ms) {
    buf_.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, kDefaultEviction, seq, seq,
                appended_at_ms);
  }

  // The entry scheduled first, selected for a flush.
  FlushBatch SelectOne() { return buf_.FlushOldest(/*target_bytes=*/0, /*max_count=*/1); }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  CompactionBuffer buf_{FlushStrategy{30s, 300s, 0.0}, clock_.SteadyFn(), kTestSeed};
};

TEST_F(LogClockTest, WaitsForTheOldestEntryWhateverFlushesFirst) {
  EXPECT_EQ(buf_.LogClockMs(), 0U);
  Absorb("k", 1, 100);
  clock_.Advance(5s);
  Absorb("j", 2, 200);
  EXPECT_EQ(buf_.LogClockMs(), 100U);

  // k's later write moves its schedule behind j's but keeps its time.
  clock_.Advance(1s);
  Absorb("k", 3, 300);
  EXPECT_EQ(buf_.LogClockMs(), 100U);

  auto j = SelectOne();
  ASSERT_EQ(j.size(), 1U);
  ASSERT_EQ(j[0].get().key, "j");
  buf_.EraseFlushed(j);
  EXPECT_EQ(buf_.LogClockMs(), 100U) << "the clock passed k's pending write";

  auto k = SelectOne();
  ASSERT_EQ(k.size(), 1U);
  EXPECT_EQ(buf_.LogClockMs(), 100U) << "the clock passed an entry mid-flush";
  buf_.EraseFlushed(k);
  EXPECT_EQ(buf_.LogClockMs(), 300U);
}

TEST_F(LogClockTest, HoldsThroughARescheduledFlush) {
  Absorb("k", 1, 100);
  clock_.Advance(5s);
  Absorb("j", 2, 200);

  auto k = SelectOne();
  ASSERT_EQ(k.size(), 1U);
  buf_.Reschedule(k);
  EXPECT_EQ(buf_.LogClockMs(), 100U);

  k = SelectOne();
  ASSERT_EQ(k.size(), 1U);
  ASSERT_EQ(k[0].get().key, "k");
  EXPECT_EQ(k[0].get().first_appended_at_ms, 100U);
  buf_.EraseFlushed(k);
  EXPECT_EQ(buf_.LogClockMs(), 200U);
}

TEST_F(LogClockTest, AnEntryAbsorbedAfterItsFlushStartsFresh) {
  Absorb("k", 1, 100);
  buf_.EraseFlushed(SelectOne());
  EXPECT_EQ(buf_.LogClockMs(), 100U);

  Absorb("k", 2, 500);
  Absorb("j", 3, 600);
  EXPECT_EQ(buf_.LogClockMs(), 500U);
}

#ifndef NDEBUG
using LogClockDeathTest = LogClockTest;

// The sequencer stamps a shard's writes in seq order, so a stamp below
// one absorbed already is a bug: caught at absorb, never absorbed.
TEST_F(LogClockDeathTest, AStampBelowOneAbsorbedIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  Absorb("k", 1, 300);
  clock_.Advance(5s);
  EXPECT_DEATH(Absorb("j", 2, 200), "appended_at went backwards");
}
#endif

TEST_F(LogClockTest, AFlushAdvancesIt) {
  Absorb("k", 1, 100);
  buf_.Clear(400);
  EXPECT_EQ(buf_.LogClockMs(), 400U);
  buf_.Clear(50);
  EXPECT_EQ(buf_.LogClockMs(), 400U);
  Absorb("k", 2, 450);
  EXPECT_EQ(buf_.LogClockMs(), 450U);
}

// OldestPendingSeq and LogClockMs keep ordered indexes; a full scan of
// the entries is the oracle. Positions sometimes run behind their
// carriers, as a Resolved's do.
TEST_F(CompactionBufferTest, PendingIndexesAgreeWithAFullScan) {
  std::mt19937_64 rng(kTestSeed);  // NOLINT(bugprone-random-generator-seed): reproducible.
  CompactionBuffer buf{strategy_, clock_.SteadyFn(), kTestSeed};
  core::SequenceId seq = 0;
  uint64_t appended_at_ms = 1000;
  uint64_t last_clock = 0;
  FlushBatch in_flight;
  for (int step = 0; step < 20000; ++step) {
    const uint64_t roll = rng() % 100;
    if (!in_flight.empty()) {
      if (roll < 60) {
        buf.EraseFlushed(in_flight);
      } else {
        buf.Reschedule(in_flight);
      }
      in_flight.clear();
    } else if (roll < 60) {
      ++seq;
      appended_at_ms += rng() % 50;
      const core::SequenceId position = (seq > 8 && rng() % 8 == 0) ? seq - 1 - (rng() % 6) : seq;
      const std::string key = "k" + std::to_string(rng() % 32);
      buf.Absorb(key, WriteOp{StringSet{.key = key, .value = "v"}}, kDefaultEviction, position, seq,
                 appended_at_ms);
    } else if (roll < 75) {
      clock_.Advance(std::chrono::seconds(rng() % 20));
      in_flight = buf.FlushReady(clock_.SteadyNow(), 1 + (rng() % 4));
    } else if (roll < 98) {
      in_flight = buf.FlushOldest(/*target_bytes=*/0, 1 + (rng() % 4));
    } else {
      buf.Clear(appended_at_ms);
    }
    ASSERT_EQ(buf.OldestPendingSeq(), buf.OldestPendingSeqScanForTesting()) << "step " << step;
    ASSERT_EQ(buf.LogClockMs(), buf.LogClockScanForTesting()) << "step " << step;
    ASSERT_GE(buf.LogClockMs(), last_clock) << "step " << step;
    last_clock = buf.LogClockMs();
  }
  if (!in_flight.empty()) buf.EraseFlushed(in_flight);
}

using CompactionBufferDeathTest = CompactionBufferTest;

// A batch is applied by reference, so the buffer must not change under it.
TEST_F(CompactionBufferDeathTest, MutatingDuringAnInFlightFlushIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  AbsorbString("a", "v");
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1U);

  EXPECT_DEATH(AbsorbString("a", "w"), "absorbed during an in-flight flush");
  EXPECT_DEATH(buffer_.Clear(0), "cleared during an in-flight flush");

  buffer_.Reschedule(flushed);
  AbsorbString("a", "w");
  buffer_.Clear(0);
}

TEST_F(CompactionBufferTest, KeyWithTwoLiveHeapEntriesIsSelectedOnce) {
  FlushStrategy no_jitter{30s, 300s, 0.0};
  CompactionBuffer buf{no_jitter, clock_.SteadyFn(), kTestSeed};
  const auto absorb = [&buf](core::EvictionTTL eviction) {
    buf.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, eviction, 1, 1, 0);
  };
  // Deadline t0+10s, then quiet t0+31s, then the t0+10s deadline again.
  absorb(core::EvictionTTL{310});
  clock_.Advance(1s);
  absorb(core::EvictionTTL{3600});
  clock_.Advance(1s);
  absorb(core::EvictionTTL{310});
  ASSERT_EQ(buf.HeapDepth(), 3U);

  clock_.Advance(15s);
  auto flushed = buf.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(flushed.size(), 1U);
  buf.EraseFlushed(flushed);
  EXPECT_EQ(buf.Size(), 0U);
}

TEST_F(CompactionBufferTest, ClearDropsEntriesAndHeap) {
  const auto eviction = core::EvictionTTL{3600};
  buffer_.Absorb("s", WriteOp{StringSet{.key = "s", .value = "v"}}, eviction, 1, 1, 0);
  buffer_.Absorb("set_k", WriteOp{SetAdd{.key = "set_k", .members = {"a", "b"}}}, eviction, 1, 1,
                 0);
  buffer_.Absorb("hash_k",
                 WriteOp{HashSet{.key = "hash_k", .fields = {{.field = "f", .value = "v"}}}},
                 eviction, 1, 1, 0);
  buffer_.Absorb(
      "zset_k",
      WriteOp{core::ops::ZsetAdd{.key = "zset_k", .entries = {{.score = 1.0, .member = "m"}}}},
      eviction, 1, 1, 0);
  ASSERT_EQ(buffer_.Size(), 4U);
  ASSERT_GT(buffer_.BytesEstimate(), 0U);

  buffer_.Clear(0);

  EXPECT_EQ(buffer_.Size(), 0U);
  EXPECT_EQ(buffer_.BytesEstimate(), 0U);
  EXPECT_FALSE(buffer_.OldestPendingSeq().has_value());
  EXPECT_EQ(buffer_.Read("s").error().code(), core::ErrorCode::kNotFound);

  // A subsequent Absorb works on the empty buffer; the heap is sane.
  buffer_.Absorb("post", WriteOp{StringSet{.key = "post", .value = "v"}}, eviction, 1, 1, 0);
  EXPECT_EQ(buffer_.Size(), 1U);
  auto r = buffer_.Read("post");
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->IsBulkString());
  EXPECT_EQ(r->AsString(), "v");
}

// ---------------------------------------------------------------------------
// COLDC-4: bounded flush heap (per-key dedup + heap-overhead accounting)
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, HotKeyDoesNotPushDuplicateHeapEntryWhenScheduleUnchanged) {
  // Two absorbs of the same key at the SAME clock instant do not move the
  // schedule, so the heap holds a single live entry, not two.
  AbsorbString("hot", "v1");
  EXPECT_EQ(buffer_.HeapDepth(), 1U);
  AbsorbString("hot", "v2");
  EXPECT_EQ(buffer_.HeapDepth(), 1U);

  // Correctness preserved: the key still flushes exactly once.
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1U);
  EXPECT_EQ(buffer_.HeapDepth(), 0U);
}

TEST_F(CompactionBufferTest, BytesEstimateIncludesHeapOverhead) {
  // A hot key whose quiet deadline keeps sliding (distinct instants) retains a
  // stale heap entry per slide. BytesEstimate must charge that occupancy so the
  // growth surfaces as pressure rather than silent unbounded heap growth.
  AbsorbString("hot", "v");
  const auto single = buffer_.BytesEstimate();
  const auto single_depth = buffer_.HeapDepth();
  ASSERT_EQ(single_depth, 1U);

  constexpr int kSlides = 50;
  for (int i = 0; i < kSlides; ++i) {
    clock_.Advance(1s);  // moves last_modified -> new schedule -> new heap entry
    AbsorbString("hot", "v");
  }

  // One live entry per distinct schedule (the original plus each slide).
  EXPECT_EQ(buffer_.HeapDepth(), static_cast<size_t>(1 + kSlides));
  // The estimate grew with the retained heap entries (heap overhead is charged).
  EXPECT_GT(buffer_.BytesEstimate(), single + static_cast<size_t>(kSlides));
}

TEST_F(CompactionBufferTest, HeapOverheadReleasedAfterFlush) {
  // Drive heap growth, then flush; the stale-skip pops must release their
  // overhead so the estimate returns to zero (no upward leak).
  AbsorbString("hot", "v");
  for (int i = 0; i < 20; ++i) {
    clock_.Advance(1s);
    AbsorbString("hot", "v");
  }
  ASSERT_GT(buffer_.HeapDepth(), 1U);

  clock_.Advance(120s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1U);
  buffer_.EraseFlushed(flushed);
  EXPECT_EQ(buffer_.HeapDepth(), 0U);
  EXPECT_EQ(buffer_.BytesEstimate(), 0U);
}

// ---------------------------------------------------------------------------
// Snapshot: the loader's copy of a key's delta
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, SnapshotCopiesTheDeltaWithItsTtlUnjudged) {
  EXPECT_FALSE(buffer_.Snapshot("s").has_value());
  buffer_.Absorb("s", WriteOp{SetAdd{.key = "s", .members = {"a", "b"}}}, kDefaultEviction, 1, 1,
                 0);
  buffer_.Absorb("s", WriteOp{core::ops::SetRem{.key = "s", .members = {"b"}}}, kDefaultEviction, 2,
                 2, 0);
  // Long past on the wall clock, which Exec judges and Snapshot does not.
  buffer_.Absorb("s", WriteOp{core::ops::Expire{.key = "s", .abs_ttl_ms = 1}}, kDefaultEviction, 3,
                 3, 0);

  const auto snapshot = buffer_.Snapshot("s");
  ASSERT_TRUE(snapshot.has_value());
  const CompactedState state = snapshot.value_or(CompactedState{});
  EXPECT_EQ(state.SetMembers(), (std::unordered_set<std::string>{"a"}));
  EXPECT_EQ(state.SetRemovedMembers(), (std::unordered_set<std::string>{"b"}));
  EXPECT_EQ(state.Ttl(), CompactedState::TtlIntent::kSetTo);
  EXPECT_EQ(state.AbsTtlMs(), 1U);
  EXPECT_EQ(buffer_.Exec(core::ops::ReadOp{core::ops::Exists{.keys = {"s"}}})->AsInteger(), 0)
      << "reads judge the TTL";

  buffer_.Absorb("s", WriteOp{SetAdd{.key = "s", .members = {"c"}}}, kDefaultEviction, 4, 4, 0);
  EXPECT_FALSE(state.SetMembers().contains("c")) << "a copy, not a view";
}

}  // namespace
}  // namespace abyss::consumer
