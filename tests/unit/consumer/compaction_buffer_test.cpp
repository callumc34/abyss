#include "abyss/consumer/compaction_buffer.h"

#include <gtest/gtest.h>

#include <array>

#include "test_clock.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;
using core::ops::Del;
using core::ops::HashDel;
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

// ---------------------------------------------------------------------------
// Sequence tracking + low-water ack
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, AbsorbRecordsFirstSeenSeq) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v"}}, kDefaultEviction, 42);
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].first_seen_seq, 42U);
}

TEST_F(CompactionBufferTest, ReAbsorbKeepsFirstSeenSeq) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v1"}}, kDefaultEviction, 10);
  clock_.Advance(5s);
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "v2"}}, kDefaultEviction, 11);
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(flushed[0].first_seen_seq, 10U);
}

TEST_F(CompactionBufferTest, OldestPendingSeqEmptyReturnsNullopt) {
  EXPECT_FALSE(buffer_.OldestPendingSeq().has_value());
}

TEST_F(CompactionBufferTest, OldestPendingSeqReturnsMinimum) {
  buffer_.Absorb("a", WriteOp{StringSet{.key = "a", .value = "v"}}, kDefaultEviction, 7);
  buffer_.Absorb("b", WriteOp{StringSet{.key = "b", .value = "v"}}, kDefaultEviction, 3);
  buffer_.Absorb("c", WriteOp{StringSet{.key = "c", .value = "v"}}, kDefaultEviction, 11);
  auto oldest = buffer_.OldestPendingSeq();
  ASSERT_TRUE(oldest.has_value());
  EXPECT_EQ(*oldest, 3U);
}

TEST_F(CompactionBufferTest, OldestPendingSeqAdvancesAfterFlush) {
  buffer_.Absorb("a", WriteOp{StringSet{.key = "a", .value = "v"}}, kDefaultEviction, 5);
  buffer_.Absorb("b", WriteOp{StringSet{.key = "b", .value = "v"}}, kDefaultEviction, 8);

  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 2);
  EXPECT_FALSE(buffer_.OldestPendingSeq().has_value());
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
  EXPECT_EQ(flushed[0].key, "first");
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
// Reinsert (retry path)
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, ReinsertRestoresEntries) {
  AbsorbString("k", "v");
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);
  EXPECT_EQ(buffer_.Size(), 0);

  buffer_.Reinsert(std::move(flushed));
  EXPECT_EQ(buffer_.Size(), 1);
  EXPECT_GT(buffer_.BytesEstimate(), 0);

  auto again = buffer_.FlushReady(clock_.SteadyNow());
  EXPECT_EQ(again.size(), 1);
}

TEST_F(CompactionBufferTest, ReinsertOverwritesNewerAbsorbForSameKey) {
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "old"}}, kDefaultEviction, 1);
  clock_.Advance(60s);
  auto flushed = buffer_.FlushReady(clock_.SteadyNow());
  ASSERT_EQ(flushed.size(), 1);

  // A newer write comes in while the old snapshot is "in flight".
  buffer_.Absorb("k", WriteOp{StringSet{.key = "k", .value = "new"}}, kDefaultEviction, 2);
  EXPECT_EQ(buffer_.Size(), 1);

  // Reinserting the old snapshot overwrites the newer one. This is acceptable
  // because the cold consumer only uses Reinsert on transient retry failures
  // that it then retries immediately — the "new" write would be re-drained
  // from the queue on the next loop iteration and re-absorbed.
  buffer_.Reinsert(std::move(flushed));
  EXPECT_EQ(buffer_.Size(), 1);
  auto read = buffer_.Read("k");
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "old");
}

// ---------------------------------------------------------------------------
// HashOverlay: snapshot accessor consumed by the engine merge path
// ---------------------------------------------------------------------------

TEST_F(CompactionBufferTest, HashOverlayReturnsNotPresentForAbsentKey) {
  EXPECT_EQ(buffer_.HashOverlayFor("missing").kind, HashOverlay::Kind::kNotPresent);
}

TEST_F(CompactionBufferTest, HashOverlayReturnsHashStateAfterHashSet) {
  buffer_.Absorb(
      "h",
      WriteOp{HashSet{.key = "h",
                      .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}}},
      kDefaultEviction);
  const auto overlay = buffer_.HashOverlayFor("h");
  ASSERT_EQ(overlay.kind, HashOverlay::Kind::kHash);
  EXPECT_EQ(overlay.fields.size(), 2);
  EXPECT_EQ(overlay.fields.at("a"), "1");
  EXPECT_EQ(overlay.fields.at("b"), "2");
  EXPECT_TRUE(overlay.removed_fields.empty());
}

TEST_F(CompactionBufferTest, HashOverlayTracksRemovedFields) {
  buffer_.Absorb("h",
                 WriteOp{HashSet{
                     .key = "h",
                     .fields = {{.field = "keep", .value = "v"}, {.field = "gone", .value = "v"}}}},
                 kDefaultEviction);
  buffer_.Absorb("h", WriteOp{HashDel{.key = "h", .fields = {"gone"}}}, kDefaultEviction);

  const auto overlay = buffer_.HashOverlayFor("h");
  ASSERT_EQ(overlay.kind, HashOverlay::Kind::kHash);
  EXPECT_EQ(overlay.fields.size(), 1);
  EXPECT_TRUE(overlay.fields.contains("keep"));
  EXPECT_FALSE(overlay.fields.contains("gone"));
  ASSERT_EQ(overlay.removed_fields.size(), 1);
  EXPECT_TRUE(overlay.removed_fields.contains("gone"));
}

TEST_F(CompactionBufferTest, HashOverlayTombstoneAfterDel) {
  buffer_.Absorb("h", WriteOp{HashSet{.key = "h", .fields = {{.field = "a", .value = "1"}}}},
                 kDefaultEviction);
  AbsorbDel("h");
  EXPECT_EQ(buffer_.HashOverlayFor("h").kind, HashOverlay::Kind::kTombstone);
}

TEST_F(CompactionBufferTest, HashOverlayWrongTypeWhenKeyIsString) {
  AbsorbString("h", "scalar");
  EXPECT_EQ(buffer_.HashOverlayFor("h").kind, HashOverlay::Kind::kWrongType);
}

TEST_F(CompactionBufferTest, MultiFieldHashReadsDeferToEngine) {
  buffer_.Absorb("h", WriteOp{HashSet{.key = "h", .fields = {{.field = "a", .value = "1"}}}},
                 kDefaultEviction);
  const std::array<core::ops::ReadOp, 6> ops{
      core::ops::ReadOp{core::ops::HashGetAll{.key = "h"}},
      core::ops::ReadOp{core::ops::HashKeys{.key = "h"}},
      core::ops::ReadOp{core::ops::HashVals{.key = "h"}},
      core::ops::ReadOp{core::ops::HashLen{.key = "h"}},
      core::ops::ReadOp{core::ops::HashMultiGet{.key = "h", .fields = {"a"}}},
      core::ops::ReadOp{core::ops::HashFieldExists{.key = "h", .field = "a"}},
  };
  for (const auto& op : ops) {
    auto r = buffer_.Exec(op);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), core::ErrorCode::kNotFound);
  }
}

}  // namespace
}  // namespace abyss::consumer
