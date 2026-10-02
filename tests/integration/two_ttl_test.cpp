#include <gtest/gtest.h>

#include <chrono>
#include <initializer_list>
#include <string>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "integration_harness.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

// Issue #74: two-TTL model end-to-end. Eviction moves tier; absolute TTL
// deletes entirely. The integration tier proves the property with TestClock
// so the assertions are deterministic and sub-second; the system tier
// (tests/system/two_ttl_test.cpp) re-proves the property against the live
// server binary over TCP.
//
// Defence-in-depth split:
//   unit  — SingleShardStore::EvictExpired attributes by_deadline vs by_ttl;
//           cold lazy-expiry bumps kTtlExpiredTotal{cold} on read.
//   integration (this file) — the engine routes correctly through the queue,
//           hot store, compaction buffer, and cold store for each scenario,
//           for strings + every collection that has a promotion-or-read path.
//   system — same scenarios end-to-end through the abyss-server binary.
class TwoTtlIntegrationTest : public ::testing::Test {
 protected:
  static testing::IntegrationHarness::Config MakeConfig() {
    testing::IntegrationHarness::Config c;
    // Default eviction long enough that scenarios 2 (TTL-only) never trip it.
    // ev_short: holds the short eviction used by scenarios 1 and 3.
    c.eviction_policy = core::EvictionPolicy{
        core::EvictionTTL{86400},
        {{.prefix = "ev_short:", .eviction = core::EvictionTTL{2}}},
    };
    return c;
  }

  static core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }

  // Drives the per-shard cold consumer until its drained_seq matches hot's
  // settled seq and flushes the compaction buffer to RocksDB. Without the
  // flush, the post-eviction read could be served from the buffer instead of
  // cold; with the flush, only cold's RocksDB-backed path can answer.
  void DrainAndFlushCold(std::string_view key) {
    const auto shard = core::ComputeShard(key, testing::IntegrationHarness::kShardCount);
    auto& c = harness_.ColdPool().ConsumerFor(shard);
    c.Drain();
    c.FlushUnscheduled();
  }

  uint64_t WallMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               harness_.Clock().WallNow().time_since_epoch())
        .count();
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_{MakeConfig()};
};

// --- Scenario 1: eviction-only -- strings & collections --------------------

TEST_F(TwoTtlIntegrationTest, S1_EvictionMovesStringToCold) {
  ASSERT_TRUE(
      harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "ev_short:s", "v"})).has_value());
  auto hot = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "ev_short:s"}));
  ASSERT_TRUE(hot.has_value()) << hot.error().message();
  EXPECT_EQ(hot->AsString(), "v");

  DrainAndFlushCold("ev_short:s");

  harness_.Clock().Advance(3s);  // past eviction deadline (2s); no TTL set.
  const auto report = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  EXPECT_EQ(report.by_deadline, 1U);
  EXPECT_EQ(report.by_ttl, 0U) << "eviction-only scenario must attribute to deadline";

  auto after = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "ev_short:s"}));
  ASSERT_TRUE(after.has_value()) << after.error().message();
  EXPECT_EQ(after->AsString(), "v") << "evicted-from-hot value must still read from cold";
}

TEST_F(TwoTtlIntegrationTest, S1_EvictionMovesSetToCold) {
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("SADD", MakeCmd({"SADD", "ev_short:set", "a", "b", "c"}))
                  .has_value());
  EXPECT_EQ(
      harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "ev_short:set"}))->AsInteger(), 3);

  DrainAndFlushCold("ev_short:set");
  harness_.Clock().Advance(3s);
  const auto report = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  EXPECT_EQ(report.by_deadline, 1U);
  EXPECT_EQ(report.by_ttl, 0U);

  auto card = harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "ev_short:set"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 3) << "evicted set must still report cardinality from cold";
  EXPECT_EQ(harness_.Engine()
                .DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "ev_short:set", "a"}))
                ->AsInteger(),
            1);
}

TEST_F(TwoTtlIntegrationTest, S1_EvictionMovesHashToCold) {
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("HSET", MakeCmd({"HSET", "ev_short:h", "f1", "v1", "f2", "v2"}))
                  .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("HLEN", MakeCmd({"HLEN", "ev_short:h"}))->AsInteger(),
            2);

  DrainAndFlushCold("ev_short:h");
  harness_.Clock().Advance(3s);
  const auto report = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  EXPECT_EQ(report.by_deadline, 1U);
  EXPECT_EQ(report.by_ttl, 0U);

  // HGET routes through the hash-overlay path; cold answers when the buffer
  // overlay is kNotPresent (after flush).
  auto v1 = harness_.Engine().DispatchRead("HGET", MakeCmd({"HGET", "ev_short:h", "f1"}));
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(v1->AsString(), "v1");
}

TEST_F(TwoTtlIntegrationTest, S1_EvictionMovesZsetToCold) {
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("ZADD", MakeCmd({"ZADD", "ev_short:z", "1", "x", "2.5", "y"}))
                  .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("ZCARD", MakeCmd({"ZCARD", "ev_short:z"}))->AsInteger(),
            2);

  DrainAndFlushCold("ev_short:z");
  harness_.Clock().Advance(3s);
  const auto report = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  EXPECT_EQ(report.by_deadline, 1U);
  EXPECT_EQ(report.by_ttl, 0U);

  auto card = harness_.Engine().DispatchRead("ZCARD", MakeCmd({"ZCARD", "ev_short:z"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 2);
  auto score = harness_.Engine().DispatchRead("ZSCORE", MakeCmd({"ZSCORE", "ev_short:z", "y"}));
  ASSERT_TRUE(score.has_value());
  EXPECT_EQ(std::stod(score->AsString()), 2.5);
}

// --- Scenario 2: absolute TTL -- strings & collections ---------------------

TEST_F(TwoTtlIntegrationTest, S2_AbsoluteTtlDeletesStringFromAllTiers) {
  const auto ttl_ms = WallMs() + 3000;
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("SET", MakeCmd({"SET", "s2", "v", "PXAT", std::to_string(ttl_ms)}))
                  .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "s2"}))->AsString(), "v");

  DrainAndFlushCold("s2");
  harness_.Clock().Advance(4s);  // past TTL; well below 24h default eviction.

  // The eviction worker would NOT remove this on time-only grounds; the
  // deadline is 24h. But FindLiveEntry strips TTL-expired entries on read,
  // so the GET still observes nil — and the engine's miss path increments
  // kMisses, not kHits.
  auto after = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "s2"}));
  ASSERT_TRUE(after.has_value()) << after.error().message();
  EXPECT_TRUE(after->IsNull()) << "absolute TTL must delete from all tiers";
}

TEST_F(TwoTtlIntegrationTest, S2_AbsoluteTtlDeletesSetFromAllTiers) {
  ASSERT_TRUE(
      harness_.Engine().DispatchWrite("SADD", MakeCmd({"SADD", "s2set", "a", "b"})).has_value());
  const auto ttl_ms = WallMs() + 3000;
  ASSERT_TRUE(
      harness_.Engine()
          .DispatchWrite("PEXPIREAT", MakeCmd({"PEXPIREAT", "s2set", std::to_string(ttl_ms)}))
          .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "s2set"}))->AsInteger(), 2);

  DrainAndFlushCold("s2set");
  harness_.Clock().Advance(4s);

  // SCARD on the expired set: hot's FindLiveEntry strips it; cold's
  // ReadMetaIfLive lazy-expires the meta and SetCard returns Integer(0).
  auto card = harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "s2set"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 0) << "expired set should report zero cardinality";
  auto ismember = harness_.Engine().DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "s2set", "a"}));
  ASSERT_TRUE(ismember.has_value());
  EXPECT_EQ(ismember->AsInteger(), 0);
}

TEST_F(TwoTtlIntegrationTest, S2_AbsoluteTtlDeletesHashFromAllTiers) {
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("HSET", MakeCmd({"HSET", "s2h", "f1", "v1", "f2", "v2"}))
                  .has_value());
  const auto ttl_ms = WallMs() + 3000;
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("PEXPIREAT", MakeCmd({"PEXPIREAT", "s2h", std::to_string(ttl_ms)}))
                  .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("HLEN", MakeCmd({"HLEN", "s2h"}))->AsInteger(), 2);

  DrainAndFlushCold("s2h");
  harness_.Clock().Advance(4s);

  auto v = harness_.Engine().DispatchRead("HGET", MakeCmd({"HGET", "s2h", "f1"}));
  ASSERT_TRUE(v.has_value());
  EXPECT_TRUE(v->IsNull()) << "HGET on expired hash should be nil";
  auto len = harness_.Engine().DispatchRead("HLEN", MakeCmd({"HLEN", "s2h"}));
  ASSERT_TRUE(len.has_value());
  EXPECT_EQ(len->AsInteger(), 0);
}

TEST_F(TwoTtlIntegrationTest, S2_AbsoluteTtlDeletesZsetFromAllTiers) {
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("ZADD", MakeCmd({"ZADD", "s2z", "1", "x", "2", "y"}))
                  .has_value());
  const auto ttl_ms = WallMs() + 3000;
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("PEXPIREAT", MakeCmd({"PEXPIREAT", "s2z", std::to_string(ttl_ms)}))
                  .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("ZCARD", MakeCmd({"ZCARD", "s2z"}))->AsInteger(), 2);

  DrainAndFlushCold("s2z");
  harness_.Clock().Advance(4s);

  auto card = harness_.Engine().DispatchRead("ZCARD", MakeCmd({"ZCARD", "s2z"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 0);
  auto score = harness_.Engine().DispatchRead("ZSCORE", MakeCmd({"ZSCORE", "s2z", "x"}));
  ASSERT_TRUE(score.has_value());
  EXPECT_TRUE(score->IsNull());
}

// --- Scenario 3: combined eviction + TTL -----------------------------------

TEST_F(TwoTtlIntegrationTest, S3_StringEvictsThenTtlExpires) {
  const auto ttl_ms = WallMs() + 5000;
  ASSERT_TRUE(
      harness_.Engine()
          .DispatchWrite("SET", MakeCmd({"SET", "ev_short:k", "v", "PXAT", std::to_string(ttl_ms)}))
          .has_value());
  EXPECT_EQ(harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "ev_short:k"}))->AsString(), "v");

  DrainAndFlushCold("ev_short:k");

  // t=3s: past eviction (2s), before TTL (5s).
  harness_.Clock().Advance(3s);
  const auto e1 = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  EXPECT_EQ(e1.by_deadline, 1U) << "eviction fires first";
  EXPECT_EQ(e1.by_ttl, 0U);

  auto r2 = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "ev_short:k"}));
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->AsString(), "v") << "evicted key still readable from cold";

  // Drain the promotion's queue entry through to cold so the next eviction
  // pass can find any promoted hot residue. (Strings have a promotion path;
  // collections currently do not — see RocksdbStore::GetPromotionCommand.)
  DrainAndFlushCold("ev_short:k");

  // Advance further. Total wall = 6s, past the 5s TTL. Total steady = 6s,
  // past the eviction window (refreshed by the promotion-induced re-apply
  // in hot, but TTL is unmoved).
  harness_.Clock().Advance(3s);

  auto r3 = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "ev_short:k"}));
  ASSERT_TRUE(r3.has_value());
  EXPECT_TRUE(r3->IsNull()) << "TTL must take the key entirely after promotion+TTL elapsed";
}

TEST_F(TwoTtlIntegrationTest, S3_SetEvictsThenTtlExpires) {
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("SADD", MakeCmd({"SADD", "ev_short:s3set", "a", "b"}))
                  .has_value());
  const auto ttl_ms = WallMs() + 5000;
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("PEXPIREAT",
                                 MakeCmd({"PEXPIREAT", "ev_short:s3set", std::to_string(ttl_ms)}))
                  .has_value());
  DrainAndFlushCold("ev_short:s3set");

  harness_.Clock().Advance(3s);
  const auto e1 = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  EXPECT_EQ(e1.by_deadline, 1U);
  EXPECT_EQ(e1.by_ttl, 0U);

  EXPECT_EQ(
      harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "ev_short:s3set"}))->AsInteger(), 2)
      << "evicted set still readable from cold pre-TTL";

  harness_.Clock().Advance(3s);

  EXPECT_EQ(
      harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "ev_short:s3set"}))->AsInteger(), 0)
      << "TTL must remove the set entirely";
}

// --- COLDC-2: EXPIRE/PERSIST-only window reaches cold ----------------------
// A TTL set/cleared in a window AFTER the value has already flushed to cold was
// silently dropped pre-fix (Emit produced nothing). The standalone trailing TTL
// op must now reach cold's meta record.

TEST_F(TwoTtlIntegrationTest, COLDC2_StandaloneExpireOnAlreadyColdSetReachesCold) {
  // Window 1: build the set and flush it to cold with NO TTL.
  ASSERT_TRUE(
      harness_.Engine().DispatchWrite("SADD", MakeCmd({"SADD", "c2set", "a", "b"})).has_value());
  DrainAndFlushCold("c2set");
  EXPECT_EQ(harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "c2set"}))->AsInteger(), 2);

  // Window 2 (a fresh compaction window): EXPIRE only — no member-bearing op.
  const auto ttl_ms = WallMs() + 3000;
  ASSERT_TRUE(
      harness_.Engine()
          .DispatchWrite("PEXPIREAT", MakeCmd({"PEXPIREAT", "c2set", std::to_string(ttl_ms)}))
          .has_value());
  DrainAndFlushCold("c2set");

  // The standalone Expire must have rewritten cold's meta TTL: past the TTL the
  // cold lazy-expiry strips the set entirely.
  harness_.Clock().Advance(4s);
  auto card = harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "c2set"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 0)
      << "standalone EXPIRE window was dropped; cold never saw the TTL (COLDC-2)";
}

TEST_F(TwoTtlIntegrationTest, COLDC2_StandalonePersistOnAlreadyColdSetClearsTtl) {
  // Window 1: build a set WITH a TTL and flush to cold.
  ASSERT_TRUE(
      harness_.Engine().DispatchWrite("SADD", MakeCmd({"SADD", "c2p", "a", "b"})).has_value());
  const auto ttl_ms = WallMs() + 3000;
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("PEXPIREAT", MakeCmd({"PEXPIREAT", "c2p", std::to_string(ttl_ms)}))
                  .has_value());
  DrainAndFlushCold("c2p");

  // Window 2: PERSIST only — clears the TTL on the already-cold set.
  ASSERT_TRUE(harness_.Engine().DispatchWrite("PERSIST", MakeCmd({"PERSIST", "c2p"})).has_value());
  DrainAndFlushCold("c2p");

  // Past the original TTL the set must still be live — the PERSIST window
  // reached cold and cleared the meta TTL flag.
  harness_.Clock().Advance(4s);
  auto card = harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "c2p"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 2)
      << "standalone PERSIST window was dropped; cold kept the stale TTL (COLDC-2)";
}

// --- COLDC-3: DEL/type-change before re-add wipes prior cold slices --------

TEST_F(TwoTtlIntegrationTest, COLDC3_DelThenReaddDoesNotResurrectColdSetMembers) {
  // Window 1: set {a,b,c} flushed to cold.
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("SADD", MakeCmd({"SADD", "c3set", "a", "b", "c"}))
                  .has_value());
  DrainAndFlushCold("c3set");
  EXPECT_EQ(harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "c3set"}))->AsInteger(), 3);

  // Window 2: DEL then re-add a single different member. Emit prepends a Del so
  // cold wipes a/b/c before the re-add lands.
  ASSERT_TRUE(harness_.Engine().DispatchWrite("DEL", MakeCmd({"DEL", "c3set"})).has_value());
  ASSERT_TRUE(harness_.Engine().DispatchWrite("SADD", MakeCmd({"SADD", "c3set", "x"})).has_value());
  DrainAndFlushCold("c3set");

  // Drive the key out of hot so SCARD/SISMEMBER resolve against cold.
  for (core::ShardId shard = 0; shard < harness_.ShardedHot().shard_count(); ++shard) {
    ASSERT_TRUE(harness_.ShardedHot().Wipe(shard).has_value());
  }

  EXPECT_EQ(harness_.Engine().DispatchRead("SCARD", MakeCmd({"SCARD", "c3set"}))->AsInteger(), 1)
      << "DEL-then-readd resurrected stale cold members (COLDC-3)";
  EXPECT_EQ(harness_.Engine()
                .DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "c3set", "a"}))
                ->AsInteger(),
            0);
  EXPECT_EQ(harness_.Engine()
                .DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "c3set", "x"}))
                ->AsInteger(),
            1);
}

TEST_F(TwoTtlIntegrationTest, COLDC3_WithinWindowTypeChangeDropsPriorSlices) {
  // A within-window type change (HSET then SET before any flush) is the
  // COLDC-3 case the leading-Del covers: Emit prepends a Del so cold never
  // receives the stale hash slices in the first place.
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("HSET", MakeCmd({"HSET", "c3t", "f1", "v1", "f2", "v2"}))
                  .has_value());
  // Same compaction window — no DrainAndFlushCold between the two writes.
  ASSERT_TRUE(
      harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "c3t", "now-a-string"})).has_value());
  DrainAndFlushCold("c3t");

  for (core::ShardId shard = 0; shard < harness_.ShardedHot().shard_count(); ++shard) {
    ASSERT_TRUE(harness_.ShardedHot().Wipe(shard).has_value());
  }

  auto getv = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "c3t"}));
  ASSERT_TRUE(getv.has_value());
  EXPECT_EQ(getv->AsString(), "now-a-string");

  // The prior hash fields must not survive the type change. The key is now a
  // string, so HLEN is EITHER WRONGTYPE (an error Result, when a type-aware tier
  // — the compaction buffer — resolves it) OR integer 0 (when the cold tier,
  // which has no hash slice, resolves it). Which one wins depends on whether the
  // buffer has drained to cold yet, so accept both. The ONLY failure is a
  // positive field count, which would mean the stale hash slices survived.
  auto hlen = harness_.Engine().DispatchRead("HLEN", MakeCmd({"HLEN", "c3t"}));
  if (hlen.has_value()) {
    EXPECT_TRUE(hlen->IsInteger() && hlen->AsInteger() == 0)
        << "stale hash fields survived the type change";
  } else {
    EXPECT_EQ(hlen.error().code(), core::ErrorCode::kWrongType)
        << "unexpected HLEN error: " << hlen.error().message();
  }
}

// An absolute TTL that lapses while the write is still buffered must
// delete the key from cold, or the older cold value resurfaces.
TEST_F(TwoTtlIntegrationTest, ExpiredBufferedWriteDeletesTheOlderColdValue) {
  ASSERT_TRUE(harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "rk", "old"})).has_value());
  DrainAndFlushCold("rk");

  const uint64_t ttl_ms = WallMs() + 1000;
  ASSERT_TRUE(
      harness_.Engine()
          .DispatchWrite("SET", MakeCmd({"SET", "rk", "new", "PXAT", std::to_string(ttl_ms)}))
          .has_value());
  harness_.Clock().Advance(2s);
  DrainAndFlushCold("rk");
  for (core::ShardId shard = 0; shard < harness_.ShardedHot().shard_count(); ++shard) {
    ASSERT_TRUE(harness_.ShardedHot().Wipe(shard).has_value());
  }

  auto got = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "rk"}));
  ASSERT_TRUE(got.has_value());
  EXPECT_TRUE(got->IsNull()) << "expired write resurrected the older cold value";
}

// --- COLDC-6: cross-window implicit type change drops prior cold slices -----

TEST_F(TwoTtlIntegrationTest, COLDC6_CrossWindowTypeChangeDropsPriorSlices) {
  // The COLDC-6 case the leading-Del does NOT cover: the type change spans two
  // compaction windows. Window 1 flushes the hash to cold; window 2's
  // CompactedState starts fresh and emits a SET with no leading Del — it has no
  // in-window signal that cold already holds a hash for the key. The cold apply
  // must drop the stale hash slices when it establishes the key as a string, or
  // the hash resurrects on read.

  // Window 1: hash flushed to cold on its own.
  ASSERT_TRUE(harness_.Engine()
                  .DispatchWrite("HSET", MakeCmd({"HSET", "c6t", "f1", "v1", "f2", "v2"}))
                  .has_value());
  DrainAndFlushCold("c6t");
  EXPECT_EQ(harness_.Engine().DispatchRead("HLEN", MakeCmd({"HLEN", "c6t"}))->AsInteger(), 2);

  // Window 2 (separate flush): SET the same key to a string.
  ASSERT_TRUE(
      harness_.Engine().DispatchWrite("SET", MakeCmd({"SET", "c6t", "now-a-string"})).has_value());
  DrainAndFlushCold("c6t");

  // Drive the key out of hot so the reads resolve against the buffer/cold tiers.
  for (core::ShardId shard = 0; shard < harness_.ShardedHot().shard_count(); ++shard) {
    ASSERT_TRUE(harness_.ShardedHot().Wipe(shard).has_value());
  }

  auto getv = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "c6t"}));
  ASSERT_TRUE(getv.has_value());
  EXPECT_EQ(getv->AsString(), "now-a-string");

  // The prior hash fields must not survive the cross-window type change. The key
  // is now a string, so HLEN is EITHER WRONGTYPE (a type-aware tier resolves it)
  // OR integer 0 (a tier with no hash slice resolves it). A positive field count
  // is the failure — it means the stale hash slices survived (the COLDC-6 bug).
  auto hlen = harness_.Engine().DispatchRead("HLEN", MakeCmd({"HLEN", "c6t"}));
  if (hlen.has_value()) {
    EXPECT_TRUE(hlen->IsInteger() && hlen->AsInteger() == 0)
        << "stale hash fields survived the cross-window type change (COLDC-6)";
  } else {
    EXPECT_EQ(hlen.error().code(), core::ErrorCode::kWrongType)
        << "unexpected HLEN error: " << hlen.error().message();
  }

  auto hgetall = harness_.Engine().DispatchRead("HGETALL", MakeCmd({"HGETALL", "c6t"}));
  if (hgetall.has_value()) {
    EXPECT_TRUE(hgetall->IsArray() && hgetall->AsArray().empty())
        << "HGETALL resurrected stale hash fields after cross-window type change (COLDC-6)";
  } else {
    EXPECT_EQ(hgetall.error().code(), core::ErrorCode::kWrongType);
  }
}

}  // namespace
}  // namespace abyss::engine
