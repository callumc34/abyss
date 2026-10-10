#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "latch.h"
#include "on_exit.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;
namespace ops = core::ops;

constexpr core::EvictionTTL kShortEviction{1};
constexpr core::EvictionTTL kLongEviction{86400};

int64_t WallMs(const abyss::testing::TestClock& clock) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(clock.WallNow().time_since_epoch())
      .count();
}

// A load token, failing the test if none is granted.
LoadToken MustBeginLoad(SingleShardStore& store, std::string_view key) {
  const auto token = store.BeginLoad(key);
  EXPECT_TRUE(token.has_value()) << "no load token for " << key;
  return token.value_or(LoadToken{});
}

LoadToken MustBeginLoad(ShardedHotStore& store, std::string_view key) {
  const LoadStart start = store.BeginLoad(key);
  EXPECT_TRUE(start.started()) << "no load token for " << key;
  return start.token_if_started().value_or(LoadToken{});
}

class ResidencyTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  SingleShardStore store_{SingleShardConfig{
      .max_memory_bytes = 1024UL * 1024,
      .stub_max_entries = 4,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  void Write(const ops::WriteOp& op, core::SequenceId seq,
             core::EvictionTTL eviction = kShortEviction) {
    auto r = store_.Apply(op, eviction, seq);
    ASSERT_TRUE(r.has_value()) << r.error().message();
  }

  void Set(std::string_view key, core::SequenceId seq, uint64_t abs_ttl_ms = 0) {
    Write(ops::StringSet{.key = key, .value = "v", .abs_ttl_ms = abs_ttl_ms}, seq);
  }

  bool Resident(std::string_view key) const {
    return store_.Exec(ops::ReadOp{ops::Exists{.keys = {key}}})->AsInteger() == 1;
  }

  // Moves past kShortEviction so every entry's deadline has elapsed.
  SingleShardStore::EvictExpiredReport EvictAfterDeadline(core::SequenceId horizon) {
    clock_.Advance(2s);
    return store_.EvictExpired(clock_.SteadyNow(), horizon);
  }

  static LoadResult LoadedString(std::string value) { return MakeLoadedFull(std::move(value), 0); }
};

// --- latest_seq ---

struct ApplyKindCase {
  std::string name;
  std::vector<ops::WriteOp> setup;
  ops::WriteOp op;
  // The op leaves a tombstone rather than a live entry.
  bool tombstones = false;
};

std::vector<ApplyKindCase> ApplyKindCases() {
  const auto set = [](std::string_view member) {
    return ops::WriteOp{ops::SetAdd{.key = "k", .members = {member, "other"}}};
  };
  const auto zset = ops::WriteOp{ops::ZsetAdd{
      .key = "k", .entries = {{.score = 1, .member = "m"}, {.score = 2, .member = "n"}}}};
  const auto hash = ops::WriteOp{ops::HashSet{
      .key = "k", .fields = {{.field = "f", .value = "v"}, {.field = "g", .value = "v"}}}};
  const auto str = ops::WriteOp{ops::StringSet{.key = "k", .value = "v"}};
  return {
      {.name = "set", .setup = {}, .op = str},
      {.name = "sadd", .setup = {}, .op = set("m")},
      {.name = "srem", .setup = {set("m")}, .op = ops::SetRem{.key = "k", .members = {"m"}}},
      {.name = "zadd", .setup = {}, .op = zset},
      {.name = "zrem", .setup = {zset}, .op = ops::ZsetRem{.key = "k", .members = {"m"}}},
      {.name = "hset", .setup = {}, .op = hash},
      {.name = "hmset",
       .setup = {},
       .op = ops::HashMSet{.key = "k", .fields = {{.field = "f", .value = "v"}}}},
      {.name = "hdel", .setup = {hash}, .op = ops::HashDel{.key = "k", .fields = {"f"}}},
      {.name = "expire",
       .setup = {str},
       .op = ops::Expire{.key = "k", .abs_ttl_ms = uint64_t{1} << 50}},
      {.name = "persist", .setup = {str}, .op = ops::Persist{.key = "k"}},
      {.name = "del", .setup = {str}, .op = ops::Del{.keys = {"k"}}, .tombstones = true},
      {.name = "srem_last",
       .setup = {set("m")},
       .op = ops::SetRem{.key = "k", .members = {"m", "other"}},
       .tombstones = true},
      {.name = "zrem_last",
       .setup = {zset},
       .op = ops::ZsetRem{.key = "k", .members = {"m", "n"}},
       .tombstones = true},
      {.name = "hdel_last",
       .setup = {hash},
       .op = ops::HashDel{.key = "k", .fields = {"f", "g"}},
       .tombstones = true},
  };
}

TEST_F(ResidencyTest, EveryApplyKindStampsLatestSeq) {
  for (const auto& c : ApplyKindCases()) {
    SCOPED_TRACE(c.name);
    store_.Wipe(core::kFirstSeq);
    for (const auto& op : c.setup) Write(op, 1);
    Write(c.op, 5);
    if (c.tombstones) {
      ASSERT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
      EXPECT_EQ(store_.GcTombstones(4), 0U) << "delete at 5 is not drained at 4";
      EXPECT_EQ(store_.GcTombstones(5), 1U);
      continue;
    }
    ASSERT_TRUE(Resident("k"));
    EXPECT_EQ(EvictAfterDeadline(4).Total(), 0U) << "write at 5 is not drained at 4";
    EXPECT_TRUE(Resident("k"));
    EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow(), 5).by_deadline, 1U);
    EXPECT_FALSE(Resident("k"));
  }
}

// --- Removal gated on cold drain ---

TEST_F(ResidencyTest, DeadlineEvictionWaitsForDrain) {
  Set("k", 3);
  EXPECT_EQ(EvictAfterDeadline(2).Total(), 0U);
  EXPECT_TRUE(Resident("k"));
  EXPECT_GT(store_.Stats().unevictable_bytes, 0U);

  const auto report = store_.EvictExpired(clock_.SteadyNow(), 3);
  EXPECT_EQ(report.by_deadline, 1U);
  EXPECT_FALSE(Resident("k"));
  EXPECT_EQ(store_.Stats().unevictable_bytes, 0U);
}

TEST_F(ResidencyTest, ExpiredButUndrainedEntryStays) {
  // A drained value without a TTL, then an undrained one that expires:
  // removing it would let a miss read the older value from cold.
  Set("k", 1);
  Set("k", 2, static_cast<uint64_t>(WallMs(clock_) + 500));
  clock_.Advance(1s);

  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow(), 1).Total(), 0U);
  EXPECT_EQ(store_.Stats().key_count, 1U) << "the expired entry is still held";
  EXPECT_FALSE(Resident("k")) << "and reads treat it as absent";
  auto get = store_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_FALSE(get.has_value());
  EXPECT_EQ(get.error().code(), core::ErrorCode::kNotFound);

  const auto report = store_.EvictExpired(clock_.SteadyNow(), 2);
  EXPECT_EQ(report.by_ttl, 1U);
  EXPECT_EQ(store_.Stats().key_count, 0U);
  EXPECT_EQ(store_.FindStub("k"), nullptr) << "a key gone by TTL leaves no stub";
}

TEST_F(ResidencyTest, ApplyOnExpiredKeyKeepsItUntilDrained) {
  Set("a", 1, static_cast<uint64_t>(WallMs(clock_) + 500));
  Set("b", 1, static_cast<uint64_t>(WallMs(clock_) + 500));
  Set("c", 1, static_cast<uint64_t>(WallMs(clock_) + 500));
  clock_.Advance(1s);
  Write(ops::Del{.keys = {"a"}}, 4);
  Write(ops::Expire{.key = "b", .abs_ttl_ms = uint64_t{1} << 50}, 4);
  Write(ops::Persist{.key = "c"}, 4);
  EXPECT_EQ(store_.Probe("a"), core::HotKeyPresence::kTombstoned) << "a DEL tombstones it";
  EXPECT_EQ(store_.Stats().key_count, 2U);
  EXPECT_EQ(store_.Stats().expired_count, 1U);

  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow(), 3).Total(), 0U);
  EXPECT_EQ(store_.GcTombstones(3), 0U);
  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow(), 4).by_ttl, 2U);
  EXPECT_EQ(store_.GcTombstones(4), 1U);
}

TEST_F(ResidencyTest, LruEvictionWaitsForDrain) {
  Set("a", 1);
  clock_.Advance(10ms);
  Set("b", 2);

  EXPECT_EQ(store_.EvictLru(0, 0), 0U);
  EXPECT_TRUE(Resident("a"));
  EXPECT_TRUE(Resident("b"));
  EXPECT_GT(store_.Stats().unevictable_bytes, 0U);

  EXPECT_EQ(store_.EvictLru(0, 1), 1U);
  EXPECT_FALSE(Resident("a"));
  EXPECT_TRUE(Resident("b"));

  EXPECT_EQ(store_.EvictLru(0, 2), 1U);
  EXPECT_FALSE(Resident("b"));
}

uint64_t MeasureStringBytes(std::string_view key, const std::string& value) {
  SingleShardStore probe{SingleShardConfig{}};
  EXPECT_TRUE(probe.Apply(ops::WriteOp{ops::StringSet{.key = key, .value = value}}, kLongEviction)
                  .has_value());
  return probe.Stats().used_bytes;
}

TEST(ResidencyMemoryTest, ApplyMakesRoomOnlyFromDrainedKeys) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  const uint64_t per_entry = MeasureStringBytes("a", value);
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = (per_entry * 2) + (per_entry / 2),
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto write = [&](std::string_view key, core::SequenceId seq, core::SequenceId horizon) {
    clock.Advance(10ms);
    return store.Apply(ops::WriteOp{ops::StringSet{.key = key, .value = value}}, kLongEviction, seq,
                       horizon);
  };
  const auto resident = [&](std::string_view key) {
    return store.Exec(ops::ReadOp{ops::StringGet{.key = key}}).has_value();
  };

  ASSERT_TRUE(write("a", 1, 0).has_value());
  ASSERT_TRUE(write("b", 2, 0).has_value());
  // Over budget with nothing drained: held, and not this write's failure.
  auto c = write("c", 3, 0);
  ASSERT_TRUE(c.has_value()) << c.error().message();
  EXPECT_TRUE(resident("a"));
  EXPECT_GT(store.Stats().used_bytes, store.Stats().max_bytes);

  // Once a drains, the next write evicts it and nothing newer.
  ASSERT_TRUE(write("d", 4, 1).has_value());
  EXPECT_FALSE(resident("a"));
  EXPECT_TRUE(resident("b"));
  EXPECT_TRUE(resident("c"));
  EXPECT_TRUE(resident("d"));
}

TEST(ResidencyMemoryTest, ValueLargerThanBudgetStillExhaustsWhenUndrained) {
  abyss::testing::TestClock clock;
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = MeasureStringBytes("k", "v"),
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  auto r = store.Apply(ops::WriteOp{ops::StringSet{.key = "big", .value = std::string(4096, 'x')}},
                       kLongEviction, 1, 0);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kResourceExhausted);
}

// --- Stubs ---

TEST_F(ResidencyTest, EvictionLeavesStubWithKeyMetadata) {
  const auto ttl = static_cast<uint64_t>(WallMs(clock_) + 60000);
  Write(ops::SetAdd{.key = "s", .members = {"m"}}, 3);
  Set("k", 4, ttl);
  EXPECT_EQ(EvictAfterDeadline(kAllDrained).by_deadline, 2U);

  const Stub* set_stub = store_.FindStub("s");
  ASSERT_NE(set_stub, nullptr);
  EXPECT_EQ(set_stub->type, Entry::Type::kSet);
  EXPECT_EQ(set_stub->abs_ttl_ms, 0);
  EXPECT_EQ(set_stub->latest_seq, 3U);

  const Stub* str_stub = store_.FindStub("k");
  ASSERT_NE(str_stub, nullptr);
  EXPECT_EQ(str_stub->type, Entry::Type::kString);
  EXPECT_EQ(str_stub->abs_ttl_ms, static_cast<int64_t>(ttl));
  EXPECT_EQ(str_stub->latest_seq, 4U);
}

TEST_F(ResidencyTest, LruEvictionLeavesStub) {
  Set("k", 1);
  EXPECT_EQ(store_.EvictLru(0, kAllDrained), 1U);
  ASSERT_NE(store_.FindStub("k"), nullptr);
  EXPECT_EQ(store_.Stats().stub_entries, 1U);
}

TEST_F(ResidencyTest, StubCacheIsBoundedAndDropsOldest) {
  // One eviction pass per key fixes the stubs' order.
  const auto set_and_evict = [&](std::string_view key) {
    Set(key, 1);
    EXPECT_EQ(EvictAfterDeadline(kAllDrained).by_deadline, 1U);
  };
  for (const auto* key : {"a", "b", "c", "d"}) set_and_evict(key);
  ASSERT_EQ(store_.Stats().stub_entries, 4U);
  set_and_evict("e");
  set_and_evict("f");

  const auto stats = store_.Stats();
  EXPECT_EQ(stats.stub_entries, 4U) << "bounded at the cap";
  EXPECT_EQ(stats.stub_drops, 2U);
  EXPECT_EQ(store_.FindStub("a"), nullptr);
  EXPECT_EQ(store_.FindStub("b"), nullptr);
  EXPECT_NE(store_.FindStub("c"), nullptr);
  EXPECT_NE(store_.FindStub("d"), nullptr);
  EXPECT_NE(store_.FindStub("e"), nullptr);
  EXPECT_NE(store_.FindStub("f"), nullptr);
}

TEST_F(ResidencyTest, ZeroCapacityKeepsNoStubs) {
  SingleShardStore store{SingleShardConfig{.steady_clock = clock_.SteadyFn()}};
  ASSERT_TRUE(store.Apply(ops::WriteOp{ops::StringSet{.key = "k", .value = "v"}}, kShortEviction, 1)
                  .has_value());
  clock_.Advance(2s);
  EXPECT_EQ(store.EvictExpired(clock_.SteadyNow(), kAllDrained).by_deadline, 1U);
  EXPECT_EQ(store.FindStub("k"), nullptr);
  EXPECT_EQ(store.Stats().stub_drops, 0U);
}

TEST_F(ResidencyTest, ApplyRemovesStub) {
  Set("k", 1);
  Set("j", 1);
  EvictAfterDeadline(kAllDrained);
  ASSERT_NE(store_.FindStub("k"), nullptr);
  ASSERT_NE(store_.FindStub("j"), nullptr);

  Set("k", 2);
  EXPECT_EQ(store_.FindStub("k"), nullptr) << "resident again";
  // A write the stub does not reflect makes it stale even when it
  // leaves the key non-resident.
  Write(ops::SetRem{.key = "j", .members = {"m"}}, 2);
  EXPECT_EQ(store_.FindStub("j"), nullptr);
}

TEST_F(ResidencyTest, LoadInstallRemovesStub) {
  Set("k", 1);
  EvictAfterDeadline(kAllDrained);
  ASSERT_NE(store_.FindStub("k"), nullptr);

  const LoadToken token = MustBeginLoad(store_, "k");
  ASSERT_TRUE(store_.CompleteLoad("k", token, LoadedString("v"), kLongEviction, kAllDrained));
  EXPECT_EQ(store_.FindStub("k"), nullptr);
  EXPECT_TRUE(Resident("k"));
}

TEST_F(ResidencyTest, DelOfNonResidentKeyLeavesTombstoneUntilDrained) {
  Set("k", 1);
  EvictAfterDeadline(kAllDrained);
  ASSERT_NE(store_.FindStub("k"), nullptr);

  auto del = store_.Apply(ops::WriteOp{ops::Del{.keys = {"k", "never"}}}, kLongEviction, 5);
  ASSERT_TRUE(del.has_value());
  EXPECT_EQ(del->AsInteger(), 0) << "neither key was resident";
  EXPECT_EQ(store_.FindStub("k"), nullptr) << "the DEL made the stub stale";
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(store_.Probe("never"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(store_.Stats().key_count, 0U);

  EXPECT_EQ(store_.GcTombstones(4), 0U);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(store_.GcTombstones(5), 2U);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);
  EXPECT_EQ(store_.Stats().used_bytes, 0U);
}

TEST_F(ResidencyTest, StubBytesCountTowardUsedBytes) {
  const std::string key = "a-longer-key-than-small-string-capacity";
  Set(key, 1);
  EvictAfterDeadline(kAllDrained);
  const auto stats = store_.Stats();
  EXPECT_EQ(stats.key_count, 0U);
  EXPECT_EQ(stats.stub_bytes, kStubBytes + key.size());
  EXPECT_EQ(stats.used_bytes, stats.stub_bytes);

  ASSERT_TRUE(store_.DropStub(key));
  EXPECT_EQ(store_.Stats().used_bytes, 0U);
  EXPECT_EQ(store_.Stats().stub_drops, 0U) << "an explicit drop is not a cap drop";
}

// --- Load placeholders ---

TEST_F(ResidencyTest, LoadWithExactTokenInstalls) {
  const LoadToken token = MustBeginLoad(store_, "s");
  EXPECT_TRUE(store_.LoadPending("s"));
  SetValue members;
  members.members = {"a", "b"};
  ASSERT_TRUE(
      store_.CompleteLoad("s", token, MakeLoadedFull(members, 0), kLongEviction, kAllDrained));

  EXPECT_FALSE(store_.LoadPending("s"));
  auto card = store_.Exec(ops::ReadOp{ops::SetCard{.key = "s"}});
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 2);
  EXPECT_EQ(store_.Stats().key_count, 1U);
  EXPECT_EQ(store_.Stats().load_discards, 0U);
}

TEST_F(ResidencyTest, InstalledStateIsAlreadyDrained) {
  const LoadToken token = MustBeginLoad(store_, "k");
  ASSERT_TRUE(store_.CompleteLoad("k", token, LoadedString("v"), kShortEviction, kAllDrained));
  EXPECT_EQ(EvictAfterDeadline(0).by_deadline, 1U) << "latest_seq 0 is evictable at any horizon";
}

// Seqs start at kFirstSeq, so a horizon of 0 drains nothing: a shard's
// first write stays through every eviction pass until cold drains it,
// while loaded state, at 0, is evictable at once.
TEST_F(ResidencyTest, AFirstWriteWaitsForItsDrainWhileLoadedStateDoesNot) {
  constexpr core::SequenceId kNothingDrained = 0;
  const LoadToken token = MustBeginLoad(store_, "loaded");
  ASSERT_TRUE(store_.CompleteLoad("loaded", token, LoadedString("v"), kShortEviction, kAllDrained));
  Set("first", core::kFirstSeq);

  EXPECT_EQ(store_.EvictLru(0, kNothingDrained), 1U);
  EXPECT_FALSE(Resident("loaded"));
  EXPECT_TRUE(Resident("first"));
  EXPECT_EQ(EvictAfterDeadline(kNothingDrained).Total(), 0U);
  EXPECT_TRUE(Resident("first"));

  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow(), core::kFirstSeq).by_deadline, 1U);
  EXPECT_FALSE(Resident("first"));
}

TEST_F(ResidencyTest, AbsentLoadInstallsADrainedTombstone) {
  const LoadToken token = MustBeginLoad(store_, "k");
  EXPECT_TRUE(store_.CompleteLoad("k", token, LoadedAbsent{}, kLongEviction, kAllDrained));
  EXPECT_FALSE(store_.LoadPending("k"));
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(store_.Stats().key_count, 0U);
  EXPECT_EQ(store_.Stats().load_discards, 0U);
  EXPECT_EQ(store_.Stats().negative_entries, 1U);
  EXPECT_EQ(store_.GcTombstones(core::kFirstSeq), 0U) << "the negative cache reclaims it";
}

TEST_F(ResidencyTest, BeginLoadNeedsNoEntryAndNoLoad) {
  Set("live", 1);
  EXPECT_FALSE(store_.BeginLoad("live").has_value());
  Write(ops::Del{.keys = {"live"}}, 2);
  EXPECT_FALSE(store_.BeginLoad("live").has_value()) << "a tombstone is resident";

  const LoadToken first = MustBeginLoad(store_, "k");
  EXPECT_FALSE(store_.BeginLoad("k").has_value());
  store_.AbortLoad("k", first);
  const LoadToken second = MustBeginLoad(store_, "k");
  EXPECT_NE(second.id, first.id) << "tokens are unique";
}

TEST_F(ResidencyTest, StaleTokenDiscards) {
  const LoadToken first = MustBeginLoad(store_, "k");
  store_.AbortLoad("k", first);
  const LoadToken second = MustBeginLoad(store_, "k");

  EXPECT_FALSE(store_.CompleteLoad("k", first, LoadedString("stale"), kLongEviction, kAllDrained));
  EXPECT_EQ(store_.Stats().load_discards, 1U);
  EXPECT_TRUE(store_.LoadPending("k")) << "the current load is untouched";
  EXPECT_TRUE(store_.CompleteLoad("k", second, LoadedString("v"), kLongEviction, kAllDrained));
}

TEST_F(ResidencyTest, ApplyDuringLoadDiscardsIt) {
  const LoadToken token = MustBeginLoad(store_, "k");
  Write(ops::StringSet{.key = "k", .value = "written"}, 2);
  EXPECT_FALSE(store_.LoadPending("k"));

  EXPECT_FALSE(store_.CompleteLoad("k", token, LoadedString("stale"), kLongEviction, kAllDrained));
  EXPECT_EQ(store_.Stats().load_discards, 1U);
  auto get = store_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "written");
}

TEST_F(ResidencyTest, WipeDuringLoadDiscardsIt) {
  const LoadToken token = MustBeginLoad(store_, "k");
  store_.Wipe(5);
  EXPECT_FALSE(store_.LoadPending("k"));

  EXPECT_FALSE(
      store_.CompleteLoad("k", token, LoadedString("pre-flush"), kLongEviction, kAllDrained));
  EXPECT_EQ(store_.Stats().load_discards, 1U);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);
}

TEST_F(ResidencyTest, AbortNeedsTheMatchingToken) {
  const LoadToken token = MustBeginLoad(store_, "k");
  store_.AbortLoad("k", LoadToken{.id = token.id + 1});
  EXPECT_TRUE(store_.LoadPending("k"));
  store_.AbortLoad("k", token);
  EXPECT_FALSE(store_.LoadPending("k"));
  EXPECT_EQ(store_.Stats().load_discards, 0U);
}

// --- Load results ---

TEST_F(ResidencyTest, ExistsInstallsAStubReplacingAnyStub) {
  Set("k", 1);
  EvictAfterDeadline(kAllDrained);
  ASSERT_NE(store_.FindStub("k"), nullptr);
  const LoadToken token = MustBeginLoad(store_, "k");
  ASSERT_TRUE(store_.CompleteLoad("k", token,
                                  LoadedExists{.type = Entry::Type::kSet, .abs_ttl_ms = 77},
                                  kLongEviction, kAllDrained));

  const Stub* stub = store_.FindStub("k");
  ASSERT_NE(stub, nullptr);
  EXPECT_EQ(stub->type, Entry::Type::kSet);
  EXPECT_EQ(stub->abs_ttl_ms, 77);
  EXPECT_EQ(stub->latest_seq, 0U);
  EXPECT_EQ(store_.View("k", kAllDrained, 0).presence, KeyView::Presence::kStub);
  EXPECT_EQ(store_.Stats().key_count, 0U);
}

TEST(ResidencyNoStubsTest, ExistsIsNotHeldWithoutStubs) {
  SingleShardStore store{SingleShardConfig{}};
  EXPECT_FALSE(store.RetainsStubs());
  const LoadToken token = MustBeginLoad(store, "k");
  EXPECT_FALSE(store.CompleteLoad("k", token, LoadedExists{}, kLongEviction, kAllDrained));
  EXPECT_FALSE(store.LoadPending("k"));
  EXPECT_EQ(store.View("k", kAllDrained, 0).presence, KeyView::Presence::kNonResident);
}

TEST(ResidencyOvertakenLoadTest, EveryResultDiscardsWhenOvertaken) {
  using Overtake = std::function<void(SingleShardStore&, LoadToken)>;
  const std::vector<std::pair<std::string, LoadResult>> results = {
      {"absent", LoadedAbsent{}},
      {"exists", LoadedExists{.type = Entry::Type::kHash}},
      {"full", MakeLoadedFull(std::string("stale"), 0)},
  };
  const std::vector<std::pair<std::string, Overtake>> overtakes = {
      {"a stale token",
       [](SingleShardStore& store, LoadToken token) {
         store.AbortLoad("k", token);
         ASSERT_TRUE(store.BeginLoad("k").has_value());
       }},
      {"an apply",
       [](SingleShardStore& store, LoadToken /*token*/) {
         ASSERT_TRUE(store
                         .Apply(ops::WriteOp{ops::StringSet{.key = "k", .value = "written"}},
                                kLongEviction, 2)
                         .has_value());
       }},
      {"a wipe", [](SingleShardStore& store, LoadToken /*token*/) { store.Wipe(5); }},
  };
  for (const auto& [result_name, result] : results) {
    for (const auto& [overtake_name, overtake] : overtakes) {
      SCOPED_TRACE(::testing::Message() << result_name << " overtaken by " << overtake_name);
      SingleShardStore store{SingleShardConfig{.stub_max_entries = 4}};
      const LoadToken token = MustBeginLoad(store, "k");
      overtake(store, token);
      const auto keys = store.Stats().key_count;

      LoadResult late = result;
      EXPECT_FALSE(store.CompleteLoad("k", token, std::move(late), kLongEviction, kAllDrained));
      EXPECT_EQ(store.Stats().load_discards, 1U);
      EXPECT_EQ(store.Stats().key_count, keys);
      EXPECT_EQ(store.FindStub("k"), nullptr);
      EXPECT_NE(store.Probe("k"), core::HotKeyPresence::kTombstoned);
    }
  }
}

TEST_F(ResidencyTest, CompleteLoadsInstallsABatchInOneCall) {
  std::vector<LoadCompletion> loads;
  loads.push_back({.key = "a", .token = MustBeginLoad(store_, "a"), .result = LoadedAbsent{}});
  loads.push_back({.key = "b",
                   .token = MustBeginLoad(store_, "b"),
                   .result = LoadedExists{.type = Entry::Type::kZset}});
  loads.push_back({.key = "c", .token = MustBeginLoad(store_, "c"), .result = LoadedString("v")});
  loads.push_back({.key = "d", .token = LoadToken{.id = 999}, .result = LoadedString("stale")});

  const core::EvictionPolicy policy{kLongEviction};
  EXPECT_EQ(store_.CompleteLoads(loads, policy), 3U);
  EXPECT_EQ(store_.View("a", kAllDrained, 0).presence, KeyView::Presence::kTombstoned);
  EXPECT_EQ(store_.View("b", kAllDrained, 0).presence, KeyView::Presence::kStub);
  const KeyView c = store_.View("c", kAllDrained, 0);
  EXPECT_EQ(c.presence, KeyView::Presence::kLive);
  EXPECT_EQ(c.string_value(), "v");
  EXPECT_EQ(store_.View("d", kAllDrained, 0).presence, KeyView::Presence::kNonResident);
  EXPECT_TRUE(std::holds_alternative<LoadedFull>(loads.back().result))
      << "a discard leaves its result";
}

TEST_F(ResidencyTest, InstallUsesTheFootprintMeasuredOffTheLock) {
  SetValue members;
  members.members = {"a", "b", "c"};
  LoadedFull full = MakeLoadedFull(members, 0);
  EXPECT_EQ(full.bytes, ApproximateBytes(Value{members}));
  full.bytes += 1000;
  const LoadToken token = MustBeginLoad(store_, "s");
  const auto before = store_.Stats().used_bytes;
  ASSERT_TRUE(store_.CompleteLoad("s", token, std::move(full), kLongEviction, kAllDrained));
  EXPECT_EQ(store_.Stats().used_bytes - before,
            ApproximateBytes(Value{members}) + 1000 + sizeof(std::string) + 1)
      << "CompleteLoad takes the given footprint and measures nothing";
}

TEST_F(ResidencyTest, ReadersSeePlaceholderAsMiss) {
  ASSERT_TRUE(store_.BeginLoad("k").has_value());
  auto get = store_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_FALSE(get.has_value());
  EXPECT_EQ(get.error().code(), core::ErrorCode::kNotFound);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);
  EXPECT_FALSE(Resident("k"));
  EXPECT_EQ(store_.Stats().key_count, 0U) << "a placeholder is not a key";
}

TEST_F(ResidencyTest, EvictionIgnoresPlaceholders) {
  ASSERT_TRUE(store_.BeginLoad("k").has_value());
  EvictAfterDeadline(kAllDrained);
  store_.EvictLru(0, kAllDrained);
  store_.GcTombstones(kAllDrained);
  EXPECT_TRUE(store_.LoadPending("k"));
  EXPECT_EQ(store_.FindStub("k"), nullptr);
}

TEST_F(ResidencyTest, StaleLoadAfterWriteDrainEvictDiscards) {
  // A load reads cold before W1; W1 lands, drains and is evicted. The
  // key is non-resident again, so only the token can tell the load is
  // older than W1.
  const LoadToken token = MustBeginLoad(store_, "k");
  Write(ops::StringSet{.key = "k", .value = "w1"}, 7);
  EXPECT_FALSE(store_.LoadPending("k"));
  EXPECT_EQ(EvictAfterDeadline(7).by_deadline, 1U);
  ASSERT_NE(store_.FindStub("k"), nullptr);
  ASSERT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);

  EXPECT_FALSE(store_.CompleteLoad("k", token, LoadedString("pre-w1"), kLongEviction, kAllDrained));
  EXPECT_EQ(store_.Stats().load_discards, 1U);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);
  const Stub* stub = store_.FindStub("k");
  ASSERT_NE(stub, nullptr) << "a discarded load leaves the stub";
  EXPECT_EQ(stub->latest_seq, 7U);
}

// --- Flush floor ---

TEST_F(ResidencyTest, WipeClearsEntriesStubsAndPlaceholders) {
  Set("evicted", 1);
  EvictAfterDeadline(kAllDrained);
  Set("live", 2);
  ASSERT_TRUE(store_.BeginLoad("loading").has_value());
  ASSERT_EQ(store_.Stats().stub_entries, 1U);

  store_.Wipe(9);
  const auto stats = store_.Stats();
  EXPECT_EQ(stats.key_count, 0U);
  EXPECT_EQ(stats.stub_entries, 0U);
  EXPECT_EQ(stats.used_bytes, 0U);
  EXPECT_EQ(store_.FindStub("evicted"), nullptr);
  EXPECT_FALSE(store_.LoadPending("loading"));
  EXPECT_FALSE(Resident("live"));
}

TEST_F(ResidencyTest, KnownAbsentUntilColdDrainsTheFlush) {
  EXPECT_FALSE(store_.KnownAbsentAfterFlush(0)) << "no flush yet";
  store_.Wipe(9);
  EXPECT_TRUE(store_.KnownAbsentAfterFlush(0));
  EXPECT_TRUE(store_.KnownAbsentAfterFlush(8));
  EXPECT_FALSE(store_.KnownAbsentAfterFlush(9));
  store_.Wipe(3);
  EXPECT_TRUE(store_.KnownAbsentAfterFlush(8)) << "the floor never moves back";
}

// --- Backpressure inputs ---

TEST(ResidencyMemoryTest, UndrainedStoreOverItsRatioIsBackpressured) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  const uint64_t per_entry = MeasureStringBytes("k0", value);
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = per_entry * 4,
      .backpressure_ratio = 1.5,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto write = [&](int i) {
    const std::string key = "k" + std::to_string(i);
    return store.Apply(ops::WriteOp{ops::StringSet{.key = key, .value = value}}, kLongEviction,
                       static_cast<core::SequenceId>(i) + 1, 0);
  };

  // Five entries: over the budget, under 1.5 times it.
  for (int i = 0; i < 5; ++i) ASSERT_TRUE(write(i).has_value());
  EXPECT_GT(store.Stats().used_bytes, store.Stats().max_bytes);
  EXPECT_FALSE(store.Stats().backpressured);

  for (int i = 5; i < 7; ++i) ASSERT_TRUE(write(i).has_value());
  auto stats = store.Stats();
  EXPECT_TRUE(stats.backpressured);
  EXPECT_EQ(stats.unevictable_bytes, stats.used_bytes) << "every key is undrained";

  store.EvictLru(store.Stats().max_bytes, kAllDrained);
  stats = store.Stats();
  EXPECT_FALSE(stats.backpressured);
  EXPECT_LE(stats.used_bytes, stats.max_bytes);
  // A walk that stops early leaves the sum to the tick's full pass.
  store.EvictExpired(clock.SteadyNow(), kAllDrained);
  EXPECT_EQ(store.Stats().unevictable_bytes, 0U);
}

// --- LRU order and footprint cache ---

TEST_F(ResidencyTest, AppliesMoveKeysToTheHotEndAndReadsGetASecondChance) {
  Set("a", 1);
  Set("b", 1);
  Set("c", 1);
  Write(ops::Expire{.key = "a", .abs_ttl_ms = uint64_t{1} << 50}, 2);
  clock_.Advance(10ms);
  store_.SetAccessTime(clock_.SteadyNow());
  ASSERT_TRUE(store_.Exec(ops::ReadOp{ops::StringGet{.key = "b"}}).has_value());
  // Coldest first: b, c, a; b was read, so c, a, b.
  EXPECT_EQ(store_.EvictLru(store_.Stats().used_bytes - 1, kAllDrained), 1U);
  EXPECT_FALSE(Resident("c"));
  EXPECT_EQ(store_.EvictLru(store_.Stats().used_bytes - 1, kAllDrained), 1U);
  EXPECT_FALSE(Resident("a"));
  EXPECT_TRUE(Resident("b"));
}

std::string NumberedKey(int i) {
  std::string digits = std::to_string(i);
  return "k" + std::string(4 - digits.size(), '0') + digits;
}

TEST(ResidencyMemoryTest, BoundaryWriteEvictsInOrderOfWhatItEvicts) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  const uint64_t per_entry = MeasureStringBytes(NumberedKey(0), value);
  constexpr int kFits = 1000;
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = per_entry * kFits,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto write = [&](int i) {
    return store.Apply(ops::WriteOp{ops::StringSet{.key = NumberedKey(i), .value = value}},
                       kLongEviction, static_cast<core::SequenceId>(i) + 1, kAllDrained);
  };
  for (int i = 0; i < kFits; ++i) ASSERT_TRUE(write(i).has_value());
  ASSERT_EQ(store.Stats().eviction_count, 0U);
  ASSERT_EQ(store.LruVisitsForTesting(), 0U);

  // One over: evict down to 95% of the budget, visiting only victims.
  ASSERT_TRUE(write(kFits).has_value());
  const uint64_t evicted = store.Stats().eviction_count;
  EXPECT_EQ(evicted, static_cast<uint64_t>((kFits / 20) + 1));
  EXPECT_EQ(store.LruVisitsForTesting(), evicted);

  // The headroom absorbs the next writes without another walk.
  for (int i = kFits + 1; i <= kFits + (kFits / 20); ++i) ASSERT_TRUE(write(i).has_value());
  EXPECT_EQ(store.Stats().eviction_count, evicted);
  EXPECT_EQ(store.LruVisitsForTesting(), evicted);
  EXPECT_LE(store.Stats().used_bytes, store.Stats().max_bytes);
}

TEST(ResidencyMemoryTest, UndrainedStoreDoesNotRewalkUntilColdAdvances) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  const uint64_t per_entry = MeasureStringBytes(NumberedKey(0), value);
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = per_entry * 10,
      .backpressure_ratio = 10.0,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const auto write = [&](int i, core::SequenceId horizon) {
    return store.Apply(ops::WriteOp{ops::StringSet{.key = NumberedKey(i), .value = value}},
                       kLongEviction, static_cast<core::SequenceId>(i) + 1, horizon);
  };
  for (int i = 0; i < 11; ++i) ASSERT_TRUE(write(i, 0).has_value());
  const uint64_t first_walk = store.LruVisitsForTesting();
  EXPECT_EQ(first_walk, 10U) << "one walk of all but the written key finds nothing drained";
  for (int i = 11; i < 30; ++i) ASSERT_TRUE(write(i, 0).has_value());
  EXPECT_EQ(store.LruVisitsForTesting(), first_walk) << "no rewalk at the same horizon";
  EXPECT_EQ(store.Stats().eviction_count, 0U);

  // Cold drains the first 20 writes: the next write walks and evicts.
  ASSERT_TRUE(write(30, 20).has_value());
  EXPECT_GT(store.Stats().eviction_count, 0U);
  EXPECT_LE(store.Stats().used_bytes - store.Stats().unevictable_bytes, store.Stats().max_bytes);
}

TEST(ResidencyMemoryTest, CachedFootprintMatchesAFullRecount) {
  SingleShardStore built{SingleShardConfig{}};
  const auto apply = [&](const ops::WriteOp& op) {
    ASSERT_TRUE(built.Apply(op, kLongEviction).has_value());
  };
  const std::string long_value(40, 'x');
  apply(ops::SetAdd{.key = "s", .members = {"a", "b", "c"}});
  apply(ops::SetRem{.key = "s", .members = {"b", "missing"}});
  apply(ops::SetAdd{.key = "s", .members = {"d", "a"}});
  apply(ops::HashSet{.key = "h",
                     .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}});
  apply(ops::HashSet{.key = "h", .fields = {{.field = "f", .value = long_value}}});
  apply(ops::HashDel{.key = "h", .fields = {"g", "missing"}});
  apply(ops::ZsetAdd{
      .key = "z",
      .entries = {
          {.score = 1, .member = "m"}, {.score = 2, .member = "n"}, {.score = 2, .member = "o"}}});
  apply(ops::ZsetAdd{.key = "z", .entries = {{.score = 3, .member = "m"}}});
  apply(ops::ZsetRem{.key = "z", .members = {"n", "missing"}});
  apply(ops::StringSet{.key = "str", .value = long_value});

  SetValue set;
  set.members = {"a", "c", "d"};
  HashValue hash;
  hash.fields = {{"f", long_value}};
  ZsetValue zset;
  zset.member_scores = {{"m", 3}, {"o", 2}};
  zset.score_members = {{2, {"o"}}, {3, {"m"}}};
  SingleShardStore loaded{SingleShardConfig{}};
  const auto install = [&](std::string_view key, Value value) {
    const LoadToken token = MustBeginLoad(loaded, key);
    ASSERT_TRUE(loaded.CompleteLoad(key, token, MakeLoadedFull(std::move(value), 0), kLongEviction,
                                    kAllDrained));
  };
  install("s", set);
  install("h", hash);
  install("z", zset);
  install("str", long_value);

  EXPECT_EQ(built.Stats().used_bytes, loaded.Stats().used_bytes);
}

// --- ShardedHotStore ---

class ShardedResidencyTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  core::EvictionPolicy policy_{kShortEviction};
  std::atomic<core::SequenceId> horizon_{0};
  ShardedHotStore store_{ShardedHotStoreConfig{
      .max_memory_bytes = 1024UL * 1024,
      .shard_count = 4,
      .drained = [this](core::ShardId) { return horizon_.load(); },
      .eviction_policy = &policy_,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  void Set(std::string_view key, core::SequenceId seq) {
    ASSERT_TRUE(
        store_.Apply(ops::WriteOp{ops::StringSet{.key = key, .value = "v"}}, seq).has_value());
  }

  // Runs `act` while another thread waits on the key's load, and checks
  // the waiter wakes long before its deadline.
  void ExpectAwaitWakes(std::string_view key, const std::function<void()>& act) {
    abyss::testing::Latch started;
    abyss::testing::Latch done;
    bool woke = false;
    std::thread waiter([&] {
      started.Open();
      woke = store_.AwaitLoad(key, core::SteadyClock::now() + 10s);
      done.Open();
    });
    const abyss::testing::OnExit join([&] { waiter.join(); });
    ASSERT_TRUE(started.Wait());
    std::this_thread::sleep_for(20ms);
    const auto acted = core::SteadyClock::now();
    act();
    ASSERT_TRUE(done.Wait(5s));
    EXPECT_TRUE(woke);
    EXPECT_LT(core::SteadyClock::now() - acted, 5s);
  }
};

TEST(ShardedResidencyNullDrainedTest, NullSupplierTreatsEverythingAsDrained) {
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{kShortEviction};
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 1024UL * 1024,
      .shard_count = 2,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  ASSERT_TRUE(
      store.Apply(ops::WriteOp{ops::StringSet{.key = "k", .value = "v"}}, 1000).has_value());
  ASSERT_TRUE(
      store.Apply(ops::WriteOp{ops::StringSet{.key = "d", .value = "v"}}, 1000).has_value());
  ASSERT_TRUE(store.Apply(ops::WriteOp{ops::Del{.keys = {"d"}}}, 1001).has_value());
  clock.Advance(2s);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()).by_deadline, 1U);
  EXPECT_EQ(store.GcTombstones(), 1U);
  EXPECT_FALSE(store.KnownAbsentAfterFlush("k"));
}

TEST_F(ShardedResidencyTest, EveryEvictionEntryPointWaitsForDrain) {
  Set("a", 5);
  Set("b", 5);
  ASSERT_TRUE(store_.Apply(ops::WriteOp{ops::Del{.keys = {"b"}}}, 6).has_value());
  clock_.Advance(2s);

  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow()).Total(), 0U);
  EXPECT_EQ(store_.GcTombstones(), 0U);
  EXPECT_GT(store_.Stats()->unevictable_bytes, 0U);

  horizon_ = 6;
  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow()).by_deadline, 1U);
  EXPECT_EQ(store_.GcTombstones(), 1U);
  EXPECT_TRUE(store_.FindStub("a").has_value());
}

TEST(ShardedResidencyMemoryTest, MemoryTargetAndInlineEvictionWaitForDrain) {
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{kLongEviction};
  const std::string value(64, 'v');
  const uint64_t per_entry = MeasureStringBytes("k0", value);
  core::SequenceId horizon = 0;
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = per_entry * 3,
      .shard_count = 1,
      .drained = [&horizon](core::ShardId) { return horizon; },
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  for (int i = 0; i < 6; ++i) {
    clock.Advance(10ms);
    const std::string key = "k" + std::to_string(i);
    ASSERT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{.key = key, .value = value}},
                           static_cast<core::SequenceId>(i) + 1)
                    .has_value());
  }
  EXPECT_EQ(store.Stats()->key_count, 6U) << "inline eviction held back";
  EXPECT_EQ(store.EvictToMemoryTarget(), 0U);

  horizon = 6;
  EXPECT_GT(store.EvictToMemoryTarget(), 0U);
  EXPECT_LE(store.Stats()->used_bytes, store.Stats()->max_bytes);
}

TEST_F(ShardedResidencyTest, StubCapComesFromTheMemoryFraction) {
  // 1 MiB × 0.02 / 80 B over 4 shards.
  const size_t per_shard = static_cast<size_t>(1024.0 * 1024 * 0.02 / kStubBytes) / 4;
  constexpr int kKeys = 1000;
  for (int i = 0; i < kKeys; ++i) Set("k" + std::to_string(i), 1);
  horizon_ = 1;
  clock_.Advance(2s);
  EXPECT_EQ(store_.EvictExpired(clock_.SteadyNow()).by_deadline, static_cast<size_t>(kKeys));

  const auto stats = store_.Stats();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->stub_entries, per_shard * 4) << "every shard evicted past its cap";
  EXPECT_EQ(stats->stub_entries + stats->stub_drops, static_cast<uint64_t>(kKeys));
  EXPECT_GT(stats->stub_drops, 0U);
  EXPECT_EQ(stats->used_bytes, stats->stub_bytes);
}

TEST_F(ShardedResidencyTest, LoadForwardsInstallUnderTheShardLock) {
  const LoadToken token = MustBeginLoad(store_, "k");
  EXPECT_EQ(store_.BeginLoad("k").status, LoadStart::Status::kPending);
  ASSERT_TRUE(store_.CompleteLoad("k", token, MakeLoadedFull(std::string("v"), 0)));
  EXPECT_EQ(store_.BeginLoad("k").status, LoadStart::Status::kResident);
  auto get = store_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "v");

  const LoadToken stale = MustBeginLoad(store_, "j");
  Set("j", 3);
  EXPECT_FALSE(store_.CompleteLoad("j", stale, LoadedAbsent{}));
  EXPECT_EQ(store_.Stats()->load_discards, 1U);
}

TEST_F(ShardedResidencyTest, StubForwards) {
  Set("k", 2);
  horizon_ = 2;
  clock_.Advance(2s);
  store_.EvictExpired(clock_.SteadyNow());

  const auto stub = store_.FindStub("k");
  ASSERT_TRUE(stub.has_value());
  EXPECT_EQ(stub.value_or(Stub{}).latest_seq, 2U);
  EXPECT_FALSE(store_.FindStub("missing").has_value());
  EXPECT_FALSE(store_.DropStub("missing"));
  EXPECT_TRUE(store_.DropStub("k"));
  EXPECT_FALSE(store_.FindStub("k").has_value());
}

TEST_F(ShardedResidencyTest, FlushFloorIsPerShard) {
  const auto shard = core::ComputeShard("k", store_.shard_count());
  std::string other = "o";
  while (core::ComputeShard(other, store_.shard_count()) == shard) other += "o";

  ASSERT_TRUE(store_.Wipe(shard, 10).has_value());
  horizon_ = 9;
  EXPECT_TRUE(store_.KnownAbsentAfterFlush("k"));
  EXPECT_FALSE(store_.KnownAbsentAfterFlush(other));
  horizon_ = 10;
  EXPECT_FALSE(store_.KnownAbsentAfterFlush("k"));
}

TEST_F(ShardedResidencyTest, NoLoadBeginsUnderTheFlushFloor) {
  // Cold may still hold what the Flush removed.
  ASSERT_TRUE(store_.Wipe(core::ComputeShard("k", store_.shard_count()), 10).has_value());
  horizon_ = 9;
  EXPECT_EQ(store_.BeginLoad("k").status, LoadStart::Status::kFlushed);
  EXPECT_FALSE(store_.LoadPending("k"));
  horizon_ = 10;
  EXPECT_TRUE(store_.BeginLoad("k").started());
  EXPECT_TRUE(store_.LoadPending("k"));
}

// BeginLoad reads the drain horizon under the shard lock, so a Flush
// racing it lands wholly before (no load begins under the floor) or
// wholly after (its Wipe drops the placeholder): the load's pre-Flush
// state is never installed.
TEST(ShardedResidencyFloorTest, ALoadRacingAFlushNeverInstallsPreFlushState) {
  constexpr core::SequenceId kFlushSeq = 10;
  abyss::testing::TestClock clock;
  std::function<void()> on_horizon;
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 1024UL * 1024,
      .shard_count = 1,
      .drained = [&on_horizon](core::ShardId) -> core::SequenceId {
        if (on_horizon) std::exchange(on_horizon, nullptr)();
        return 0;
      },
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  std::atomic<bool> flushed{false};
  std::thread flusher;
  const abyss::testing::OnExit join([&flusher] {
    if (flusher.joinable()) flusher.join();
  });
  on_horizon = [&] {
    flusher = std::thread([&] {
      EXPECT_TRUE(store.Wipe(0, kFlushSeq).has_value());
      flushed = true;
    });
    const auto until = core::SteadyClock::now() + 100ms;
    while (!flushed && core::SteadyClock::now() < until) std::this_thread::sleep_for(1ms);
    EXPECT_FALSE(flushed) << "the horizon was read outside the shard lock";
  };

  const LoadStart start = store.BeginLoad("k");
  flusher.join();
  ASSERT_TRUE(flushed);
  if (start.started()) {
    EXPECT_FALSE(store.CompleteLoad("k", start.token, MakeLoadedFull(std::string("old"), 0)));
  } else {
    EXPECT_EQ(start.status, LoadStart::Status::kFlushed);
  }
  const auto read = store.Read(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(read.result.has_value());
  EXPECT_TRUE(read.result->IsNull()) << "pre-Flush state was installed";
  EXPECT_EQ(read.fence, kFlushSeq);
}

TEST_F(ShardedResidencyTest, CompleteLoadsForwardsInOneHold) {
  const auto shard = core::ComputeShard("k", store_.shard_count());
  std::string other = "j";
  while (core::ComputeShard(other, store_.shard_count()) != shard) other += "j";
  std::vector<LoadCompletion> loads;
  loads.push_back({.key = "k",
                   .token = MustBeginLoad(store_, "k"),
                   .result = MakeLoadedFull(std::string("v"), 0)});
  loads.push_back({.key = other, .token = MustBeginLoad(store_, other), .result = LoadedAbsent{}});

  EXPECT_EQ(store_.CompleteLoads(shard, loads), 2U);
  EXPECT_FALSE(store_.LoadPending("k"));
  EXPECT_FALSE(store_.LoadPending(other));
  auto get = store_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "v");
  EXPECT_EQ(store_.Probe(other), core::HotKeyPresence::kTombstoned);
}

TEST(ShardedResidencyStubsTest, RetainsStubsFollowsTheStubBudget) {
  EXPECT_TRUE(ShardedHotStore(ShardedHotStoreConfig{.shard_count = 2}).RetainsStubs());
  EXPECT_FALSE(ShardedHotStore(ShardedHotStoreConfig{.max_memory_bytes = 0, .shard_count = 2})
                   .RetainsStubs());
}

TEST_F(ShardedResidencyTest, AwaitLoadWakesOnComplete) {
  const LoadToken token = MustBeginLoad(store_, "k");
  ExpectAwaitWakes("k", [&] { EXPECT_TRUE(store_.CompleteLoad("k", token, LoadedAbsent{})); });
}

TEST_F(ShardedResidencyTest, AwaitLoadWakesOnAbort) {
  const LoadToken token = MustBeginLoad(store_, "k");
  ExpectAwaitWakes("k", [&] { store_.AbortLoad("k", token); });
}

TEST_F(ShardedResidencyTest, AwaitLoadWakesOnApply) {
  ASSERT_TRUE(store_.BeginLoad("k").started());
  ExpectAwaitWakes("k", [&] { Set("k", 4); });
}

TEST_F(ShardedResidencyTest, AwaitLoadWakesOnDel) {
  ASSERT_TRUE(store_.BeginLoad("k").started());
  ExpectAwaitWakes("k", [&] {
    EXPECT_TRUE(store_.Apply(ops::WriteOp{ops::Del{.keys = {"k"}}}, 4).has_value());
  });
}

TEST_F(ShardedResidencyTest, AwaitLoadWakesOnWipe) {
  ASSERT_TRUE(store_.BeginLoad("k").started());
  const auto shard = core::ComputeShard("k", store_.shard_count());
  ExpectAwaitWakes("k", [&] { EXPECT_TRUE(store_.Wipe(shard, 4).has_value()); });
}

TEST_F(ShardedResidencyTest, AwaitLoadTimesOutWhileTheLoadIsPending) {
  ASSERT_TRUE(store_.BeginLoad("k").started());
  const auto start = core::SteadyClock::now();
  EXPECT_FALSE(store_.AwaitLoad("k", start + 50ms));
  EXPECT_GE(core::SteadyClock::now() - start, 50ms);
  EXPECT_TRUE(store_.AwaitLoad("other", start)) << "no load pending";
}

// A key written after a Flush is resident: BeginLoad judges the entry
// before the floor, in either hold.
TEST_F(ShardedResidencyTest, AKeyWrittenAfterAFlushIsResidentNotFlushed) {
  const core::ShardId shard = core::ComputeShard("k", store_.shard_count());
  ASSERT_TRUE(store_.Wipe(shard, 10).has_value());
  Set("k", 11);
  EXPECT_EQ(store_.BeginLoad("k").status, LoadStart::Status::kResident);
  const std::array<core::ShardId, 1> held{shard};
  auto locks = store_.LockExclusive(held);
  EXPECT_EQ(locks.BeginLoad("k").status, LoadStart::Status::kResident);
}

class NegativeCacheTest : public ::testing::Test {
 protected:
  void InstallAbsent(std::string_view key) {
    const LoadToken token = MustBeginLoad(store_, key);
    ASSERT_TRUE(store_.CompleteLoad(key, token, LoadedAbsent{}, kLongEviction, kAllDrained));
  }
  bool Negative(std::string_view key) const {
    const KeyView view = store_.View(key, kAllDrained, 0);
    return view.presence == KeyView::Presence::kTombstoned && view.latest_seq == 0;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  SingleShardStore store_{SingleShardConfig{.negative_max_entries = 2}};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(NegativeCacheTest, AbsentLoadsAreAFifoBoundedByTheirCap) {
  InstallAbsent("a");
  InstallAbsent("b");
  InstallAbsent("c");
  EXPECT_FALSE(store_.HasEntry("a")) << "the oldest went first";
  EXPECT_TRUE(Negative("b"));
  EXPECT_TRUE(Negative("c"));
  EXPECT_EQ(store_.Stats().negative_entries, 2U);
}

TEST_F(NegativeCacheTest, AbsentLoadsNeverWaitBehindAnUndrainedDel) {
  ASSERT_TRUE(
      store_.Apply(ops::WriteOp{ops::Del{.keys = {"deleted"}}}, kLongEviction, 9, 0).has_value());
  InstallAbsent("a");
  InstallAbsent("b");
  InstallAbsent("c");
  EXPECT_EQ(store_.GcTombstones(0), 0U) << "the DEL is undrained";
  EXPECT_FALSE(store_.HasEntry("a"));
  EXPECT_EQ(store_.Stats().negative_entries, 2U);
  EXPECT_EQ(store_.View("deleted", 0, 0).latest_seq, 9U);
}

TEST_F(NegativeCacheTest, AWriteTakesAKeyOutOfTheCache) {
  InstallAbsent("a");
  ASSERT_TRUE(store_.Apply(ops::WriteOp{ops::StringSet{.key = "a", .value = "v"}}, kLongEviction, 3)
                  .has_value());
  InstallAbsent("b");
  ASSERT_TRUE(store_.Apply(ops::WriteOp{ops::Del{.keys = {"b"}}}, kLongEviction, 4, 0).has_value());
  EXPECT_EQ(store_.Stats().negative_entries, 0U);
  InstallAbsent("c");
  InstallAbsent("d");
  InstallAbsent("e");
  EXPECT_EQ(store_.Exec(ops::ReadOp{ops::StringGet{.key = "a"}})->AsString(), "v")
      << "a rewritten key outlived its place in the FIFO";
  EXPECT_EQ(store_.View("b", 0, 0).latest_seq, 4U) << "an undrained DEL is not the cache's";
  EXPECT_EQ(store_.Stats().negative_entries, 2U);
}

TEST_F(NegativeCacheTest, ABatchKeepsItsAbsentLoadsUntilTheNextGc) {
  std::vector<LoadCompletion> loads;
  for (const auto* key : {"a", "b", "c"}) {
    loads.push_back({.key = key, .token = MustBeginLoad(store_, key), .result = LoadedAbsent{}});
  }
  EXPECT_EQ(store_.CompleteLoads(loads, core::EvictionPolicy{}), 3U);
  EXPECT_EQ(store_.Stats().negative_entries, 3U) << "decide reads every one";
  EXPECT_EQ(store_.GcTombstones(kAllDrained), 0U);
  EXPECT_EQ(store_.Stats().negative_entries, 2U);
}

TEST_F(ResidencyTest, AStubAnswersMetaReadsAndNothingElse) {
  const LoadToken token = MustBeginLoad(store_, "h");
  const int64_t ttl = WallMs(clock_) + 10'500;
  ASSERT_TRUE(store_.CompleteLoad("h", token,
                                  LoadedExists{.type = Entry::Type::kHash, .abs_ttl_ms = ttl},
                                  kLongEviction, kAllDrained));
  const auto read = [&](const ops::ReadOp& op) { return store_.Read(op, kAllDrained); };
  const auto exists = read(ops::ReadOp{ops::Exists{.keys = {"h"}}});
  ASSERT_TRUE(exists.result.has_value());
  EXPECT_EQ(exists.result->AsInteger(), 1);
  EXPECT_EQ(exists.fence, 0U);
  EXPECT_EQ(read(ops::ReadOp{ops::Type{.key = "h"}}).result->AsString(), "hash");
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "h"}}).result->AsInteger(), 11);
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "h", .millis = true}}).result->AsInteger(), 10'500);
  const auto field = read(ops::ReadOp{ops::HashGet{.key = "h", .field = "f"}});
  ASSERT_FALSE(field.result.has_value());
  EXPECT_EQ(field.result.error().code(), core::ErrorCode::kNotFound) << "a stub holds no value";

  clock_.Advance(11s);
  EXPECT_EQ(read(ops::ReadOp{ops::Exists{.keys = {"h"}}}).result->AsInteger(), 0);
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "h"}}).result->AsInteger(), -2);
  EXPECT_EQ(read(ops::ReadOp{ops::Type{.key = "h"}}).result->AsString(), "none");
}

TEST_F(ResidencyTest, TtlAndTypeOfAResidentKey) {
  Set("plain", core::kFirstSeq);
  Set("timed", core::kFirstSeq + 1, static_cast<uint64_t>(WallMs(clock_) + 1'499));
  const auto read = [&](const ops::ReadOp& op) { return store_.Read(op, kAllDrained); };
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "plain"}}).result->AsInteger(), -1);
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "timed"}}).result->AsInteger(), 1) << "rounded";
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "timed", .millis = true}}).result->AsInteger(), 1499);
  EXPECT_EQ(read(ops::ReadOp{ops::Type{.key = "plain"}}).result->AsString(), "string");
  EXPECT_EQ(read(ops::ReadOp{ops::Ttl{.key = "timed"}}).fence, core::kFirstSeq + 1);
}

TEST(ShardedFillTest, AFillPastTheBackpressureLimitIsDropped) {
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024,
      .shard_count = 1,
      .drained = [](core::ShardId) { return core::SequenceId{0}; },
  }};
  // Undrained, so nothing can be evicted to make room.
  core::SequenceId seq = 0;
  while (store.EvictShardToTarget(0)) {
    ASSERT_LT(seq, 1000U);
    const auto applied = store.Apply(ops::WriteOp{ops::StringSet{.key = "k" + std::to_string(seq),
                                                                 .value = std::string(4096, 'x')}},
                                     seq + 1);
    EXPECT_TRUE(applied.has_value() ||
                applied.error().code() == core::ErrorCode::kResourceExhausted);
    ++seq;
  }
  const LoadToken token = MustBeginLoad(store, "fill");
  EXPECT_EQ(store.Fill("fill", token, MakeLoadedFull(std::string("v"), 0)),
            ShardedHotStore::FillResult::kOverBackpressure);
  EXPECT_FALSE(store.LoadPending("fill"));
  EXPECT_EQ(store.BeginLoad("fill").status, LoadStart::Status::kStarted) << "nothing installed";
}

TEST(ShardedFillTest, AFillOverItsShareOfTheBudgetIsNeverMade) {
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024, .shard_count = 1, .fill_max_fraction = 0.25}};
  const LoadToken big = MustBeginLoad(store, "big");
  EXPECT_EQ(store.Fill("big", big, MakeLoadedFull(std::string(20000, 'x'), 0)),
            ShardedHotStore::FillResult::kTooLarge)
      << "past a quarter of the shard's budget";
  EXPECT_FALSE(store.LoadPending("big"));
  const LoadToken small_token = MustBeginLoad(store, "small");
  EXPECT_EQ(store.Fill("small", small_token, MakeLoadedFull(std::string(8000, 'x'), 0)),
            ShardedHotStore::FillResult::kInstalled);
  const LoadToken again = MustBeginLoad(store, "big");
  EXPECT_TRUE(store.CompleteLoad("big", again, MakeLoadedFull(std::string(20000, 'x'), 0)))
      << "a load decide asked for is no fill";
}

TEST(ShardedFillTest, AFillEvictsDrainedKeysToMakeRoom) {
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024, .shard_count = 1, .fill_max_fraction = 0.5}};
  for (core::SequenceId seq = 1; seq <= 12; ++seq) {
    EXPECT_TRUE(store
                    .Apply(ops::WriteOp{ops::StringSet{.key = "k" + std::to_string(seq),
                                                       .value = std::string(4096, 'x')}},
                           seq)
                    .has_value());
  }
  const auto before = store.Stats()->eviction_count;
  const LoadToken token = MustBeginLoad(store, "fill");
  ASSERT_EQ(store.Fill("fill", token, MakeLoadedFull(std::string(16384, 'y'), 0)),
            ShardedHotStore::FillResult::kInstalled);
  EXPECT_GT(store.Stats()->eviction_count, before);
  EXPECT_LE(store.Stats()->used_bytes, 64UL * 1024);
}

}  // namespace
}  // namespace abyss::hot
