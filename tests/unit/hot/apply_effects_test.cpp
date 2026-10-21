#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/hot/single_shard_store.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

namespace ops = core::ops;
using Presence = KeyView::Presence;

constexpr core::EvictionTTL kEviction{3600};

class ApplyEffectsTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  SingleShardStore store_{SingleShardConfig{
      .stub_max_entries = 8,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  core::EvictionPolicy policy_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  uint64_t NowMs() const {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
            .count());
  }

  static core::WallTime AtMs(uint64_t ms) { return core::WallTime{std::chrono::milliseconds{ms}}; }

  void Write(const ops::WriteOp& op, core::SequenceId seq) {
    ASSERT_TRUE(store_.Apply(op, kEviction, seq).has_value());
  }

  KeyView View(std::string_view key, core::SequenceId horizon = kAllDrained) const {
    return store_.View(key, horizon, NowMs());
  }

  std::vector<core::RespValue> Apply(std::vector<core::Effect>& effects, core::SequenceId first_seq,
                                     std::optional<uint64_t> at_ms = std::nullopt) {
    return store_.ApplyEffects(effects, first_seq, AtMs(at_ms.value_or(NowMs())), policy_,
                               kAllDrained);
  }

  // Leaves `key` as a stub: written at seq 1, drained, then evicted.
  void MakeStub(std::string_view key, uint64_t abs_ttl_ms = 0) {
    Write(ops::StringSet{.key = key, .value = "v", .abs_ttl_ms = abs_ttl_ms}, 1);
    ASSERT_EQ(store_.EvictLru(0, 1), 1U);
    ASSERT_NE(store_.FindStub(key), nullptr);
  }
};

// Marked as decide marks it: SET and DEL replace the key's state.
core::Effect EffectOf(std::vector<std::string> args) {
  core::Effect effect;
  effect.key = args.at(1);
  effect.replaces_state = args.at(0) == "SET" || args.at(0) == "DEL";
  effect.cmd.args = std::move(args);
  return effect;
}

// --- View ---

TEST_F(ApplyEffectsTest, ViewOfALiveKey) {
  Write(ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = NowMs() + 1000}, 7);
  const KeyView view = View("k");
  EXPECT_EQ(view.presence, Presence::kLive);
  EXPECT_EQ(view.type, Entry::Type::kString);
  EXPECT_EQ(view.abs_ttl_ms, static_cast<int64_t>(NowMs() + 1000));
  EXPECT_EQ(view.latest_seq, 7U);
  EXPECT_FALSE(view.flush_floor);
  EXPECT_EQ(view.string_value(), "v");
}

TEST_F(ApplyEffectsTest, ViewOfATombstone) {
  Write(ops::StringSet{.key = "k", .value = "v"}, 1);
  Write(ops::Del{.keys = {"k"}}, 4);
  const KeyView view = View("k");
  EXPECT_EQ(view.presence, Presence::kTombstoned);
  EXPECT_EQ(view.latest_seq, 4U);
  EXPECT_FALSE(view.flush_floor);
  EXPECT_EQ(view.value, nullptr);
}

TEST_F(ApplyEffectsTest, ViewOfAnExpiredEntryKeepsItsValue) {
  Write(ops::SetAdd{.key = "k", .members = {"m"}}, 3);
  Write(ops::Expire{.key = "k", .abs_ttl_ms = NowMs() + 10}, 4);
  EXPECT_EQ(View("k").presence, Presence::kLive);
  const KeyView view = store_.View("k", kAllDrained, NowMs() + 10);
  EXPECT_EQ(view.presence, Presence::kExpired) << "judged at the caller's time";
  EXPECT_TRUE(view.set_has("m"));
  EXPECT_EQ(view.latest_seq, 4U);
}

TEST_F(ApplyEffectsTest, ViewOfAStub) {
  MakeStub("k", NowMs() + 1000);
  const KeyView view = View("k");
  EXPECT_EQ(view.presence, Presence::kStub);
  EXPECT_EQ(view.type, Entry::Type::kString);
  EXPECT_EQ(view.abs_ttl_ms, static_cast<int64_t>(NowMs() + 1000));
  EXPECT_EQ(view.latest_seq, 1U);
  EXPECT_EQ(view.value, nullptr);

  const KeyView later = store_.View("k", kAllDrained, NowMs() + 1000);
  EXPECT_EQ(later.presence, Presence::kExpired) << "a stub past its TTL is expired";
  EXPECT_EQ(later.value, nullptr);
}

TEST_F(ApplyEffectsTest, ViewOfANonResidentKey) {
  EXPECT_EQ(View("never").presence, Presence::kNonResident);
  MakeStub("k");
  ASSERT_TRUE(store_.BeginLoad("k").has_value());
  EXPECT_EQ(View("k").presence, Presence::kNonResident) << "a load in flight hides the stub";
}

TEST_F(ApplyEffectsTest, ViewUnderTheFlushFloor) {
  store_.Wipe(5);
  // A loaded key is drained at once, so it can be evicted to a stub
  // the floor still covers.
  const LoadToken token = store_.BeginLoad("loaded").value_or(LoadToken{});
  ASSERT_NE(token.id, 0U);
  ASSERT_TRUE(store_.CompleteLoad("loaded", token, MakeLoadedFull(std::string("v"), 0), kEviction,
                                  kAllDrained));
  ASSERT_EQ(store_.EvictLru(0, 0), 1U);

  for (const std::string_view key : {"absent", "loaded"}) {
    SCOPED_TRACE(key);
    const KeyView floor = View(key, 4);
    EXPECT_EQ(floor.presence, Presence::kTombstoned);
    EXPECT_TRUE(floor.flush_floor);
    EXPECT_EQ(floor.latest_seq, 5U);
  }
  EXPECT_EQ(View("absent", 5).presence, Presence::kNonResident) << "drained past the Flush";
  EXPECT_EQ(View("loaded", 5).presence, Presence::kStub);
}

TEST_F(ApplyEffectsTest, ViewAccessorsFollowTheType) {
  Write(ops::SetAdd{.key = "s", .members = {"a", "b"}}, 1);
  Write(ops::ZsetAdd{.key = "z", .entries = {{.score = 1.5, .member = "a"}}}, 1);
  Write(ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "v"}}}, 1);
  Write(ops::StringSet{.key = "k", .value = "str"}, 1);

  const KeyView s = View("s");
  EXPECT_TRUE(s.set_has("a"));
  EXPECT_FALSE(s.set_has("z"));
  EXPECT_EQ(s.collection_size(), 2U);
  EXPECT_FALSE(s.zset_score("a").has_value());

  const KeyView z = View("z");
  EXPECT_EQ(z.zset_score("a"), 1.5);
  EXPECT_FALSE(z.zset_score("b").has_value());
  EXPECT_EQ(z.collection_size(), 1U);

  const KeyView h = View("h");
  EXPECT_TRUE(h.hash_has("f"));
  EXPECT_EQ(h.hash_get("f"), "v");
  EXPECT_FALSE(h.hash_get("g").has_value());
  EXPECT_EQ(h.string_value(), "");

  const KeyView k = View("k");
  EXPECT_EQ(k.string_value(), "str");
  EXPECT_EQ(k.collection_size(), 0U);
  EXPECT_FALSE(k.set_has("str"));
  ASSERT_NE(k.value, nullptr);
  EXPECT_EQ(std::get<std::string>(*k.value), "str");
}

// --- ApplyEffects ---

TEST_F(ApplyEffectsTest, AppliesInOrderAtIncreasingSeqs) {
  std::vector<core::Effect> effects = {EffectOf({"SET", "a", "1"}), EffectOf({"SADD", "s", "x"}),
                                       EffectOf({"HSET", "h", "f", "v"}), EffectOf({"DEL", "a"})};
  const auto replies = Apply(effects, 10);
  ASSERT_EQ(replies.size(), 4U);
  EXPECT_EQ(replies[0].AsString(), "OK");
  EXPECT_EQ(replies[1].AsInteger(), 1);
  EXPECT_EQ(replies[2].AsInteger(), 1);
  EXPECT_EQ(replies[3].AsInteger(), 1) << "the DEL sees the SET before it";
  EXPECT_EQ(View("a").presence, Presence::kTombstoned);
  EXPECT_EQ(View("a").latest_seq, 13U);
  EXPECT_EQ(View("s").latest_seq, 11U);
  EXPECT_EQ(View("h").latest_seq, 12U);
}

TEST_F(ApplyEffectsTest, SetMovesItsValueIn) {
  const std::string value(4096, 'x');
  std::vector<core::Effect> effects = {EffectOf({"SET", "k", value})};
  const char* bytes = effects[0].cmd.args[2].data();
  const auto replies = Apply(effects, 1);
  EXPECT_EQ(replies.at(0).AsString(), "OK");
  EXPECT_TRUE(effects[0].cmd.args[2].empty()) << "moved out of the effect";
  EXPECT_EQ(View("k").string_value(), value);
  EXPECT_EQ(View("k").string_value().data(), bytes) << "the same buffer, not a copy";
}

TEST_F(ApplyEffectsTest, SetGetMovesTheOldValueOut) {
  std::vector<core::Effect> seed = {EffectOf({"SET", "k", std::string(4096, 'o')})};
  Apply(seed, 1);
  const char* old_bytes = View("k").string_value().data();

  std::vector<core::Effect> effects = {EffectOf({"SET", "k", "new"})};
  effects[0].reply_old_value = true;
  auto replies = Apply(effects, 2);
  ASSERT_TRUE(replies.at(0).IsBulkString());
  EXPECT_EQ(replies[0].AsString(), std::string(4096, 'o'));
  EXPECT_EQ(replies[0].AsString().data(), old_bytes) << "the same buffer, not a copy";
  EXPECT_EQ(View("k").string_value(), "new");

  std::vector<core::Effect> fresh = {EffectOf({"SET", "fresh", "v"})};
  fresh[0].reply_old_value = true;
  EXPECT_TRUE(Apply(fresh, 3).at(0).IsNull());
}

TEST_F(ApplyEffectsTest, DelOfAStubOnlyKeyLeavesATombstone) {
  MakeStub("k");
  std::vector<core::Effect> effects = {EffectOf({"DEL", "k"})};
  const auto replies = Apply(effects, 5);
  EXPECT_EQ(replies.at(0).AsInteger(), 0);
  EXPECT_EQ(store_.FindStub("k"), nullptr);
  const KeyView view = View("k", 1);
  EXPECT_EQ(view.presence, Presence::kTombstoned);
  EXPECT_EQ(view.latest_seq, 5U);
  EXPECT_EQ(store_.GcTombstones(4), 0U) << "held until cold drains the DEL";
  EXPECT_EQ(store_.GcTombstones(5), 1U);
}

// Replay can meet a value past its TTL at an effect's instant whose
// expiry no decision logged, cold having deleted it first: the effect
// applies as it was decided, over an absent key.
TEST_F(ApplyEffectsTest, AnEffectJudgesExpiryAtItsOwnInstant) {
  Write(ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = NowMs() + 10}, 1);
  std::vector<core::Effect> effects = {EffectOf({"ZADD", "k", "1", "m"})};
  effects[0].replaces_state = true;
  const auto replies = Apply(effects, 2, NowMs() + 10);
  EXPECT_EQ(replies.at(0).AsInteger(), 1);
  const KeyView view = View("k");
  EXPECT_EQ(view.presence, Presence::kLive);
  EXPECT_EQ(view.type, Entry::Type::kZset);
  EXPECT_EQ(view.abs_ttl_ms, 0);

  // Before its TTL the same value is live to an effect.
  Write(ops::StringSet{.key = "j", .value = "v", .abs_ttl_ms = NowMs() + 10}, 3);
  std::vector<core::Effect> get = {EffectOf({"SET", "j", "w"})};
  get[0].reply_old_value = true;
  EXPECT_EQ(Apply(get, 4, NowMs() + 9).at(0).AsString(), "v");
}

TEST_F(ApplyEffectsTest, AnObservedExpiryDelTombstonesTheEntry) {
  Write(ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = NowMs() + 10}, 1);
  std::vector<core::Effect> effects = {EffectOf({"DEL", "k"})};
  effects[0].observed_expiry = true;
  const uint64_t expired_before = store_.Stats().expired_count;
  const auto replies = Apply(effects, 2, NowMs() + 10);
  EXPECT_EQ(replies.at(0).AsInteger(), 0) << "expired at its instant; decide replies";
  EXPECT_EQ(View("k").presence, Presence::kTombstoned);
  EXPECT_EQ(store_.Stats().key_count, 0U);
  EXPECT_EQ(store_.Stats().expired_count, expired_before + 1) << "an expiry, not a delete";
}

// Neither appended_at nor the wall clock expires anything, so a replay
// long after the TTL applies what the decision saw.
TEST_F(ApplyEffectsTest, ApplyJudgesNoTtl) {
  const uint64_t then = NowMs();
  Write(ops::SetAdd{.key = "k", .members = {"a"}}, 1);
  Write(ops::Expire{.key = "k", .abs_ttl_ms = then + 10}, 2);
  clock_.Advance(std::chrono::seconds(60));

  std::vector<core::Effect> effects = {EffectOf({"SADD", "k", "b"}),
                                       EffectOf({"PEXPIREAT", "k", std::to_string(then + 20)})};
  const auto replies = Apply(effects, 3, then + 5);
  EXPECT_EQ(replies.at(0).AsInteger(), 1);
  EXPECT_EQ(replies.at(1).AsInteger(), 1) << "live at appended_at";
  const KeyView view = store_.View("k", kAllDrained, then + 5);
  EXPECT_EQ(view.collection_size(), 2U) << "the earlier member survives";
  EXPECT_EQ(view.abs_ttl_ms, static_cast<int64_t>(then + 20));
}

TEST_F(ApplyEffectsTest, AnOvershootStillApplies) {
  SingleShardStore tiny{SingleShardConfig{.max_memory_bytes = 64}};
  std::vector<core::Effect> effects = {EffectOf({"SET", "k", std::string(1024, 'x')})};
  const auto replies = tiny.ApplyEffects(effects, 1, AtMs(NowMs()), policy_, /*horizon=*/0);
  EXPECT_EQ(replies.at(0).AsString(), "OK");
  EXPECT_EQ(tiny.View("k", 0, NowMs()).presence, Presence::kLive);
  EXPECT_GT(tiny.Stats().used_bytes, 64U);
}

TEST_F(ApplyEffectsTest, AStateReplacingEffectMayMeetAnExpiredKey) {
  Write(ops::SetAdd{.key = "k", .members = {"a"}}, 1);
  Write(ops::Expire{.key = "k", .abs_ttl_ms = NowMs() + 10}, 2);
  std::vector<core::Effect> effects = {EffectOf({"SET", "k", "v"})};
  EXPECT_EQ(Apply(effects, 3, NowMs() + 10).at(0).AsString(), "OK");
  EXPECT_EQ(View("k").string_value(), "v");
}

#ifndef NDEBUG
using ApplyEffectsExpiryDeathTest = ApplyEffectsTest;

// Decide logs a DEL first, so meeting one is a decide or clock bug.
TEST_F(ApplyEffectsExpiryDeathTest, AStateReadingEffectOnAKeyExpiredAtAppendedAtIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  Write(ops::SetAdd{.key = "k", .members = {"a"}}, 1);
  Write(ops::Expire{.key = "k", .abs_ttl_ms = NowMs() + 10}, 2);
  std::vector<core::Effect> effects = {EffectOf({"SADD", "k", "b"})};
  EXPECT_DEATH(Apply(effects, 3, NowMs() + 10), "met a key expired at appended_at: k");
}

TEST_F(ApplyEffectsExpiryDeathTest, ASetGetOnAKeyExpiredAtAppendedAtIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  Write(ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = NowMs() + 10}, 1);
  std::vector<core::Effect> effects = {EffectOf({"SET", "k", "w"})};
  effects[0].reply_old_value = true;
  EXPECT_DEATH(Apply(effects, 2, NowMs() + 10), "met a key expired at appended_at: k");
}
#endif

TEST(ApplyEffectsDeathTest, AMalformedEffectIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        SingleShardStore store{SingleShardConfig{}};
        std::vector<core::Effect> effects = {EffectOf({"ZADD", "k", "not-a-score", "m"})};
        store.ApplyEffects(effects, 1, core::WallTime{}, core::EvictionPolicy{}, kAllDrained);
      },
      "decided effect does not parse");
}

// Decide ruled the error out and the effect is already logged, so a
// reply would ack a write hot and the log disagree on.
TEST(ApplyEffectsDeathTest, AnEffectThatFailsToApplyIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        SingleShardStore store{SingleShardConfig{}};
        std::vector<core::Effect> effects;
        effects.push_back(EffectOf({"SET", "k", "v"}));
        effects.push_back(EffectOf({"SADD", "k", "m"}));
        effects[0].replaces_state = true;
        store.ApplyEffects(effects, 1, core::WallTime{}, core::EvictionPolicy{}, kAllDrained);
      },
      "a decided effect failed to apply");
}

}  // namespace
}  // namespace abyss::hot
