#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/engine/decide.h"
#include "abyss/hot/single_shard_store.h"
#include "decide_fixture.h"

namespace abyss::engine {
namespace {

using core::PredicateFlags;
using testing::DecideOn;
using testing::Describe;
using testing::ErrorOf;
using testing::ExpectEffects;
using testing::Expired;
using testing::ExpiredStub;
using testing::FakeKey;
using testing::FlushAbsent;
using testing::HashOf;
using testing::Keys;
using testing::kNow;
using testing::kTtl;
using testing::Loads;
using testing::Presence;
using testing::SetOf;
using testing::Str;
using testing::StubOf;
using testing::Tombstone;
using testing::Type;
using testing::ZsetOf;
namespace ops = core::ops;

const FakeKey kNonResident{.presence = Presence::kNonResident};

// --- MSET and MSETNX ---

TEST(DecideMultiKeyTest, MsetWritesEveryKeyBlind) {
  const Decision d = DecideOn(Keys{}, {"MSET", "a", "1", "b", "2"});
  EXPECT_TRUE(d.needs_load.empty());
  ExpectEffects(d.effects, {{.args = {"SET", "a", "1"}, .replaces_state = true},
                            {.args = {"SET", "b", "2"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), "+OK");
}

TEST(DecideMultiKeyTest, MsetnxSetsAllWhenNoneExist) {
  const Decision d = DecideOn(Keys{{"a", Tombstone()}, {"b", FlushAbsent()}},
                              {"MSETNX", "a", "1", "b", "2"}, PredicateFlags::kMsetNx);
  ExpectEffects(d.effects, {{.args = {"SET", "a", "1"}, .replaces_state = true},
                            {.args = {"SET", "b", "2"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":1");
}

TEST(DecideMultiKeyTest, MsetnxSetsNothingWhenAnyExists) {
  for (const FakeKey& b : {Str("v"), SetOf({"m"})}) {
    const Decision d = DecideOn(Keys{{"a", Tombstone()}, {"b", b}, {"c", Tombstone()}},
                                {"MSETNX", "a", "1", "b", "2", "c", "3"}, PredicateFlags::kMsetNx);
    EXPECT_TRUE(d.effects.empty());
    EXPECT_EQ(Describe(d.reply), ":0");
  }
}

TEST(DecideMultiKeyTest, MsetnxTreatsAnExpiredKeyAsAbsent) {
  const Decision d = DecideOn(Keys{{"a", Tombstone()}, {"b", Expired(Str("old"))}},
                              {"MSETNX", "a", "1", "b", "2"}, PredicateFlags::kMsetNx);
  ExpectEffects(d.effects, {{.args = {"DEL", "b"}, .replaces_state = true},
                            {.args = {"SET", "a", "1"}, .replaces_state = true},
                            {.args = {"SET", "b", "2"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":1");
}

TEST(DecideMultiKeyTest, MsetnxLoadsEveryUnknownKeyAtOnce) {
  const Decision d =
      DecideOn(Keys{{"a", Tombstone()}, {"c", Expired(Str("x"))}},
               {"MSETNX", "a", "1", "b", "2", "c", "3", "d", "4"}, PredicateFlags::kMsetNx);
  EXPECT_EQ(d.needs_load, Loads({"b", "d"}, Need::kExistence)) << "existence is all it needs";
  EXPECT_TRUE(d.effects.empty()) << "nothing else is valid, the expiry DEL included";
  EXPECT_FALSE(d.reply.has_value());
  EXPECT_TRUE(d.observed.empty());
}

TEST(DecideMultiKeyTest, MsetnxIsAnsweredByAStub) {
  const Decision d = DecideOn(Keys{{"a", Tombstone()}, {"b", StubOf(Type::kHash)}},
                              {"MSETNX", "a", "1", "b", "2"}, PredicateFlags::kMsetNx);
  EXPECT_TRUE(d.needs_load.empty());
  EXPECT_TRUE(d.effects.empty());
  EXPECT_EQ(Describe(d.reply), ":0");
}

TEST(DecideMultiKeyTest, MsetnxRepeatedKeySetsBoth) {
  const Decision d =
      DecideOn(Keys{{"k", Tombstone()}}, {"MSETNX", "k", "1", "k", "2"}, PredicateFlags::kMsetNx);
  ExpectEffects(d.effects, {{.args = {"SET", "k", "1"}, .replaces_state = true},
                            {.args = {"SET", "k", "2"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":1");
}

TEST(DecideMultiKeyTest, RestoreGivesMsetAndMsetnxTheirValuesBack) {
  const Keys keys = {{"a", Tombstone()}, {"b", Tombstone()}};
  for (const auto& [args, flags] : std::vector<std::pair<std::vector<std::string>, PredicateFlags>>{
           {{"MSET", "a", "1", "b", "2"}, PredicateFlags::kNone},
           {{"msetnx", "a", "1", "b", "2"}, PredicateFlags::kMsetNx}}) {
    SCOPED_TRACE(args[0]);
    core::RespCommand cmd{.args = args};
    Decision d = Decide(cmd, flags, kNow, testing::LookupOf(keys));
    ASSERT_EQ(d.moved.size(), 2U);
    std::vector<std::vector<std::string>> effects;
    effects.reserve(d.effects.size());
    for (const auto& e : d.effects) effects.push_back(e.cmd.args);
    Restore(std::move(d), cmd);
    EXPECT_EQ(cmd.args, args);
    const Decision again = Decide(cmd, flags, kNow, testing::LookupOf(keys));
    ASSERT_EQ(again.effects.size(), effects.size());
    for (size_t i = 0; i < effects.size(); ++i) EXPECT_EQ(again.effects[i].cmd.args, effects[i]);
  }
}

// --- DEL and UNLINK ---

TEST(DecideMultiKeyTest, DelCountsOnlyLiveKeys) {
  const Keys keys = {{"live", Str("v", 0, 3)},       {"stub", StubOf(Type::kSet)},
                     {"tomb", Tombstone(4)},         {"flushed", FlushAbsent(9)},
                     {"expired", Expired(Str("v"))}, {"expired_stub", ExpiredStub()}};
  for (const char* name : {"DEL", "UNLINK"}) {
    SCOPED_TRACE(name);
    const Decision d =
        DecideOn(keys, {name, "live", "stub", "tomb", "flushed", "expired", "expired_stub"});
    ExpectEffects(d.effects, {{.args = {"DEL", "live"}, .replaces_state = true},
                              {.args = {"DEL", "stub"}, .replaces_state = true},
                              {.args = {"DEL", "expired"}, .replaces_state = true},
                              {.args = {"DEL", "expired_stub"}, .replaces_state = true}});
    for (const auto& effect : d.effects) {
      const bool expired = effect.key == "expired" || effect.key == "expired_stub";
      EXPECT_EQ(effect.observed_expiry, expired) << effect.key;
    }
    EXPECT_EQ(Describe(d.reply), ":2");
    ASSERT_EQ(d.observed.size(), 6U);
    EXPECT_EQ(d.observed[0], (std::pair<std::string, core::SequenceId>{"live", 3}));
    EXPECT_EQ(d.observed[3], (std::pair<std::string, core::SequenceId>{"flushed", 9}));
  }
}

TEST(DecideMultiKeyTest, DelOfARepeatedKeyCountsOnce) {
  const Decision d = DecideOn(Keys{{"k", Str("v")}}, {"DEL", "k", "k"});
  ExpectEffects(d.effects, {{.args = {"DEL", "k"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":1");
}

TEST(DecideMultiKeyTest, DelLoadsKeysNoStubAnswers) {
  const Decision d =
      DecideOn(Keys{{"a", Str("v")}, {"b", kNonResident}}, {"DEL", "a", "b", "c", "b"});
  EXPECT_EQ(d.needs_load, Loads({"b", "c"}, Need::kExistence));
  EXPECT_TRUE(d.effects.empty());
  EXPECT_FALSE(d.reply.has_value());
}

// --- RENAMENX ---

TEST(DecideMultiKeyTest, RenamenxMovesTheSourceWithItsTtl) {
  const Decision d = DecideOn(Keys{{"src", Str("v", kTtl, 5)}, {"dst", Tombstone(8)}},
                              {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
  ExpectEffects(d.effects, {{.args = {"DEL", "src"}, .replaces_state = true},
                            {.args = {"SET", "dst", "v", "PXAT", std::to_string(kTtl)},
                             .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":1");
  EXPECT_EQ(d.observed,
            (std::vector<std::pair<std::string, core::SequenceId>>{{"src", 5}, {"dst", 8}}));
}

TEST(DecideMultiKeyTest, RenamenxOfAMissingSourceIsAnError) {
  for (const FakeKey& src : {Tombstone(), FlushAbsent()}) {
    const Decision d = DecideOn(Keys{{"src", src}, {"dst", Tombstone()}},
                                {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
    EXPECT_TRUE(d.effects.empty());
    EXPECT_EQ(Describe(d.reply), "-ERR no such key");
    EXPECT_FALSE(d.error.has_value()) << "a reply, not a refused command";
  }
  const Decision expired = DecideOn(Keys{{"src", Expired(SetOf({"m"}))}, {"dst", Tombstone()}},
                                    {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
  ExpectEffects(expired.effects, {{.args = {"DEL", "src"}, .replaces_state = true}});
  EXPECT_EQ(Describe(expired.reply), "-ERR no such key");
}

TEST(DecideMultiKeyTest, RenamenxOntoAPresentKeyIsRefused) {
  const Decision d = DecideOn(Keys{{"src", Str("v")}, {"dst", HashOf({{"f", "v"}})}},
                              {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
  EXPECT_TRUE(d.effects.empty());
  EXPECT_EQ(Describe(d.reply), ":0");
}

TEST(DecideMultiKeyTest, RenamenxOntoAnExpiredKeyReplacesIt) {
  const Decision d = DecideOn(Keys{{"src", Str("v")}, {"dst", Expired(Str("old"))}},
                              {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
  ExpectEffects(d.effects, {{.args = {"DEL", "dst"}, .replaces_state = true},
                            {.args = {"DEL", "src"}, .replaces_state = true},
                            {.args = {"SET", "dst", "v"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":1");
}

TEST(DecideMultiKeyTest, RenamenxToItself) {
  const Decision present =
      DecideOn(Keys{{"k", Str("v")}}, {"RENAMENX", "k", "k"}, PredicateFlags::kNx);
  EXPECT_TRUE(present.effects.empty());
  EXPECT_EQ(Describe(present.reply), ":0");
  const Decision absent =
      DecideOn(Keys{{"k", Tombstone()}}, {"RENAMENX", "k", "k"}, PredicateFlags::kNx);
  EXPECT_EQ(Describe(absent.reply), "-ERR no such key");
}

TEST(DecideMultiKeyTest, RenamenxLoadsTheSourceAndProbesTheDestination) {
  const Decision d = DecideOn(Keys{}, {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
  EXPECT_EQ(d.needs_load, (std::vector<KeyLoad>{{.key = "src", .need = Need::kState},
                                                {.key = "dst", .need = Need::kExistence}}));
  EXPECT_TRUE(d.effects.empty());
}

TEST(DecideMultiKeyTest, AStubDestinationIsPresent) {
  const Keys keys = {{"src", Str("v")}, {"dst", StubOf(Type::kZset)}};
  const Decision renamenx = DecideOn(keys, {"RENAMENX", "src", "dst"}, PredicateFlags::kNx);
  EXPECT_TRUE(renamenx.effects.empty());
  EXPECT_EQ(Describe(renamenx.reply), ":0");
  const Decision copy = DecideOn(keys, {"COPY", "src", "dst", "REPLACE"});
  ExpectEffects(copy.effects, {{.args = {"DEL", "dst"}, .replaces_state = true},
                               {.args = {"SET", "dst", "v"}, .replaces_state = true}});
}

// --- COPY ---

TEST(DecideMultiKeyTest, CopyOntoAnAbsentKey) {
  const Decision d = DecideOn(Keys{{"src", SetOf({"m"}, kTtl)}, {"dst", Tombstone()}},
                              {"COPY", "src", "dst"}, PredicateFlags::kNx);
  ExpectEffects(d.effects, {{.args = {"SADD", "dst", "m"}, .replaces_state = true},
                            {.args = {"PEXPIREAT", "dst", std::to_string(kTtl)}}});
  EXPECT_EQ(Describe(d.reply), ":1");
}

TEST(DecideMultiKeyTest, CopyOntoAPresentKeyNeedsReplace) {
  const Keys keys = {{"src", Str("v")}, {"dst", ZsetOf({{"a", 1}})}};
  const Decision refused = DecideOn(keys, {"COPY", "src", "dst"}, PredicateFlags::kNx);
  EXPECT_TRUE(refused.effects.empty());
  EXPECT_EQ(Describe(refused.reply), ":0");

  const Decision replaced = DecideOn(keys, {"COPY", "src", "dst", "replace"});
  ExpectEffects(replaced.effects, {{.args = {"DEL", "dst"}, .replaces_state = true},
                                   {.args = {"SET", "dst", "v"}, .replaces_state = true}});
  EXPECT_EQ(Describe(replaced.reply), ":1");
}

TEST(DecideMultiKeyTest, CopyOfAMissingSourceCopiesNothing) {
  const Decision d = DecideOn(Keys{{"src", Expired(Str("v"))}, {"dst", Tombstone()}},
                              {"COPY", "src", "dst", "DB", "0", "REPLACE"});
  ExpectEffects(d.effects, {{.args = {"DEL", "src"}, .replaces_state = true}});
  EXPECT_EQ(Describe(d.reply), ":0");
}

TEST(DecideMultiKeyTest, CopyRefusesBadOptionsAndItself) {
  const std::vector<std::pair<std::vector<std::string>, std::string>> cases = {
      {{"COPY", "k", "k"}, "source and destination objects are the same"},
      {{"COPY", "src", "dst", "BOGUS"}, "syntax error"},
      {{"COPY", "src", "dst", "DB"}, "syntax error"},
      {{"COPY", "src", "dst", "DB", "x"}, "value is not an integer or out of range"},
      {{"COPY", "src", "dst", "DB", "1"}, "DB index is out of range"}};
  for (const auto& [args, message] : cases) {
    SCOPED_TRACE(args.back());
    const Decision d =
        DecideOn(Keys{{"k", Str("v")}, {"src", Str("v")}, {"dst", Tombstone()}}, args);
    EXPECT_EQ(ErrorOf(d), core::ErrorCode::kInvalidArgument);
    EXPECT_EQ(d.error.has_value() ? d.error->message() : "", message);
    EXPECT_TRUE(d.effects.empty());
  }
}

// --- Decide then apply, against a real shard ---

void ExpectSameValue(const hot::KeyView& got, const hot::KeyView& want);

class DecideApplyTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  hot::SingleShardStore store_{hot::SingleShardConfig{}};
  core::EvictionPolicy policy_;
  core::SequenceId next_seq_ = 1;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  void Seed(const ops::WriteOp& op) {
    ASSERT_TRUE(store_.Apply(op, core::EvictionTTL{3600}, next_seq_++).has_value());
  }

  hot::KeyView View(std::string_view key) const { return store_.View(key, hot::kAllDrained, kNow); }

  // With `logged` applied to store_, each flagged effect is truthful:
  // from it on, the key's own effects rebuild the key from nothing, as
  // the replayer does.
  void ExpectFlagsTruthful(const std::vector<core::Effect>& logged) const {
    const core::WallTime at{std::chrono::milliseconds(kNow)};
    for (size_t i = 0; i < logged.size(); ++i) {
      if (!logged[i].replaces_state) continue;
      SCOPED_TRACE("flagged effect " + std::to_string(i) + ": " + logged[i].cmd.args[0]);
      std::vector<core::Effect> tail;
      for (size_t j = i; j < logged.size(); ++j) {
        if (logged[j].key == logged[i].key) tail.push_back(logged[j]);
      }
      hot::SingleShardStore alone{hot::SingleShardConfig{}};
      alone.ApplyEffects(tail, 1, at, policy_, hot::kAllDrained);
      const hot::KeyView want = View(logged[i].key);
      const hot::KeyView got = alone.View(logged[i].key, hot::kAllDrained, kNow);
      if (want.presence == Presence::kTombstoned) {
        EXPECT_EQ(got.presence, Presence::kTombstoned);
      } else {
        ExpectSameValue(got, want);
      }
    }
  }

  // The command's reply: the decision's, else its last effect's.
  std::string DecideAndApply(std::vector<std::string> args,
                             PredicateFlags flags = PredicateFlags::kNone) {
    core::RespCommand cmd{.args = std::move(args)};
    Decision d = Decide(cmd, flags, kNow, [this](std::string_view key) { return View(key); });
    EXPECT_TRUE(d.needs_load.empty());
    EXPECT_FALSE(d.error.has_value());
    const std::vector<core::Effect> logged = d.effects;
    const auto replies =
        store_.ApplyEffects(d.effects, next_seq_, core::WallTime(std::chrono::milliseconds(kNow)),
                            policy_, hot::kAllDrained);
    next_seq_ += d.effects.size();
    ExpectFlagsTruthful(logged);
    if (d.reply.has_value()) return Describe(d.reply);
    EXPECT_FALSE(replies.empty()) << "an empty reply needs an effect to give it";
    return replies.empty() ? "" : Describe(replies.back());
  }
};

TEST_F(DecideApplyTest, ApplyAnswersWhenTheDecisionLeavesTheReply) {
  Seed(ops::SetAdd{.key = "s", .members = {"a"}});
  Seed(ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "v"}}});
  Seed(ops::ZsetAdd{.key = "z", .entries = {{.score = 1, .member = "a"}}});
  Seed(ops::StringSet{.key = "k", .value = "old"});
  Seed(ops::Del{.keys = {"fresh"}});

  EXPECT_EQ(DecideAndApply({"SADD", "s", "a", "b", "c"}), ":2");
  EXPECT_EQ(DecideAndApply({"SREM", "s", "a", "z"}), ":1");
  EXPECT_EQ(DecideAndApply({"HSET", "h", "f", "w", "g", "x"}), ":1");
  EXPECT_EQ(DecideAndApply({"HMSET", "h", "f", "w"}), "+OK");
  EXPECT_EQ(DecideAndApply({"HDEL", "h", "f", "nope"}), ":1");
  EXPECT_EQ(DecideAndApply({"ZADD", "z", "2", "a", "3", "b", "4", "b"}), ":1");
  EXPECT_EQ(DecideAndApply({"ZREM", "z", "a"}), ":1");
  EXPECT_EQ(DecideAndApply({"PEXPIRE", "k", "60000"}), ":1");
  EXPECT_EQ(DecideAndApply({"PERSIST", "k"}), ":1");
  EXPECT_EQ(DecideAndApply({"SET", "k", "new", "XX"}, PredicateFlags::kXx), "+OK");
  EXPECT_EQ(DecideAndApply({"SET", "k", "newer", "GET"}, PredicateFlags::kGet), "$new");
  EXPECT_EQ(DecideAndApply({"SET", "fresh", "v", "GET"}, PredicateFlags::kGet), "nil");
  EXPECT_EQ(View("k").string_value(), "newer");
}

TEST_F(DecideApplyTest, ExpiredKeyIsDeletedThenRecreated) {
  Seed(ops::SetAdd{.key = "s", .members = {"old"}});
  Seed(ops::Expire{.key = "s", .abs_ttl_ms = kNow - 1});
  ASSERT_EQ(View("s").presence, Presence::kExpired);
  EXPECT_EQ(DecideAndApply({"SADD", "s", "new"}), ":1");
  const hot::KeyView s = View("s");
  EXPECT_EQ(s.presence, Presence::kLive);
  EXPECT_EQ(s.collection_size(), 1U);
  EXPECT_TRUE(s.set_has("new"));
  EXPECT_EQ(s.abs_ttl_ms, 0);
}

struct Source {
  std::string name;
  ops::WriteOp op;
};

std::vector<Source> Sources() {
  return {
      {"String", ops::StringSet{.key = "src", .value = "v"}},
      {"Set", ops::SetAdd{.key = "src", .members = {"a", "b", "c"}}},
      {"Zset", ops::ZsetAdd{.key = "src",
                            .entries = {{.score = 2, .member = "a"},
                                        {.score = 1, .member = "b"},
                                        {.score = 1, .member = "c"}}}},
      {"Hash",
       ops::HashSet{.key = "src",
                    .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}}},
  };
}

void ExpectSameValue(const hot::KeyView& got, const hot::KeyView& want) {
  ASSERT_EQ(got.presence, Presence::kLive);
  ASSERT_NE(got.value, nullptr);
  ASSERT_NE(want.value, nullptr);
  EXPECT_EQ(got.type, want.type);
  EXPECT_EQ(got.abs_ttl_ms, want.abs_ttl_ms);
  std::visit(
      [&want](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        const auto& expected = std::get<T>(*want.value);
        if constexpr (std::is_same_v<T, std::string>) {
          EXPECT_EQ(value, expected);
        } else if constexpr (std::is_same_v<T, hot::SetValue>) {
          EXPECT_EQ(value.members, expected.members);
        } else if constexpr (std::is_same_v<T, hot::ZsetValue>) {
          EXPECT_EQ(value.member_scores, expected.member_scores);
          EXPECT_EQ(value.score_members, expected.score_members);
        } else {
          EXPECT_EQ(value.fields, expected.fields);
        }
      },
      *got.value);
}

// Every type, with and without a TTL, renamed, copied, and copied over
// an existing key.
TEST_F(DecideApplyTest, RenamenxAndCopyRecreateEveryType) {
  for (const auto& source : Sources()) {
    for (const uint64_t ttl : {uint64_t{0}, static_cast<uint64_t>(kTtl)}) {
      for (const std::string_view mode : {"RENAMENX", "COPY", "COPY REPLACE"}) {
        SCOPED_TRACE(source.name + (ttl > 0 ? " with TTL, " : ", ") + std::string(mode));
        store_.Wipe(0);
        Seed(source.op);
        if (ttl > 0) Seed(ops::Expire{.key = "src", .abs_ttl_ms = ttl});
        const bool replace = mode == "COPY REPLACE";
        if (replace) {
          Seed(ops::SetAdd{.key = "dst", .members = {"stale"}});
        } else {
          Seed(ops::Del{.keys = {"dst"}});
        }
        // The source's value, copied before the command changes it.
        hot::SingleShardStore before{hot::SingleShardConfig{}};
        ASSERT_TRUE(before.Apply(source.op, core::EvictionTTL{3600}, 1).has_value());
        if (ttl > 0) {
          ASSERT_TRUE(
              before.Apply(ops::Expire{.key = "src", .abs_ttl_ms = ttl}, core::EvictionTTL{3600}, 2)
                  .has_value());
        }

        std::vector<std::string> args = {mode == "RENAMENX" ? "RENAMENX" : "COPY", "src", "dst"};
        if (replace) args.emplace_back("REPLACE");
        core::RespCommand cmd{.args = args};
        Decision d = Decide(cmd, PredicateFlags::kNx, kNow,
                            [this](std::string_view key) { return View(key); });
        ASSERT_FALSE(d.error.has_value());
        EXPECT_EQ(Describe(d.reply), ":1");
        for (const auto& e : d.effects) {
          EXPECT_EQ(e.replaces_state, e.cmd.args[0] != "PEXPIREAT") << e.cmd.args[0];
        }
        const bool rename = mode == "RENAMENX";
        ASSERT_FALSE(d.effects.empty());
        if (rename) {
          EXPECT_EQ(d.effects.front().cmd.args, (std::vector<std::string>{"DEL", "src"}));
        }
        if (replace) {
          EXPECT_EQ(d.effects.front().cmd.args, (std::vector<std::string>{"DEL", "dst"}));
        }
        const bool pexpireat = d.effects.back().cmd.args[0] == "PEXPIREAT";
        EXPECT_EQ(pexpireat, ttl > 0 && source.name != "String");

        const std::vector<core::Effect> logged = d.effects;
        store_.ApplyEffects(d.effects, next_seq_, core::WallTime(std::chrono::milliseconds(kNow)),
                            policy_, hot::kAllDrained);
        next_seq_ += d.effects.size();
        ExpectFlagsTruthful(logged);
        ExpectSameValue(View("dst"), before.View("src", hot::kAllDrained, kNow));
        if (rename) {
          EXPECT_EQ(View("src").presence, Presence::kTombstoned);
        } else {
          ExpectSameValue(View("src"), before.View("src", hot::kAllDrained, kNow));
        }
      }
    }
  }
}

}  // namespace
}  // namespace abyss::engine
