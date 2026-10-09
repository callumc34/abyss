#include "abyss/core/ops.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

namespace abyss::core::ops {
namespace {

TEST(ParseReadOpTest, ParsesGet) {
  RespCommand cmd{{"GET", "mykey"}};
  auto op = ParseReadOp("GET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* get = std::get_if<StringGet>(&*op);
  ASSERT_NE(get, nullptr);
  EXPECT_EQ(get->key, "mykey");
}

// MGET / EXISTS are decomposed in the engine before parse; ParseReadOp doesn't
// know about them. See TieringEngine::DispatchFanOut.
TEST(ParseReadOpTest, MgetNotRegistered) {
  RespCommand cmd{{"MGET", "k1"}};
  EXPECT_FALSE(ParseReadOp("MGET", cmd).has_value());
}

TEST(ParseReadOpTest, ParsesSismember) {
  RespCommand cmd{{"SISMEMBER", "myset", "member"}};
  auto op = ParseReadOp("SISMEMBER", cmd);
  ASSERT_TRUE(op.has_value());
  auto* sis = std::get_if<SetIsMember>(&*op);
  ASSERT_NE(sis, nullptr);
  EXPECT_EQ(sis->key, "myset");
  EXPECT_EQ(sis->member, "member");
}

TEST(ParseReadOpTest, RejectsUnknownCommand) {
  RespCommand cmd{{"XYZZY", "key"}};
  auto op = ParseReadOp("XYZZY", cmd);
  ASSERT_FALSE(op.has_value());
}

TEST(ParseReadOpTest, ZrangePlainIsIndexMode) {
  RespCommand cmd{{"ZRANGE", "z", "0", "-1"}};
  auto op = ParseReadOp("ZRANGE", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zr = std::get_if<ZsetRange>(&*op);
  ASSERT_NE(zr, nullptr);
  EXPECT_EQ(zr->key, "z");
  EXPECT_EQ(zr->min, "0");
  EXPECT_EQ(zr->max, "-1");
  EXPECT_FALSE(zr->by_score);
  EXPECT_FALSE(zr->by_lex);
}

TEST(ParseReadOpTest, ZrangeByScoreFlagAndWithScores) {
  RespCommand cmd{{"ZRANGE", "z", "1", "3", "BYSCORE", "WITHSCORES"}};
  auto op = ParseReadOp("ZRANGE", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zr = std::get_if<ZsetRange>(&*op);
  ASSERT_NE(zr, nullptr);
  EXPECT_TRUE(zr->by_score);
  EXPECT_FALSE(zr->by_lex);
  EXPECT_TRUE(zr->with_scores);
}

TEST(ParseReadOpTest, ZrangeByLexWithLimitAndRev) {
  RespCommand cmd{{"ZRANGE", "z", "[a", "(c", "BYLEX", "REV", "LIMIT", "1", "2"}};
  auto op = ParseReadOp("ZRANGE", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zr = std::get_if<ZsetRange>(&*op);
  ASSERT_NE(zr, nullptr);
  EXPECT_TRUE(zr->by_lex);
  EXPECT_FALSE(zr->by_score);
  EXPECT_TRUE(zr->rev);
  EXPECT_EQ(zr->offset, 1);
  EXPECT_EQ(zr->count, 2);
}

TEST(ParseReadOpTest, ZrangeByScoreCommandSetsByScore) {
  RespCommand cmd{{"ZRANGEBYSCORE", "z", "-inf", "+inf"}};
  auto op = ParseReadOp("ZRANGEBYSCORE", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zr = std::get_if<ZsetRange>(&*op);
  ASSERT_NE(zr, nullptr);
  EXPECT_TRUE(zr->by_score);
  EXPECT_FALSE(zr->by_lex);
}

TEST(ParseReadOpTest, ZrangeByLexCommandSetsByLex) {
  RespCommand cmd{{"ZRANGEBYLEX", "z", "[b", "(d"}};
  auto op = ParseReadOp("ZRANGEBYLEX", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zr = std::get_if<ZsetRange>(&*op);
  ASSERT_NE(zr, nullptr);
  EXPECT_TRUE(zr->by_lex);
  EXPECT_FALSE(zr->by_score);
  EXPECT_EQ(zr->min, "[b");
  EXPECT_EQ(zr->max, "(d");
}

TEST(ParseReadOpTest, ZrangeBySCoreAndByLexMutuallyExclusive) {
  RespCommand cmd{{"ZRANGE", "z", "0", "1", "BYSCORE", "BYLEX"}};
  auto op = ParseReadOp("ZRANGE", cmd);
  EXPECT_FALSE(op.has_value());
}

TEST(ParseReadOpTest, ZrangeLimitRequiresTwoArgs) {
  RespCommand cmd{{"ZRANGE", "z", "0", "1", "LIMIT", "5"}};
  auto op = ParseReadOp("ZRANGE", cmd);
  EXPECT_FALSE(op.has_value());
}

TEST(ParseWriteOpTest, ParsesSet) {
  RespCommand cmd{{"SET", "k", "v"}};
  auto op = ParseWriteOp("SET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* set = std::get_if<StringSet>(&*op);
  ASSERT_NE(set, nullptr);
  EXPECT_EQ(set->key, "k");
  EXPECT_EQ(set->value, "v");
  EXPECT_EQ(set->abs_ttl_ms, 0U);
}

TEST(ParseWriteOpTest, ParsesDel) {
  RespCommand cmd{{"DEL", "a", "b", "c"}};
  auto op = ParseWriteOp("DEL", cmd);
  ASSERT_TRUE(op.has_value());
  auto* del = std::get_if<Del>(&*op);
  ASSERT_NE(del, nullptr);
  EXPECT_EQ(del->keys.size(), 3U);
}

TEST(ParseWriteOpTest, MsetNotRegistered) {
  RespCommand cmd{{"MSET", "k1", "v1"}};
  EXPECT_FALSE(ParseWriteOp("MSET", cmd).has_value());
}

TEST(ParseWriteOpTest, ParsesSadd) {
  RespCommand cmd{{"SADD", "myset", "a", "b"}};
  auto op = ParseWriteOp("SADD", cmd);
  ASSERT_TRUE(op.has_value());
  auto* sadd = std::get_if<SetAdd>(&*op);
  ASSERT_NE(sadd, nullptr);
  EXPECT_EQ(sadd->key, "myset");
  EXPECT_EQ(sadd->members.size(), 2U);
}

TEST(ParseWriteOpTest, ParsesZadd) {
  RespCommand cmd{{"ZADD", "zset", "1.5", "m1", "2.5", "m2"}};
  auto op = ParseWriteOp("ZADD", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zadd = std::get_if<ZsetAdd>(&*op);
  ASSERT_NE(zadd, nullptr);
  EXPECT_EQ(zadd->key, "zset");
  EXPECT_EQ(zadd->entries.size(), 2U);
  EXPECT_DOUBLE_EQ(zadd->entries[0].score, 1.5);
  EXPECT_EQ(zadd->entries[1].member, "m2");
}

TEST(ParseWriteOpTest, ParsesZaddWithFlags) {
  RespCommand cmd{{"ZADD", "zset", "NX", "GT", "1.5", "m1"}};
  auto op = ParseWriteOp("ZADD", cmd);
  ASSERT_TRUE(op.has_value());
  auto* zadd = std::get_if<ZsetAdd>(&*op);
  ASSERT_NE(zadd, nullptr);
  EXPECT_EQ(zadd->entries.size(), 1U);
  EXPECT_DOUBLE_EQ(zadd->entries[0].score, 1.5);
}

TEST(ParseWriteOpTest, ParsesHset) {
  RespCommand cmd{{"HSET", "h", "f1", "v1", "f2", "v2"}};
  auto op = ParseWriteOp("HSET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* hset = std::get_if<HashSet>(&*op);
  ASSERT_NE(hset, nullptr);
  EXPECT_EQ(hset->key, "h");
  EXPECT_EQ(hset->fields.size(), 2U);
}

TEST(ParseWriteOpTest, MsetRejectsWrongParity) {
  RespCommand cmd{{"MSET", "k1", "v1", "k2"}};
  auto op = ParseWriteOp("MSET", cmd);
  ASSERT_FALSE(op.has_value());
}

TEST(ParseWriteOpTest, ZaddRejectsUnpairedScoreMember) {
  RespCommand cmd{{"ZADD", "zset", "1.5"}};
  auto op = ParseWriteOp("ZADD", cmd);
  ASSERT_FALSE(op.has_value());
}

// A NaN score would break the zset's score order.
TEST(ParseWriteOpTest, ZaddRejectsANanScore) {
  for (const char* score : {"nan", "NaN", "-nan", "NAN"}) {
    RespCommand cmd{{"ZADD", "zset", "1", "a", score, "m"}};
    auto op = ParseWriteOp("ZADD", cmd);
    ASSERT_FALSE(op.has_value()) << score;
    EXPECT_EQ(op.error().code(), ErrorCode::kInvalidArgument);
    EXPECT_EQ(op.error().message(), "value is not a valid float");
  }
  RespCommand inf{{"ZADD", "zset", "inf", "a", "-inf", "b"}};
  EXPECT_TRUE(ParseWriteOp("ZADD", inf).has_value()) << "infinities still parse";
}

TEST(ParseWriteOpTest, HsetRejectsOddFieldValues) {
  RespCommand cmd{{"HSET", "h", "f1", "v1", "f2"}};
  auto op = ParseWriteOp("HSET", cmd);
  ASSERT_FALSE(op.has_value());
}

TEST(ParseWriteOpTest, ParsesHmset) {
  RespCommand cmd{{"HMSET", "h", "f1", "v1", "f2", "v2"}};
  auto op = ParseWriteOp("HMSET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* hmset = std::get_if<HashMSet>(&*op);
  ASSERT_NE(hmset, nullptr);
  EXPECT_EQ(hmset->key, "h");
  EXPECT_EQ(hmset->fields.size(), 2U);
  EXPECT_EQ(hmset->fields[0].field, "f1");
  EXPECT_EQ(hmset->fields[0].value, "v1");
}

TEST(ParseWriteOpTest, HmsetRejectsOddFieldValues) {
  RespCommand cmd{{"HMSET", "h", "f1", "v1", "f2"}};
  auto op = ParseWriteOp("HMSET", cmd);
  ASSERT_FALSE(op.has_value());
}

TEST(ParseReadOpTest, ParsesHmget) {
  RespCommand cmd{{"HMGET", "h", "f1", "f2", "f3"}};
  auto op = ParseReadOp("HMGET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* hmget = std::get_if<HashMultiGet>(&*op);
  ASSERT_NE(hmget, nullptr);
  EXPECT_EQ(hmget->key, "h");
  EXPECT_EQ(hmget->fields.size(), 3U);
}

TEST(ParseReadOpTest, ParsesHexists) {
  RespCommand cmd{{"HEXISTS", "h", "f"}};
  auto op = ParseReadOp("HEXISTS", cmd);
  ASSERT_TRUE(op.has_value());
  auto* hex = std::get_if<HashFieldExists>(&*op);
  ASSERT_NE(hex, nullptr);
  EXPECT_EQ(hex->key, "h");
  EXPECT_EQ(hex->field, "f");
}

TEST(ParseReadOpTest, ParsesHkeysHvalsHlen) {
  {
    RespCommand cmd{{"HKEYS", "h"}};
    auto op = ParseReadOp("HKEYS", cmd);
    ASSERT_TRUE(op.has_value());
    auto* k = std::get_if<HashKeys>(&*op);
    ASSERT_NE(k, nullptr);
    EXPECT_EQ(k->key, "h");
  }
  {
    RespCommand cmd{{"HVALS", "h"}};
    auto op = ParseReadOp("HVALS", cmd);
    ASSERT_TRUE(op.has_value());
    auto* v = std::get_if<HashVals>(&*op);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->key, "h");
  }
  {
    RespCommand cmd{{"HLEN", "h"}};
    auto op = ParseReadOp("HLEN", cmd);
    ASSERT_TRUE(op.has_value());
    auto* l = std::get_if<HashLen>(&*op);
    ASSERT_NE(l, nullptr);
    EXPECT_EQ(l->key, "h");
  }
}

TEST(PrimaryKeyTest, ExtractsFromNewHashReads) {
  EXPECT_EQ(PrimaryKey(ReadOp{HashKeys{.key = "h"}}), "h");
  EXPECT_EQ(PrimaryKey(ReadOp{HashVals{.key = "h"}}), "h");
  EXPECT_EQ(PrimaryKey(ReadOp{HashLen{.key = "h"}}), "h");
  EXPECT_EQ(PrimaryKey(ReadOp{HashFieldExists{.key = "h", .field = "f"}}), "h");
  HashMultiGet hmget;
  hmget.key = "h";
  hmget.fields = {std::string_view("a"), std::string_view("b")};
  EXPECT_EQ(PrimaryKey(ReadOp{hmget}), "h");
}

TEST(PrimaryKeyTest, ExtractsFromHashMSet) {
  EXPECT_EQ(PrimaryKey(WriteOp{HashMSet{.key = "h"}}), "h");
}

TEST(PrimaryKeyTest, ExtractsFromReadOps) {
  EXPECT_EQ(PrimaryKey(ReadOp{StringGet{.key = "k"}}), "k");
  EXPECT_EQ(PrimaryKey(ReadOp{SetMembers{.key = "s"}}), "s");

  Exists exists;
  const auto keys = std::to_array<std::string_view>({"a", "b"});
  exists.keys = {keys.begin(), keys.end()};
  EXPECT_EQ(PrimaryKey(ReadOp{exists}), "a");
}

TEST(PrimaryKeyTest, ExtractsFromWriteOps) {
  EXPECT_EQ(PrimaryKey(WriteOp{StringSet{.key = "k", .value = "v"}}), "k");

  Del del;
  const auto keys = std::to_array<std::string_view>({"x", "y"});
  del.keys = {keys.begin(), keys.end()};
  EXPECT_EQ(PrimaryKey(WriteOp{del}), "x");
}

// --- CanonicalCommand -------------------------------------------------------
//
// The canonical form is what reaches the WAL, so its contract is a round trip:
// re-parsing it must reproduce the op the client's spelling produced. That is
// what makes canonicalising invisible to hot, to cold, and to the reply -- all
// three are pure functions of the WriteOp.

// Every spelling in this table must collapse to the same canonical bytes AND
// the same op. `now_ms` is fixed so relative TTLs are comparable.
constexpr uint64_t kNow = 1'700'000'000'000;

void ExpectRoundTrip(const RespCommand& original) {
  auto op = ParseWriteOp(original.Name(), original, kNow);
  ASSERT_TRUE(op.has_value()) << original.Name();
  const auto canonical = CanonicalCommand(*op);
  auto reparsed = ParseWriteOp(canonical.Name(), canonical, kNow);
  ASSERT_TRUE(reparsed.has_value()) << "canonical form does not re-parse: " << canonical.Name();
  EXPECT_EQ(op->index(), reparsed->index()) << "canonical form changed the op variant";
  EXPECT_EQ(CanonicalCommand(*reparsed).args, canonical.args)
      << "canonicalisation is not idempotent";
}

TEST(CanonicalCommandTest, RoundTripsEveryWriteOp) {
  const std::vector<RespCommand> cases{
      RespCommand{{"SET", "k", "v"}},
      RespCommand{{"SET", "k", "v", "EX", "60"}},
      RespCommand{{"SET", "k", "v", "PXAT", "1700000060000"}},
      RespCommand{{"SETEX", "k", "60", "v"}},
      RespCommand{{"PSETEX", "k", "60000", "v"}},
      RespCommand{{"DEL", "k"}},
      RespCommand{{"UNLINK", "k"}},
      RespCommand{{"SADD", "s", "a", "b"}},
      RespCommand{{"SREM", "s", "a"}},
      RespCommand{{"ZADD", "z", "1.5", "m", "2", "n"}},
      RespCommand{{"ZREM", "z", "m"}},
      RespCommand{{"HSET", "h", "f", "v"}},
      RespCommand{{"HMSET", "h", "f", "v"}},
      RespCommand{{"HDEL", "h", "f"}},
      RespCommand{{"EXPIRE", "k", "60"}},
      RespCommand{{"PEXPIRE", "k", "60000"}},
      RespCommand{{"EXPIREAT", "k", "1700000060"}},
      RespCommand{{"PEXPIREAT", "k", "1700000060000"}},
      RespCommand{{"PERSIST", "k"}},
  };
  for (const auto& c : cases) {
    ExpectRoundTrip(c);
  }
}

// WriteOp holds views into the command it was parsed from, so the canonical
// command must outlive anything parsed out of it. Named locals, never a
// temporary threaded straight into ParseWriteOp.
RespCommand Canonicalise(const RespCommand& original) {
  auto op = ParseWriteOp(original.Name(), original, kNow);
  EXPECT_TRUE(op.has_value()) << original.Name();
  return op.has_value() ? CanonicalCommand(*op) : RespCommand{};
}

// The four TTL spellings differ only in how they say "when"; canonicalising
// resolves that to one absolute instant, so the WAL carries one form.
TEST(CanonicalCommandTest, TtlSpellingsCollapseToPxat) {
  const RespCommand expected{{"SET", "k", "v", "PXAT", std::to_string(kNow + 60000)}};
  EXPECT_EQ(Canonicalise(RespCommand{{"SET", "k", "v", "EX", "60"}}).args, expected.args);
  EXPECT_EQ(Canonicalise(RespCommand{{"SET", "k", "v", "PX", "60000"}}).args, expected.args);
  EXPECT_EQ(Canonicalise(RespCommand{{"SETEX", "k", "60", "v"}}).args, expected.args);
}

// HMSET replies +OK where HSET replies with a count, so they are different ops
// and must stay different commands. Collapsing them would corrupt the reply.
TEST(CanonicalCommandTest, HmsetIsNotCollapsedToHset) {
  EXPECT_EQ(Canonicalise(RespCommand{{"HSET", "h", "f", "v"}}).Name(), "HSET");
  EXPECT_EQ(Canonicalise(RespCommand{{"HMSET", "h", "f", "v"}}).Name(), "HMSET");
}

// Scores go through double -> text -> double on the way to the WAL. to_string
// would round to 6 decimals; the shortest-round-trip form must not.
TEST(CanonicalCommandTest, ZaddScoresSurviveTextRoundTrip) {
  const RespCommand original{
      {"ZADD", "z", "3.141592653589793", "pi", "-0.1", "neg", "1e300", "big"}};
  auto op = ParseWriteOp("ZADD", original, kNow);
  ASSERT_TRUE(op.has_value());
  const auto canonical = CanonicalCommand(*op);
  auto reparsed = ParseWriteOp("ZADD", canonical, kNow);
  ASSERT_TRUE(reparsed.has_value());

  const auto& before = std::get<ZsetAdd>(*op).entries;
  const auto& after = std::get<ZsetAdd>(*reparsed).entries;
  ASSERT_EQ(before.size(), after.size());
  for (size_t i = 0; i < before.size(); ++i) {
    EXPECT_DOUBLE_EQ(before[i].score, after[i].score);
    EXPECT_EQ(before[i].member, after[i].member);
  }
}

// Expire{0} means "expire now"; Persist means "clear the TTL". The canonical
// forms must keep them apart or a PERSIST replays as an immediate deletion.
TEST(CanonicalCommandTest, ExpireZeroIsDistinctFromPersist) {
  const auto expire_now = CanonicalCommand(WriteOp{Expire{.key = "k", .abs_ttl_ms = 0}});
  const auto persist = CanonicalCommand(WriteOp{Persist{.key = "k"}});
  EXPECT_NE(expire_now.Name(), persist.Name());

  auto reparsed = ParseWriteOp(expire_now.Name(), expire_now, kNow);
  ASSERT_TRUE(reparsed.has_value());
  EXPECT_TRUE(std::holds_alternative<Expire>(*reparsed));
}

uint64_t ParsedTtl(const RespCommand& cmd) {
  auto op = ParseWriteOp(cmd.Name(), cmd, kNow);
  if (!op.has_value()) {
    ADD_FAILURE() << cmd.Name() << ": " << op.error().message();
    return 0;
  }
  if (const auto* expire = std::get_if<Expire>(&*op)) return expire->abs_ttl_ms;
  return std::get<StringSet>(*op).abs_ttl_ms;
}

std::string ParseError(const RespCommand& cmd) {
  auto op = ParseWriteOp(cmd.Name(), cmd, kNow);
  return op.has_value() ? "" : op.error().message();
}

TEST(ParseWriteOpTest, SetFamilyRejectsAnExpiryOfZeroOrLess) {
  for (const std::string ttl : {"0", "-1"}) {
    for (const std::string opt : {"EX", "PX", "EXAT", "PXAT"}) {
      EXPECT_EQ(ParseError(RespCommand{{"SET", "k", "v", opt, ttl}}),
                "invalid expire time in 'set' command")
          << opt << " " << ttl;
    }
  }
  EXPECT_EQ(ParseError(RespCommand{{"SETEX", "k", "0", "v"}}),
            "invalid expire time in 'setex' command");
  EXPECT_EQ(ParseError(RespCommand{{"PSETEX", "k", "-5", "v"}}),
            "invalid expire time in 'psetex' command");
}

TEST(ParseWriteOpTest, SetRejectsConflictingOptions) {
  const std::vector<RespCommand> cases{
      RespCommand{{"SET", "k", "v", "EX", "1", "PX", "1"}},
      RespCommand{{"SET", "k", "v", "EX", "1", "KEEPTTL"}},
      RespCommand{{"SET", "k", "v", "KEEPTTL", "PXAT", "1"}},
      RespCommand{{"SET", "k", "v", "NX", "XX"}},
      RespCommand{{"SET", "k", "v", "EX"}},
  };
  for (const auto& cmd : cases) {
    EXPECT_EQ(ParseError(cmd), "syntax error") << cmd.args.size();
  }
  EXPECT_EQ(ParsedTtl(RespCommand{{"SET", "k", "v", "NX", "GET", "KEEPTTL"}}), 0U);
}

// A past time stays a time (0 would mean no TTL), so the key expires
// rather than persisting.
TEST(ParseWriteOpTest, ExpireAtOrBeforeNowIsPast) {
  EXPECT_EQ(ParsedTtl(RespCommand{{"EXPIREAT", "k", "0"}}), 1U);
  EXPECT_EQ(ParsedTtl(RespCommand{{"PEXPIREAT", "k", "-1"}}), 1U);
  EXPECT_EQ(ParsedTtl(RespCommand{{"PEXPIRE", "k", "0"}}), kNow);
  EXPECT_EQ(ParsedTtl(RespCommand{{"EXPIRE", "k", "-5"}}), kNow - 5000);
  EXPECT_EQ(ParsedTtl(RespCommand{{"EXPIRE", "k", "-9223372036854775"}}), 1U);
}

TEST(ParseWriteOpTest, ExpiryOverflowIsRejected) {
  const std::string max = "9223372036854775807";
  EXPECT_EQ(ParseError(RespCommand{{"EXPIRE", "k", max}}),
            "invalid expire time in 'expire' command");
  EXPECT_EQ(ParseError(RespCommand{{"PEXPIRE", "k", max}}),
            "invalid expire time in 'pexpire' command");
  EXPECT_EQ(ParseError(RespCommand{{"EXPIREAT", "k", "-9223372036854775807"}}),
            "invalid expire time in 'expireat' command");
  EXPECT_EQ(ParseError(RespCommand{{"SET", "k", "v", "EX", max}}),
            "invalid expire time in 'set' command");
  EXPECT_EQ(ParsedTtl(RespCommand{{"PEXPIREAT", "k", max}}), 9223372036854775807U);
}

// Redis 7's texts.
TEST(ParseWriteOpTest, ErrorsUseRedisTexts) {
  EXPECT_EQ(ParseError(RespCommand{{"SET", "k", "v", "BOGUS"}}), "syntax error");
  EXPECT_EQ(ParseError(RespCommand{{"ZADD", "z", "1"}}), "syntax error");
  EXPECT_EQ(ParseError(RespCommand{{"ZADD", "z", "NX", "1", "a", "2"}}), "syntax error");
  EXPECT_EQ(ParseError(RespCommand{{"HSET", "h", "f", "v", "g"}}),
            "wrong number of arguments for 'hset' command");
  EXPECT_EQ(ParseError(RespCommand{{"HMSET", "h", "f", "v", "g"}}),
            "wrong number of arguments for 'hmset' command");
  for (const auto& cmd :
       {RespCommand{{"EXPIRE", "k", "ten"}}, RespCommand{{"PEXPIREAT", "k", "1.5"}},
        RespCommand{{"SET", "k", "v", "PX", "x"}}, RespCommand{{"SETEX", "k", "", "v"}}}) {
    EXPECT_EQ(ParseError(cmd), "value is not an integer or out of range") << cmd.Name();
  }
}

TEST(ParseReadOpTest, ErrorsAreAscii) {
  const auto error = [](const RespCommand& cmd) {
    auto op = ParseReadOp(cmd.Name(), cmd);
    return op.has_value() ? "" : op.error().message();
  };
  for (const auto& cmd : {RespCommand{{"ZRANGE", "z", "0", "1", "BYSCORE", "BYLEX"}},
                          RespCommand{{"ZRANGE", "z", "0", "1", "LIMIT", "5"}},
                          RespCommand{{"ZRANGE", "z", "0", "1", "BOGUS"}},
                          RespCommand{{"ZRANGEBYSCORE", "z", "0", "1", "BOGUS"}},
                          RespCommand{{"ZRANGEBYLEX", "z", "a", "b", "WITHSCORES"}}}) {
    EXPECT_EQ(error(cmd), "syntax error") << cmd.Name();
  }
  EXPECT_EQ(error(RespCommand{{"ZRANGE", "z", "0", "1", "BYSCORE", "LIMIT", "x", "1"}}),
            "value is not an integer or out of range");
}

}  // namespace
}  // namespace abyss::core::ops
