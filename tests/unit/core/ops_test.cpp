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

}  // namespace
}  // namespace abyss::core::ops
