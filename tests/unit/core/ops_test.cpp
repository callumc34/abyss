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

TEST(ParseReadOpTest, ParsesMget) {
  RespCommand cmd{{"MGET", "k1", "k2", "k3"}};
  auto op = ParseReadOp("MGET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* mget = std::get_if<MultiStringGet>(&*op);
  ASSERT_NE(mget, nullptr);
  EXPECT_EQ(mget->keys.size(), 3U);
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

TEST(ParseWriteOpTest, ParsesMset) {
  RespCommand cmd{{"MSET", "k1", "v1", "k2", "v2"}};
  auto op = ParseWriteOp("MSET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* mset = std::get_if<MultiStringSet>(&*op);
  ASSERT_NE(mset, nullptr);
  EXPECT_EQ(mset->entries.size(), 2U);
  EXPECT_EQ(mset->entries[0].key, "k1");
  EXPECT_EQ(mset->entries[1].value, "v2");
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

TEST(PrimaryKeyTest, ExtractsFromReadOps) {
  EXPECT_EQ(PrimaryKey(ReadOp{StringGet{.key = "k"}}), "k");
  EXPECT_EQ(PrimaryKey(ReadOp{SetMembers{.key = "s"}}), "s");

  MultiStringGet mget;
  mget.keys = {std::string_view("a"), std::string_view("b")};
  EXPECT_EQ(PrimaryKey(ReadOp{mget}), "a");
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
