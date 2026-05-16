#include <set>
#include <string>

#include "server_fixture.h"

namespace abyss::system_test {
namespace {

// --- String operations ------------------------------------------------------

class StringOpsTest : public DataCommandTest {};

TEST_F(StringOpsTest, SetAndGet) {
  EXPECT_TRUE(Client().Command({"SET", "k", "hello"}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "hello");
}

TEST_F(StringOpsTest, SetOverwrite) {
  EXPECT_TRUE(Client().Command({"SET", "k", "v1"}).IsOk());
  EXPECT_TRUE(Client().Command({"SET", "k", "v2"}).IsOk());
  EXPECT_EQ(Client().Command({"GET", "k"}).String(), "v2");
}

TEST_F(StringOpsTest, GetNonexistent) { EXPECT_TRUE(Client().Command({"GET", "missing"}).IsNil()); }

TEST_F(StringOpsTest, SetExSeconds) {
  EXPECT_TRUE(Client().Command({"SET", "k", "v", "EX", "3600"}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "v");
}

TEST_F(StringOpsTest, SetPxMilliseconds) {
  EXPECT_TRUE(Client().Command({"SET", "k", "v", "PX", "3600000"}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "v");
}

TEST_F(StringOpsTest, Setex) {
  EXPECT_TRUE(Client().Command({"SETEX", "k", "3600", "v"}).IsOk());
  EXPECT_EQ(Client().Command({"GET", "k"}).String(), "v");
}

TEST_F(StringOpsTest, Psetex) {
  EXPECT_TRUE(Client().Command({"PSETEX", "k", "3600000", "v"}).IsOk());
  EXPECT_EQ(Client().Command({"GET", "k"}).String(), "v");
}

TEST_F(StringOpsTest, MsetAndMget) {
  EXPECT_TRUE(Client().Command({"MSET", "a", "1", "b", "2", "c", "3"}).IsOk());
  auto r = Client().Command({"MGET", "a", "b", "c"});
  ASSERT_TRUE(r.IsArray());
  ASSERT_EQ(r.Elements().size(), 3);
  EXPECT_EQ(r.Elements()[0].String(), "1");
  EXPECT_EQ(r.Elements()[1].String(), "2");
  EXPECT_EQ(r.Elements()[2].String(), "3");
}

TEST_F(StringOpsTest, MgetPartialHit) {
  EXPECT_TRUE(Client().Command({"SET", "a", "1"}).IsOk());
  auto r = Client().Command({"MGET", "a", "missing", "a"});
  ASSERT_TRUE(r.IsArray());
  ASSERT_EQ(r.Elements().size(), 3);
  EXPECT_TRUE(r.Elements()[0].IsBulk());
  EXPECT_TRUE(r.Elements()[1].IsNil());
  EXPECT_TRUE(r.Elements()[2].IsBulk());
}

TEST_F(StringOpsTest, BinaryValue) {
  std::string binary_val("hello\x00world\r\n\xff",
                         14);  // NOLINT(bugprone-string-literal-with-embedded-nul)
  EXPECT_TRUE(Client().Command({"SET", "k", binary_val}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), binary_val);
}

TEST_F(StringOpsTest, EmptyStringValue) {
  EXPECT_TRUE(Client().Command({"SET", "k", ""}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "");
}

TEST_F(StringOpsTest, LargeValue) {
  std::string big(1024UL * 1024UL, 'x');
  EXPECT_TRUE(Client().Command({"SET", "k", big}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String().size(), big.size());
  EXPECT_EQ(r.String(), big);
}

// --- Set operations ---------------------------------------------------------

class SetOpsTest : public DataCommandTest {};

TEST_F(SetOpsTest, SaddAndSmembers) {
  auto add = Client().Command({"SADD", "s", "a", "b", "c"});
  ASSERT_TRUE(add.IsInteger());
  EXPECT_EQ(add.Integer(), 3);

  auto members = Client().Command({"SMEMBERS", "s"});
  ASSERT_TRUE(members.IsArray());
  EXPECT_EQ(members.Elements().size(), 3);
}

TEST_F(SetOpsTest, SaddDuplicate) {
  Client().Command({"SADD", "s", "a"});
  auto r = Client().Command({"SADD", "s", "a"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 0);
}

TEST_F(SetOpsTest, SremExisting) {
  Client().Command({"SADD", "s", "a", "b"});
  auto r = Client().Command({"SREM", "s", "a"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 1);
  EXPECT_EQ(Client().Command({"SCARD", "s"}).Integer(), 1);
}

TEST_F(SetOpsTest, SremNonexistent) {
  Client().Command({"SADD", "s", "a"});
  auto r = Client().Command({"SREM", "s", "missing"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 0);
}

TEST_F(SetOpsTest, Scard) {
  EXPECT_EQ(Client().Command({"SCARD", "empty"}).Integer(), 0);
  Client().Command({"SADD", "s", "a", "b", "c"});
  EXPECT_EQ(Client().Command({"SCARD", "s"}).Integer(), 3);
}

TEST_F(SetOpsTest, SismemberFound) {
  Client().Command({"SADD", "s", "a"});
  auto r = Client().Command({"SISMEMBER", "s", "a"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 1);
}

TEST_F(SetOpsTest, SismemberNotFound) {
  Client().Command({"SADD", "s", "a"});
  auto r = Client().Command({"SISMEMBER", "s", "b"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 0);
}

// --- Hash operations --------------------------------------------------------

class HashOpsTest : public DataCommandTest {};

TEST_F(HashOpsTest, HsetAndHget) {
  auto set = Client().Command({"HSET", "h", "field", "value"});
  ASSERT_TRUE(set.IsInteger());
  EXPECT_EQ(set.Integer(), 1);

  auto get = Client().Command({"HGET", "h", "field"});
  ASSERT_TRUE(get.IsBulk());
  EXPECT_EQ(get.String(), "value");
}

TEST_F(HashOpsTest, HsetOverwrite) {
  Client().Command({"HSET", "h", "f", "v1"});
  Client().Command({"HSET", "h", "f", "v2"});
  EXPECT_EQ(Client().Command({"HGET", "h", "f"}).String(), "v2");
}

TEST_F(HashOpsTest, HgetNonexistentField) {
  Client().Command({"HSET", "h", "f", "v"});
  EXPECT_TRUE(Client().Command({"HGET", "h", "missing"}).IsNil());
}

TEST_F(HashOpsTest, Hgetall) {
  Client().Command({"HSET", "h", "a", "1"});
  Client().Command({"HSET", "h", "b", "2"});
  auto r = Client().Command({"HGETALL", "h"});
  ASSERT_TRUE(r.IsArray());
  EXPECT_EQ(r.Elements().size(), 4);
}

TEST_F(HashOpsTest, Hdel) {
  Client().Command({"HSET", "h", "a", "1", "b", "2"});
  auto r = Client().Command({"HDEL", "h", "a"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 1);
  EXPECT_TRUE(Client().Command({"HGET", "h", "a"}).IsNil());
}

TEST_F(HashOpsTest, Hmset) {
  EXPECT_TRUE(Client().Command({"HMSET", "h", "a", "1", "b", "2"}).IsOk());

  EXPECT_EQ(Client().Command({"HGET", "h", "a"}).String(), "1");
  EXPECT_EQ(Client().Command({"HGET", "h", "b"}).String(), "2");
}

TEST_F(HashOpsTest, Hmget) {
  Client().Command({"HSET", "h", "a", "1", "b", "2"});
  auto r = Client().Command({"HMGET", "h", "a", "missing", "b"});
  ASSERT_TRUE(r.IsArray());
  const auto& a = r.Elements();
  ASSERT_EQ(a.size(), 3U);
  EXPECT_EQ(a[0].String(), "1");
  EXPECT_TRUE(a[1].IsNil());
  EXPECT_EQ(a[2].String(), "2");
}

TEST_F(HashOpsTest, Hexists) {
  Client().Command({"HSET", "h", "a", "1"});
  EXPECT_EQ(Client().Command({"HEXISTS", "h", "a"}).Integer(), 1);
  EXPECT_EQ(Client().Command({"HEXISTS", "h", "missing"}).Integer(), 0);
  EXPECT_EQ(Client().Command({"HEXISTS", "no_such_key", "a"}).Integer(), 0);
}

TEST_F(HashOpsTest, Hkeys) {
  Client().Command({"HSET", "h", "a", "1", "b", "2"});
  auto r = Client().Command({"HKEYS", "h"});
  ASSERT_TRUE(r.IsArray());
  std::set<std::string> got;
  for (const auto& e : r.Elements()) got.insert(e.String());
  EXPECT_EQ(got, (std::set<std::string>{"a", "b"}));
}

TEST_F(HashOpsTest, Hvals) {
  Client().Command({"HSET", "h", "a", "1", "b", "2"});
  auto r = Client().Command({"HVALS", "h"});
  ASSERT_TRUE(r.IsArray());
  std::set<std::string> got;
  for (const auto& e : r.Elements()) got.insert(e.String());
  EXPECT_EQ(got, (std::set<std::string>{"1", "2"}));
}

TEST_F(HashOpsTest, Hlen) {
  Client().Command({"HSET", "h", "a", "1", "b", "2", "c", "3"});
  EXPECT_EQ(Client().Command({"HLEN", "h"}).Integer(), 3);
  EXPECT_EQ(Client().Command({"HLEN", "no_such_key"}).Integer(), 0);
}

TEST_F(HashOpsTest, HdelAllFieldsHgetallReturnsEmptyArray) {
  // Redis semantic: a hash with no remaining fields is "no such key" — HGETALL
  // returns an empty array (not nil), the meta record is deleted.
  Client().Command({"HSET", "h", "a", "1", "b", "2"});
  EXPECT_EQ(Client().Command({"HDEL", "h", "a", "b"}).Integer(), 2);
  auto r = Client().Command({"HGETALL", "h"});
  ASSERT_TRUE(r.IsArray());
  EXPECT_TRUE(r.Elements().empty());
}

TEST_F(HashOpsTest, MultiFieldReadsOnMissingKey) {
  EXPECT_TRUE(Client().Command({"HGETALL", "missing"}).Elements().empty());
  EXPECT_TRUE(Client().Command({"HKEYS", "missing"}).Elements().empty());
  EXPECT_TRUE(Client().Command({"HVALS", "missing"}).Elements().empty());
  EXPECT_EQ(Client().Command({"HLEN", "missing"}).Integer(), 0);

  auto hmget = Client().Command({"HMGET", "missing", "f1", "f2"});
  ASSERT_TRUE(hmget.IsArray());
  ASSERT_EQ(hmget.Elements().size(), 2U);
  EXPECT_TRUE(hmget.Elements()[0].IsNil());
  EXPECT_TRUE(hmget.Elements()[1].IsNil());
}

// --- Sorted set operations --------------------------------------------------

class ZsetOpsTest : public DataCommandTest {};

TEST_F(ZsetOpsTest, ZaddAndZscore) {
  auto add = Client().Command({"ZADD", "z", "1.5", "member"});
  ASSERT_TRUE(add.IsInteger());
  EXPECT_EQ(add.Integer(), 1);

  auto score = Client().Command({"ZSCORE", "z", "member"});
  ASSERT_TRUE(score.IsBulk());
  EXPECT_EQ(score.String(), "1.5");
}

TEST_F(ZsetOpsTest, ZaddUpdateScore) {
  Client().Command({"ZADD", "z", "1.0", "m"});
  Client().Command({"ZADD", "z", "2.0", "m"});
  EXPECT_EQ(Client().Command({"ZSCORE", "z", "m"}).String(), "2");
}

TEST_F(ZsetOpsTest, Zcard) {
  EXPECT_EQ(Client().Command({"ZCARD", "empty"}).Integer(), 0);
  Client().Command({"ZADD", "z", "1", "a", "2", "b"});
  EXPECT_EQ(Client().Command({"ZCARD", "z"}).Integer(), 2);
}

TEST_F(ZsetOpsTest, Zrem) {
  Client().Command({"ZADD", "z", "1", "a", "2", "b"});
  auto r = Client().Command({"ZREM", "z", "a"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 1);
  EXPECT_TRUE(Client().Command({"ZSCORE", "z", "a"}).IsNil());
}

// --- Key operations ---------------------------------------------------------

class KeyOpsTest : public DataCommandTest {};

TEST_F(KeyOpsTest, DelExisting) {
  Client().Command({"SET", "k", "v"});
  auto r = Client().Command({"DEL", "k"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 1);
  EXPECT_TRUE(Client().Command({"GET", "k"}).IsNil());
}

TEST_F(KeyOpsTest, DelNonexistent) {
  auto r = Client().Command({"DEL", "missing"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 0);
}

TEST_F(KeyOpsTest, DelMultiple) {
  Client().Command({"SET", "a", "1"});
  Client().Command({"SET", "b", "2"});
  auto r = Client().Command({"DEL", "a", "b", "missing"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 2);
}

TEST_F(KeyOpsTest, ExistsTrue) {
  Client().Command({"SET", "k", "v"});
  auto r = Client().Command({"EXISTS", "k"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 1);
}

TEST_F(KeyOpsTest, ExistsFalse) {
  auto r = Client().Command({"EXISTS", "missing"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 0);
}

// --- Cross-type safety ------------------------------------------------------

class TypeSafetyTest : public DataCommandTest {};

TEST_F(TypeSafetyTest, WrongtypeStringAsSet) {
  Client().Command({"SET", "k", "v"});
  auto r = Client().Command({"SADD", "k", "member"});
  EXPECT_TRUE(r.IsError());
  EXPECT_NE(r.String().find("WRONGTYPE"), std::string::npos);
}

TEST_F(TypeSafetyTest, WrongtypeSetAsString) {
  Client().Command({"SADD", "k", "member"});
  auto r = Client().Command({"GET", "k"});
  EXPECT_TRUE(r.IsError());
  EXPECT_NE(r.String().find("WRONGTYPE"), std::string::npos);
}

TEST_F(TypeSafetyTest, WrongtypeHashAsString) {
  Client().Command({"HSET", "k", "f", "v"});
  auto r = Client().Command({"GET", "k"});
  EXPECT_TRUE(r.IsError());
  EXPECT_NE(r.String().find("WRONGTYPE"), std::string::npos);
}

TEST_F(TypeSafetyTest, DelWorksOnAnyType) {
  Client().Command({"SADD", "set_key", "a"});
  Client().Command({"SET", "str_key", "v"});
  Client().Command({"HSET", "hash_key", "f", "v"});
  auto r = Client().Command({"DEL", "set_key", "str_key", "hash_key"});
  ASSERT_TRUE(r.IsInteger());
  EXPECT_EQ(r.Integer(), 3);
}

// --- FLUSHDB / FLUSHALL -----------------------------------------------------

class FlushTest : public DataCommandTest {};

TEST_F(FlushTest, FlushdbWipesMixedTypes) {
  ASSERT_TRUE(Client().Command({"SET", "s", "v"}).IsOk());
  ASSERT_TRUE(Client().Command({"SADD", "set_k", "m1", "m2"}).IsInteger());
  ASSERT_TRUE(Client().Command({"HSET", "hash_k", "f", "v"}).IsInteger());
  ASSERT_TRUE(Client().Command({"ZADD", "zset_k", "1", "a", "2", "b"}).IsInteger());

  auto flush = Client().Command({"FLUSHDB"});
  ASSERT_TRUE(flush.IsStatus()) << "FLUSHDB response: " << flush.String();
  EXPECT_EQ(flush.String(), "OK");

  EXPECT_TRUE(Client().Command({"GET", "s"}).IsNil());
  EXPECT_EQ(Client().Command({"SCARD", "set_k"}).Integer(), 0);
  EXPECT_EQ(Client().Command({"HLEN", "hash_k"}).Integer(), 0);
  EXPECT_EQ(Client().Command({"ZCARD", "zset_k"}).Integer(), 0);
  EXPECT_EQ(Client().Command({"EXISTS", "s", "set_k", "hash_k", "zset_k"}).Integer(), 0);
}

TEST_F(FlushTest, FlushallBehavesAsFlushdb) {
  ASSERT_TRUE(Client().Command({"SET", "k", "v"}).IsOk());
  auto flush = Client().Command({"FLUSHALL"});
  ASSERT_TRUE(flush.IsStatus());
  EXPECT_EQ(flush.String(), "OK");
  EXPECT_TRUE(Client().Command({"GET", "k"}).IsNil());
}

TEST_F(FlushTest, FlushdbAcceptsAsyncModifier) {
  ASSERT_TRUE(Client().Command({"SET", "k", "v"}).IsOk());
  auto flush = Client().Command({"FLUSHDB", "ASYNC"});
  ASSERT_TRUE(flush.IsStatus());
  EXPECT_EQ(flush.String(), "OK");
  EXPECT_TRUE(Client().Command({"GET", "k"}).IsNil());
}

TEST_F(FlushTest, FlushdbAcceptsSyncModifier) {
  ASSERT_TRUE(Client().Command({"SET", "k", "v"}).IsOk());
  auto flush = Client().Command({"FLUSHDB", "SYNC"});
  ASSERT_TRUE(flush.IsStatus());
  EXPECT_EQ(flush.String(), "OK");
  EXPECT_TRUE(Client().Command({"GET", "k"}).IsNil());
}

TEST_F(FlushTest, FlushdbRejectsUnknownModifier) {
  auto r = Client().Command({"FLUSHDB", "FOO"});
  ASSERT_TRUE(r.IsError());
}

TEST_F(FlushTest, WritesAfterFlushSurvive) {
  ASSERT_TRUE(Client().Command({"SET", "before", "x"}).IsOk());
  ASSERT_TRUE(Client().Command({"FLUSHDB"}).IsStatus());
  ASSERT_TRUE(Client().Command({"SET", "after", "y"}).IsOk());

  EXPECT_TRUE(Client().Command({"GET", "before"}).IsNil());
  auto after = Client().Command({"GET", "after"});
  ASSERT_TRUE(after.IsBulk());
  EXPECT_EQ(after.String(), "y");
}

}  // namespace
}  // namespace abyss::system_test
