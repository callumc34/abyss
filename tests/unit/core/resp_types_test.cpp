#include "abyss/core/resp_types.h"

#include <gtest/gtest.h>

namespace abyss::core {
namespace {

TEST(RespValueTest, Null) {
  auto v = RespValue::Null();
  EXPECT_TRUE(v.IsNull());
  EXPECT_EQ(v.type(), RespValue::Type::kNull);
}

TEST(RespValueTest, NullArrayIsDistinctFromNullBulk) {
  auto v = RespValue::NullArray();
  EXPECT_TRUE(v.IsNullArray());
  EXPECT_FALSE(v.IsNull());
  EXPECT_EQ(v.type(), RespValue::Type::kNullArray);
  EXPECT_FALSE(RespValue::Null().IsNullArray());
}

TEST(RespValueTest, SimpleString) {
  auto v = RespValue::SimpleString("OK");
  EXPECT_TRUE(v.IsSimpleString());
  EXPECT_FALSE(v.IsBulkString());
  EXPECT_EQ(v.type(), RespValue::Type::kSimpleString);
  EXPECT_EQ(v.AsString(), "OK");
}

TEST(RespValueTest, BulkString) {
  auto v = RespValue::BulkString("hello");
  EXPECT_TRUE(v.IsBulkString());
  EXPECT_FALSE(v.IsSimpleString());
  EXPECT_EQ(v.type(), RespValue::Type::kBulkString);
  EXPECT_EQ(v.AsString(), "hello");
}

TEST(RespValueTest, BulkStringBinarySafe) {
  std::string binary{'a', '\0', 'b', '\r', '\n', 'c'};
  auto v = RespValue::BulkString(binary);
  EXPECT_EQ(v.AsString(), binary);
  EXPECT_EQ(v.AsString().size(), 6);
}

TEST(RespValueTest, Integer) {
  auto v = RespValue::Integer(42);
  EXPECT_TRUE(v.IsInteger());
  EXPECT_EQ(v.AsInteger(), 42);
}

TEST(RespValueTest, ErrorFormatsPrefixAndMessage) {
  auto v = RespValue::Error(ErrorPrefix::kWrongType,
                            "Operation against a key holding the wrong kind of value");
  EXPECT_TRUE(v.IsError());
  EXPECT_EQ(v.AsString(), "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(RespValueTest, ErrorGenericPrefix) {
  auto v = RespValue::Error(ErrorPrefix::kErr, "syntax error");
  EXPECT_EQ(v.AsString(), "ERR syntax error");
}

TEST(RespValueTest, ErrorMovedCarriesSlotAndAddress) {
  auto v = RespValue::Error(ErrorPrefix::kMoved, "3999 127.0.0.1:6380");
  EXPECT_EQ(v.AsString(), "MOVED 3999 127.0.0.1:6380");
}

TEST(RespValueTest, ErrorPrefixOfReturnsTypedEnum) {
  auto v = RespValue::Error(ErrorPrefix::kNoProto, "unsupported protocol version");
  EXPECT_EQ(v.ErrorPrefixOf(), ErrorPrefix::kNoProto);
}

TEST(RespValueTest, RawErrorKeepsBodyVerbatim) {
  auto v = RespValue::RawError("WEIRD something");
  EXPECT_TRUE(v.IsError());
  EXPECT_EQ(v.AsString(), "WEIRD something");
  EXPECT_EQ(v.ErrorPrefixOf(), ErrorPrefix::kErr);
}

TEST(RespValueTest, RawErrorDoesNotRePrependKnownPrefix) {
  auto v = RespValue::RawError("ERR syntax error");
  EXPECT_EQ(v.AsString(), "ERR syntax error");
}

TEST(RespValueTest, ErrorMessageStripsPrefix) {
  auto v = RespValue::Error(ErrorPrefix::kWrongType, "operation against a wrong type");
  EXPECT_EQ(v.ErrorMessage(), "operation against a wrong type");
}

TEST(RespValueTest, ErrorMessageEmptyForNonError) {
  EXPECT_EQ(RespValue::Null().ErrorMessage(), "");
  EXPECT_EQ(RespValue::SimpleString("OK").ErrorMessage(), "");
}

TEST(RespValueTest, Array) {
  auto v = RespValue::Array({
      RespValue::BulkString("one"),
      RespValue::Integer(2),
  });
  EXPECT_TRUE(v.IsArray());
  ASSERT_EQ(v.AsArray().size(), 2);
  EXPECT_TRUE(v.AsArray()[0].IsBulkString());
  EXPECT_TRUE(v.AsArray()[1].IsInteger());
}

TEST(ErrorPrefixStringTest, AllPrefixesHaveCanonicalText) {
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kErr), "ERR");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kWrongType), "WRONGTYPE");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kLoading), "LOADING");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kMoved), "MOVED");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kCrossSlot), "CROSSSLOT");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kOom), "OOM");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kNoScript), "NOSCRIPT");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kNoProto), "NOPROTO");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kReadOnly), "READONLY");
  EXPECT_EQ(ErrorPrefixString(ErrorPrefix::kNoAuth), "NOAUTH");
}

TEST(RespCommandTest, NameAndArgCount) {
  RespCommand cmd{{"SET", "key", "value"}};
  EXPECT_EQ(cmd.Name(), "SET");
  EXPECT_EQ(cmd.ArgCount(), 3);
}

}  // namespace
}  // namespace abyss::core
