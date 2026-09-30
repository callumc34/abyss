#include "abyss/resp/serializer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "abyss/resp/parser.h"

namespace abyss::resp {
namespace {

std::string Bytes(const std::vector<uint8_t>& v) {
  return {reinterpret_cast<const char*>(v.data()), v.size()};
}

std::span<const uint8_t> AsSpan(const std::vector<uint8_t>& v) { return {v.data(), v.size()}; }

std::span<const uint8_t> StrSpan(const std::string& s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

TEST(SerializerTest, Null) {
  auto out = Serializer::Serialize(core::RespValue::Null());
  EXPECT_EQ(Bytes(out), "$-1\r\n");
}

// RESP-5: Redis distinguishes the array-shaped nil (`*-1`) from the bulk nil
// (`$-1`); COMMAND INFO of an unknown command uses the former.
TEST(SerializerTest, NullArrayIsDistinctFromNullBulk) {
  EXPECT_EQ(Bytes(Serializer::Serialize(core::RespValue::NullArray())), "*-1\r\n");
  EXPECT_EQ(Bytes(Serializer::Serialize(core::RespValue::Null())), "$-1\r\n");
}

TEST(SerializerTest, NullArrayNestedInsideArray) {
  auto out = Serializer::Serialize(core::RespValue::Array({
      core::RespValue::NullArray(),
      core::RespValue::Null(),
  }));
  EXPECT_EQ(Bytes(out), "*2\r\n*-1\r\n$-1\r\n");
}

TEST(SerializerTest, SimpleString) {
  auto out = Serializer::Serialize(core::RespValue::SimpleString("OK"));
  EXPECT_EQ(Bytes(out), "+OK\r\n");
}

TEST(SerializerTest, BulkString) {
  auto out = Serializer::Serialize(core::RespValue::BulkString("hello"));
  EXPECT_EQ(Bytes(out), "$5\r\nhello\r\n");
}

TEST(SerializerTest, BulkStringEmpty) {
  auto out = Serializer::Serialize(core::RespValue::BulkString(""));
  EXPECT_EQ(Bytes(out), "$0\r\n\r\n");
}

TEST(SerializerTest, BulkStringBinarySafe) {
  std::string payload{'a', '\0', 'b', '\r', '\n', 'c'};
  auto out = Serializer::Serialize(core::RespValue::BulkString(payload));
  std::string expected = "$6\r\n";
  expected.append(payload);
  expected.append("\r\n");
  EXPECT_EQ(Bytes(out), expected);
}

TEST(SerializerTest, Integer) {
  auto out = Serializer::Serialize(core::RespValue::Integer(42));
  EXPECT_EQ(Bytes(out), ":42\r\n");
}

TEST(SerializerTest, NegativeInteger) {
  auto out = Serializer::Serialize(core::RespValue::Integer(-123));
  EXPECT_EQ(Bytes(out), ":-123\r\n");
}

TEST(SerializerTest, Error) {
  auto out =
      Serializer::Serialize(core::RespValue::Error(core::ErrorPrefix::kWrongType, "bad type"));
  EXPECT_EQ(Bytes(out), "-WRONGTYPE bad type\r\n");
}

TEST(SerializerTest, EmptyArray) {
  auto out = Serializer::Serialize(core::RespValue::Array({}));
  EXPECT_EQ(Bytes(out), "*0\r\n");
}

TEST(SerializerTest, ArrayOfMixedTypes) {
  auto out = Serializer::Serialize(core::RespValue::Array({
      core::RespValue::BulkString("SET"),
      core::RespValue::Integer(1),
      core::RespValue::SimpleString("OK"),
  }));
  EXPECT_EQ(Bytes(out), "*3\r\n$3\r\nSET\r\n:1\r\n+OK\r\n");
}

TEST(SerializerTest, NestedArray) {
  auto out = Serializer::Serialize(core::RespValue::Array({
      core::RespValue::Integer(1),
      core::RespValue::Array({
          core::RespValue::BulkString("a"),
          core::RespValue::BulkString("b"),
      }),
  }));
  EXPECT_EQ(Bytes(out), "*2\r\n:1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n");
}

TEST(SerializerTest, SerializeCommand) {
  core::RespCommand cmd{{"SET", "key", "value"}};
  auto out = Serializer::SerializeCommand(cmd);
  EXPECT_EQ(Bytes(out), "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
}

TEST(SerializerTest, SerializeCommandBinarySafeArg) {
  std::string binary_arg{'\x00', '\xff', '\r', '\n'};
  core::RespCommand cmd{{"SET", "k", binary_arg}};
  auto out = Serializer::SerializeCommand(cmd);
  std::string expected = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n";
  expected.append(binary_arg);
  expected.append("\r\n");
  EXPECT_EQ(Bytes(out), expected);
}

// ── Round-trip with parser ──────────────────────────────────────────────────

TEST(SerializerRoundTripTest, SimpleString) {
  auto original = core::RespValue::SimpleString("PONG");
  auto bytes = Serializer::Serialize(original);
  auto parsed = Parser::Parse(AsSpan(bytes));
  ASSERT_TRUE(parsed.has_value());
  EXPECT_TRUE(parsed->value.IsSimpleString());
  EXPECT_EQ(parsed->value.AsString(), "PONG");
}

TEST(SerializerRoundTripTest, BulkStringWithBinary) {
  std::string payload{'\x00', '\x01', '\r', '\n', 'Z'};
  auto original = core::RespValue::BulkString(payload);
  auto bytes = Serializer::Serialize(original);
  auto parsed = Parser::Parse(AsSpan(bytes));
  ASSERT_TRUE(parsed.has_value());
  EXPECT_TRUE(parsed->value.IsBulkString());
  EXPECT_EQ(parsed->value.AsString(), payload);
}

TEST(SerializerRoundTripTest, IntegerRange) {
  for (int64_t v : {int64_t{0}, int64_t{1}, int64_t{-1}, int64_t{9223372036854775807},
                    int64_t{-9223372036854775807 - 1}}) {
    auto bytes = Serializer::Serialize(core::RespValue::Integer(v));
    auto parsed = Parser::Parse(AsSpan(bytes));
    ASSERT_TRUE(parsed.has_value()) << "failed for " << v;
    EXPECT_EQ(parsed->value.AsInteger(), v);
  }
}

TEST(SerializerRoundTripTest, ErrorPreservesPrefixedMessage) {
  auto original = core::RespValue::Error(core::ErrorPrefix::kMoved, "3999 127.0.0.1:6380");
  auto bytes = Serializer::Serialize(original);
  auto parsed = Parser::Parse(AsSpan(bytes));
  ASSERT_TRUE(parsed.has_value());
  EXPECT_TRUE(parsed->value.IsError());
  EXPECT_EQ(parsed->value.AsString(), "MOVED 3999 127.0.0.1:6380");
}

// RESP-6: a prefix outside ErrorPrefix must survive parse -> serialise intact.
TEST(SerializerRoundTripTest, UnknownErrorPrefixRoundTrips) {
  const std::string wire = "-WEIRD something\r\n";
  auto parsed = Parser::Parse(StrSpan(wire));
  ASSERT_TRUE(parsed.has_value());
  ASSERT_TRUE(parsed->value.IsError());
  EXPECT_EQ(parsed->value.AsString(), "WEIRD something");
  EXPECT_EQ(Bytes(Serializer::Serialize(parsed->value)), wire);
}

TEST(SerializerRoundTripTest, KnownErrorPrefixesUnchanged) {
  for (const std::string& wire :
       {std::string("-WRONGTYPE bad type\r\n"), std::string("-OOM command not allowed\r\n")}) {
    auto parsed = Parser::Parse(StrSpan(wire));
    ASSERT_TRUE(parsed.has_value()) << wire;
    ASSERT_TRUE(parsed->value.IsError());
    EXPECT_EQ(Bytes(Serializer::Serialize(parsed->value)), wire);
  }
  const std::string wrongtype_wire = "-WRONGTYPE bad type\r\n";
  auto wrongtype = Parser::Parse(StrSpan(wrongtype_wire));
  ASSERT_TRUE(wrongtype.has_value());
  EXPECT_EQ(wrongtype->value.ErrorPrefixOf(), core::ErrorPrefix::kWrongType);
  EXPECT_EQ(wrongtype->value.ErrorMessage(), "bad type");
}

TEST(SerializerRoundTripTest, ArrayOfCommandArgs) {
  core::RespCommand cmd{{"ZADD", "scores", "1", "alice", "2", "bob"}};
  auto bytes = Serializer::SerializeCommand(cmd);
  auto parsed = Parser::ParseCommand(AsSpan(bytes));
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->command.args, cmd.args);
}

TEST(SerializerRoundTripTest, NestedArrayStructurePreserved) {
  auto original = core::RespValue::Array({
      core::RespValue::BulkString("ok"),
      core::RespValue::Array({
          core::RespValue::Integer(1),
          core::RespValue::Integer(2),
      }),
      core::RespValue::Null(),
  });
  auto bytes = Serializer::Serialize(original);
  auto parsed = Parser::Parse(AsSpan(bytes));
  ASSERT_TRUE(parsed.has_value());
  ASSERT_TRUE(parsed->value.IsArray());
  const auto& a = parsed->value.AsArray();
  ASSERT_EQ(a.size(), 3U);
  EXPECT_EQ(a[0].AsString(), "ok");
  ASSERT_TRUE(a[1].IsArray());
  EXPECT_EQ(a[1].AsArray()[0].AsInteger(), 1);
  EXPECT_EQ(a[1].AsArray()[1].AsInteger(), 2);
  EXPECT_TRUE(a[2].IsNull());
}

}  // namespace
}  // namespace abyss::resp
