#include "abyss/resp/parser.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace abyss::resp {
namespace {

std::span<const uint8_t> Bytes(const std::string& s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

TEST(ParserTest, SimpleStringOk) {
  std::string input = "+OK\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsSimpleString());
  EXPECT_EQ(r->value.AsString(), "OK");
  EXPECT_EQ(r->bytes_consumed, input.size());
}

TEST(ParserTest, Integer) {
  std::string input = ":12345\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsInteger());
  EXPECT_EQ(r->value.AsInteger(), 12345);
}

TEST(ParserTest, NegativeInteger) {
  std::string input = ":-7\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->value.AsInteger(), -7);
}

TEST(ParserTest, BulkString) {
  std::string input = "$5\r\nhello\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsBulkString());
  EXPECT_EQ(r->value.AsString(), "hello");
  EXPECT_EQ(r->bytes_consumed, input.size());
}

TEST(ParserTest, BulkStringEmpty) {
  std::string input = "$0\r\n\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsBulkString());
  EXPECT_EQ(r->value.AsString(), "");
}

TEST(ParserTest, BulkStringBinarySafe) {
  std::string input;
  input.append("$6\r\n");
  input.append({'a', '\0', 'b', '\r', '\n', 'c'});
  input.append("\r\n");
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->value.AsString().size(), 6U);
  EXPECT_EQ(r->value.AsString()[1], '\0');
  EXPECT_EQ(r->value.AsString()[3], '\r');
}

TEST(ParserTest, NullBulkString) {
  std::string input = "$-1\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsNull());
}

TEST(ParserTest, NullArray) {
  std::string input = "*-1\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsNull());
}

TEST(ParserTest, EmptyArray) {
  std::string input = "*0\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsArray());
  EXPECT_EQ(r->value.AsArray().size(), 0U);
}

TEST(ParserTest, ArrayOfBulkStrings) {
  std::string input = "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->value.IsArray());
  ASSERT_EQ(r->value.AsArray().size(), 2U);
  EXPECT_EQ(r->value.AsArray()[0].AsString(), "SET");
  EXPECT_EQ(r->value.AsArray()[1].AsString(), "key");
}

TEST(ParserTest, NestedArray) {
  // *2\r\n:1\r\n*2\r\n+a\r\n:2\r\n
  std::string input = "*2\r\n:1\r\n*2\r\n+a\r\n:2\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->value.IsArray());
  const auto& outer = r->value.AsArray();
  ASSERT_EQ(outer.size(), 2U);
  EXPECT_EQ(outer[0].AsInteger(), 1);
  ASSERT_TRUE(outer[1].IsArray());
  const auto& inner = outer[1].AsArray();
  ASSERT_EQ(inner.size(), 2U);
  EXPECT_EQ(inner[0].AsString(), "a");
  EXPECT_EQ(inner[1].AsInteger(), 2);
}

TEST(ParserTest, ErrorValueRoundTripsPrefix) {
  std::string input = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->value.IsError());
  EXPECT_EQ(r->value.AsString(),
            "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(ParserTest, PartialSimpleStringIsIncomplete) {
  std::string input = "+OK";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

TEST(ParserTest, PartialBulkBodyIsIncomplete) {
  std::string input = "$5\r\nhel";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

TEST(ParserTest, PartialArrayIsIncomplete) {
  std::string input = "*2\r\n$3\r\nSET\r\n";  // missing second element
  auto r = Parser::Parse(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

TEST(ParserTest, BulkWithoutCrlfTailIsMalformed) {
  std::string input = "$3\r\nabc##";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(ParserTest, UnknownTypeByteIsMalformed) {
  std::string input = "!foo\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(ParserTest, BytesConsumedLeavesTrailingDataAlone) {
  std::string input = "+OK\r\n+NEXT\r\n";
  auto r = Parser::Parse(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->bytes_consumed, 5U);  // "+OK\r\n"
}

TEST(ParserTest, ParseCommandFromBulkArray) {
  std::string input = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->command.args.size(), 3U);
  EXPECT_EQ(r->command.args[0], "SET");
  EXPECT_EQ(r->command.args[1], "k");
  EXPECT_EQ(r->command.args[2], "v");
}

TEST(ParserTest, ParseCommandRejectsNonArrayFrame) {
  std::string input = "+OK\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(ParserTest, ParseCommandRejectsNonBulkElement) {
  std::string input = "*2\r\n$3\r\nSET\r\n:1\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(ParserTest, InlineCommandSimple) {
  std::string input = "PING\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->command.args.size(), 1U);
  EXPECT_EQ(r->command.args[0], "PING");
}

TEST(ParserTest, InlineCommandMultipleArgs) {
  std::string input = "SET foo bar\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->command.args.size(), 3U);
  EXPECT_EQ(r->command.args[0], "SET");
  EXPECT_EQ(r->command.args[1], "foo");
  EXPECT_EQ(r->command.args[2], "bar");
}

TEST(ParserTest, InlineCommandAcceptsLfOnly) {
  std::string input = "PING\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->command.args[0], "PING");
}

TEST(ParserTest, InlineCommandDoubleQuotedWithEscapes) {
  std::string input = "SET greeting \"hello\\nworld\"\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->command.args.size(), 3U);
  EXPECT_EQ(r->command.args[2], "hello\nworld");
}

TEST(ParserTest, InlineCommandHexEscape) {
  std::string input = "SET k \"\\x41\\x42\"\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->command.args[2], "AB");
}

TEST(ParserTest, InlineCommandSingleQuoted) {
  std::string input = "SET k 'hello world'\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_TRUE(r.has_value());
  ASSERT_EQ(r->command.args.size(), 3U);
  EXPECT_EQ(r->command.args[2], "hello world");
}

TEST(ParserTest, InlineCommandIncompleteNoNewline) {
  std::string input = "PING";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

TEST(ParserTest, EmptyBufferIsIncomplete) {
  std::vector<uint8_t> empty;
  auto r = Parser::Parse(empty);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

// RESP-1: a multibulk header declaring a count far above the limit is Malformed
// BEFORE any reserve()/allocation. Pre-fix this reserved billions of elements
// and crashed (length_error/bad_alloc).
TEST(ParserTest, HugeArrayCountIsMalformedNotCrash) {
  std::string input = "*9999999999999999999\r\n";  // overflows int64 parse -> Malformed
  auto r = Parser::Parse(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(ParserTest, ArrayCountAboveLimitIsMalformed) {
  ParserLimits limits;
  const std::string over = "*" + std::to_string(limits.max_array_elements + 1) + "\r\n";
  auto r = Parser::Parse(Bytes(over), limits);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(ParserTest, ArrayCountAtLimitIsAcceptedFraming) {
  // At the limit the count itself is accepted (the frame is then Incomplete
  // because the elements aren't present) — proves the boundary is inclusive.
  ParserLimits limits;
  const std::string at_limit = "*" + std::to_string(limits.max_array_elements) + "\r\n";
  auto r = Parser::Parse(Bytes(at_limit), limits);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

TEST(ParserTest, HugeBulkLengthIsMalformedNotCrash) {
  ParserLimits limits;
  const std::string over = "$" + std::to_string(limits.max_bulk_len + 1) + "\r\n";
  auto r = Parser::Parse(Bytes(over), limits);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

// RESP-2: a fully-received inline line with an unbalanced quote can never be
// satisfied, so it is Malformed — not kIncomplete (which would stall forever).
TEST(ParserTest, UnterminatedInlineDoubleQuoteIsMalformed) {
  std::string input = "SET key \"oops\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_NE(std::string(r.error().message()).find("unbalanced quotes"), std::string::npos);
}

TEST(ParserTest, UnterminatedInlineSingleQuoteIsMalformed) {
  std::string input = "SET k 'oops\r\n";
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

// RESP-2 regression guard: a genuinely-incomplete inline buffer (no terminator
// yet) still returns kIncomplete so legitimate streaming is not broken.
TEST(ParserTest, TrulyIncompleteInlineStillIncomplete) {
  std::string input = "SET key \"oo";  // no \n yet
  auto r = Parser::ParseCommand(Bytes(input));
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kIncomplete);
}

}  // namespace
}  // namespace abyss::resp
