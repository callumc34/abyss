#include <gtest/gtest.h>

#include <string>

#include "abyss/core/ops.h"

namespace abyss::core::ops {
namespace {

// ENGINE-7: ParseSet is the unconditional SET path; the resolver's ParseSetArgs
// is the conditional one. Both must accept exactly the same option tokens, or a
// SET the frontend admits becomes unparseable once queued.

TEST(ParseSetOptionsTest, UnconditionalSetRejectsUnknownOption) {
  RespCommand cmd{{"SET", "k", "v", "BOGUS"}};
  auto op = ParseWriteOp("SET", cmd);
  ASSERT_FALSE(op.has_value());
  EXPECT_EQ(op.error().code(), ErrorCode::kInvalidArgument);
  EXPECT_NE(std::string(op.error().message()).find("syntax error"), std::string::npos);
}

TEST(ParseSetOptionsTest, UnknownOptionAfterValidTtlStillRejected) {
  RespCommand cmd{{"SET", "k", "v", "EX", "10", "BOGUS"}};
  EXPECT_FALSE(ParseWriteOp("SET", cmd).has_value());
}

TEST(ParseSetOptionsTest, TtlOptionsStillParse) {
  for (const std::string& opt :
       {std::string("EX"), std::string("PX"), std::string("EXAT"), std::string("PXAT")}) {
    RespCommand cmd{{"SET", "k", "v", opt, "10"}};
    auto op = ParseWriteOp("SET", cmd);
    ASSERT_TRUE(op.has_value()) << opt;
    auto* set = std::get_if<StringSet>(&*op);
    ASSERT_NE(set, nullptr);
    EXPECT_GT(set->abs_ttl_ms, 0U);
  }
}

TEST(ParseSetOptionsTest, TtlOptionIsCaseInsensitiveAndNeedsAValue) {
  RespCommand lower{{"SET", "k", "v", "px", "5"}};
  EXPECT_TRUE(ParseWriteOp("SET", lower).has_value());

  RespCommand dangling{{"SET", "k", "v", "EX"}};
  EXPECT_FALSE(ParseWriteOp("SET", dangling).has_value());

  RespCommand non_numeric{{"SET", "k", "v", "EX", "later"}};
  EXPECT_FALSE(ParseWriteOp("SET", non_numeric).has_value());
}

// NX/XX/GET/KEEPTTL are routed to the resolver, which materialises a plain SET.
// They stay legal here so the two parse paths agree on the accepted token set.
TEST(ParseSetOptionsTest, ResolverRoutedOptionsRemainAccepted) {
  for (const std::string& opt :
       {std::string("NX"), std::string("XX"), std::string("GET"), std::string("KEEPTTL")}) {
    RespCommand cmd{{"SET", "k", "v", opt}};
    EXPECT_TRUE(ParseWriteOp("SET", cmd).has_value()) << opt;
  }
}

TEST(ParseSetOptionsTest, PlainSetUnaffected) {
  RespCommand cmd{{"SET", "k", "v"}};
  auto op = ParseWriteOp("SET", cmd);
  ASSERT_TRUE(op.has_value());
  auto* set = std::get_if<StringSet>(&*op);
  ASSERT_NE(set, nullptr);
  EXPECT_EQ(set->abs_ttl_ms, 0U);
}

}  // namespace
}  // namespace abyss::core::ops
