#include "abyss/resp/predicate_extractor.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"

namespace abyss::resp {
namespace {

using core::PredicateFlags;
using core::RespCommand;

// The message, or "" when the flags extract.
std::string ErrorOf(const core::Result<PredicateFlags>& flags) {
  return flags.has_value() ? "" : flags.error().message();
}

TEST(PredicateExtractorTest, SetNxWithXxIsASyntaxError) {
  EXPECT_EQ(ErrorOf(ExtractSetFlags(RespCommand{{"SET", "k", "v", "NX", "XX"}})), "syntax error");
  EXPECT_EQ(*ExtractSetFlags(RespCommand{{"SET", "k", "v", "xx", "GET"}}),
            PredicateFlags::kXx | PredicateFlags::kGet);
}

// Redis 7's texts, checked in its order: NX with XX first.
TEST(PredicateExtractorTest, ZaddFlagConflictsUseRedisTexts) {
  const std::string nx_xx = "XX and NX options at the same time are not compatible";
  const std::string gt_lt_nx = "GT, LT, and/or NX options at the same time are not compatible";
  const std::vector<std::pair<std::vector<std::string>, std::string>> cases = {
      {{"NX", "XX"}, nx_xx},    {{"XX", "NX", "GT"}, nx_xx}, {{"GT", "LT"}, gt_lt_nx},
      {{"NX", "GT"}, gt_lt_nx}, {{"lt", "nx"}, gt_lt_nx},    {{"CH", "GT", "LT"}, gt_lt_nx},
      {{"XX", "GT", "CH"}, ""},
  };
  for (const auto& [tokens, message] : cases) {
    RespCommand cmd{{"ZADD", "k"}};
    cmd.args.insert(cmd.args.end(), tokens.begin(), tokens.end());
    cmd.args.insert(cmd.args.end(), {"1", "m"});
    EXPECT_EQ(ErrorOf(ExtractZAddFlags(cmd)), message) << tokens.front();
  }
}

TEST(PredicateExtractorTest, ExpireFlagConflictsUseRedisTexts) {
  const std::string nx_any = "NX and XX, GT or LT options at the same time are not compatible";
  const std::string gt_lt = "GT and LT options at the same time are not compatible";
  const std::vector<std::pair<std::vector<std::string>, std::string>> cases = {
      {{"NX", "XX"}, nx_any}, {{"NX", "GT"}, nx_any},      {{"lt", "nx"}, nx_any},
      {{"GT", "LT"}, gt_lt},  {{"XX", "GT", "LT"}, gt_lt}, {{"NX", "GT", "LT"}, nx_any},
      {{"XX", "GT"}, ""},
  };
  for (const auto& [tokens, message] : cases) {
    RespCommand cmd{{"EXPIRE", "k", "10"}};
    cmd.args.insert(cmd.args.end(), tokens.begin(), tokens.end());
    EXPECT_EQ(ErrorOf(ExtractExpireFlags(cmd)), message) << tokens.front();
  }
}

TEST(PredicateExtractorTest, ZaddIncrIsRefused) {
  const RespCommand cmd{{"ZADD", "k", "XX", "INCR", "1", "m"}};
  EXPECT_EQ(ErrorOf(ExtractZAddFlags(cmd)), "ZADD INCR is not supported");
}

TEST(PredicateExtractorTest, ExpireRejectsAnUnknownOption) {
  const RespCommand cmd{{"EXPIRE", "k", "10", "XX", "bogus"}};
  EXPECT_EQ(ErrorOf(ExtractExpireFlags(cmd)), "Unsupported option bogus");
}

}  // namespace
}  // namespace abyss::resp
