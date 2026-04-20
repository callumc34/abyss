#include "abyss/core/queue_entry.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace abyss::core::entry {
namespace {

QueueEntry MakeWriteEntry(std::initializer_list<std::string> args) {
  return QueueEntry{
      .seq = 1,
      .appended_at = WallClock::now(),
      .payload = entry::Write{.cmd = RespCommand{.args = std::vector<std::string>(args)}},
  };
}

QueueEntry MakeConditionalEntry() {
  return QueueEntry{
      .seq = 1,
      .appended_at = WallClock::now(),
      .payload = entry::Conditional{.cmd = RespCommand{.args = {"SET", "k", "v"}}},
  };
}

QueueEntry MakeResolvedEntry(Decision decision, std::optional<RespCommand> materialised) {
  return QueueEntry{
      .seq = 1,
      .appended_at = WallClock::now(),
      .payload = entry::Resolved{.decision = decision, .materialised_op = std::move(materialised)},
  };
}

TEST(ExtractApplicableCommand, WriteReturnsCmd) {
  auto e = MakeWriteEntry({"SET", "k", "v"});
  auto r = ExtractApplicableCommand(e);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ((*r)->args.front(), "SET");
}

TEST(ExtractApplicableCommand, ConditionalReturnsInvalidArgument) {
  auto e = MakeConditionalEntry();
  auto r = ExtractApplicableCommand(e);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), ErrorCode::kInvalidArgument);
}

TEST(ExtractApplicableCommand, ResolvedApplyWithMaterialisedReturnsCmd) {
  auto e = MakeResolvedEntry(Decision::kApply, RespCommand{.args = {"SET", "k", "v"}});
  auto r = ExtractApplicableCommand(e);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ((*r)->args.front(), "SET");
}

TEST(ExtractApplicableCommand, ResolvedApplyWithoutMaterialisedReturnsNotFound) {
  auto e = MakeResolvedEntry(Decision::kApply, std::nullopt);
  auto r = ExtractApplicableCommand(e);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), ErrorCode::kNotFound);
}

TEST(ExtractApplicableCommand, ResolvedSkipReturnsNotFound) {
  auto e = MakeResolvedEntry(Decision::kSkip, std::nullopt);
  auto r = ExtractApplicableCommand(e);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), ErrorCode::kNotFound);
}

}  // namespace
}  // namespace abyss::core::entry
