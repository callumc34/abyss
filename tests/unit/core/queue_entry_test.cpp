#include "abyss/core/queue_entry.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <variant>
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

QueueEntry MakeResolvedEntry(Decision decision, std::vector<RespCommand> materialised_ops) {
  return QueueEntry{
      .seq = 1,
      .appended_at = WallClock::now(),
      .payload =
          entry::Resolved{.decision = decision, .materialised_ops = std::move(materialised_ops)},
  };
}

TEST(QueueEntryVariant, WriteCarriesCommand) {
  auto e = MakeWriteEntry({"SET", "k", "v"});
  ASSERT_TRUE(std::holds_alternative<Write>(e.payload));
  const auto& w = std::get<Write>(e.payload);
  EXPECT_EQ(w.cmd.args.front(), "SET");
}

TEST(QueueEntryVariant, ConditionalCarriesCommandAndFlags) {
  auto e = MakeConditionalEntry();
  ASSERT_TRUE(std::holds_alternative<Conditional>(e.payload));
  const auto& c = std::get<Conditional>(e.payload);
  EXPECT_EQ(c.cmd.args.front(), "SET");
  EXPECT_EQ(c.flags, PredicateFlags::kNone);
}

TEST(QueueEntryVariant, ResolvedApplyCarriesMaterialisedOps) {
  auto e = MakeResolvedEntry(Decision::kApply, {RespCommand{.args = {"SET", "k", "v"}}});
  ASSERT_TRUE(std::holds_alternative<Resolved>(e.payload));
  const auto& r = std::get<Resolved>(e.payload);
  EXPECT_EQ(r.decision, Decision::kApply);
  ASSERT_EQ(r.materialised_ops.size(), 1U);
  EXPECT_EQ(r.materialised_ops[0].args.front(), "SET");
}

TEST(QueueEntryVariant, ResolvedSkipHasEmptyMaterialisedOps) {
  auto e = MakeResolvedEntry(Decision::kSkip, {});
  ASSERT_TRUE(std::holds_alternative<Resolved>(e.payload));
  const auto& r = std::get<Resolved>(e.payload);
  EXPECT_EQ(r.decision, Decision::kSkip);
  EXPECT_TRUE(r.materialised_ops.empty());
}

TEST(QueueEntryVariant, ResolvedSupportsMultipleMaterialisedOps) {
  auto e = MakeResolvedEntry(
      Decision::kApply,
      {RespCommand{.args = {"DEL", "src"}}, RespCommand{.args = {"SET", "dst", "v"}},
       RespCommand{.args = {"PEXPIREAT", "dst", "1700000000000"}}});
  const auto& r = std::get<Resolved>(e.payload);
  EXPECT_EQ(r.materialised_ops.size(), 3U);
  EXPECT_EQ(r.materialised_ops[0].args[0], "DEL");
  EXPECT_EQ(r.materialised_ops[1].args[0], "SET");
  EXPECT_EQ(r.materialised_ops[2].args[0], "PEXPIREAT");
}

}  // namespace
}  // namespace abyss::core::entry
