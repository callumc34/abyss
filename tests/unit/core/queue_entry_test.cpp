#include "abyss/core/queue_entry.h"

#include <gtest/gtest.h>

#include <string>
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

TEST(QueueEntryVariant, WriteCarriesCommand) {
  auto e = MakeWriteEntry({"SET", "k", "v"});
  ASSERT_TRUE(std::holds_alternative<Write>(e.payload));
  const auto& w = std::get<Write>(e.payload);
  EXPECT_EQ(w.cmd.args.front(), "SET");
}

// The log carries decided effects and Flushes, nothing else.
TEST(QueueEntryVariant, OnlyWritesAndFlushesAreEntries) {
  static_assert(std::variant_size_v<decltype(QueueEntry::payload)> == 2);
  const QueueEntry flush{.seq = 1, .payload = entry::Flush{}};
  EXPECT_TRUE(std::holds_alternative<Flush>(flush.payload));
  EXPECT_FALSE(flush.replaces_state);
}

}  // namespace
}  // namespace abyss::core::entry
