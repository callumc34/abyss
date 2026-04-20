#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

#include "abyss/log/log.h"
#include "abyss/log/testing.h"

namespace abyss::log {
namespace {

bool Contains(std::string_view hay, std::string_view needle) { return hay.contains(needle); }

TEST(TextSink, IncludesLevelComponentAndMessage) {
  const std::string out = testing::FormatText(Level::kWarn, "queue.segment", "lag crossed", {});
  EXPECT_TRUE(Contains(out, "[warning]") || Contains(out, "[warn]"));
  EXPECT_TRUE(Contains(out, "queue.segment"));
  EXPECT_TRUE(Contains(out, "lag crossed"));
  EXPECT_EQ(out.back(), '\n');
}

TEST(TextSink, AppendsStructuredFieldsAsKvPairs) {
  const std::array<LogField, 2> fields{
      LogField{"reason", std::string_view{"pressure"}},
      LogField{"batch_size", int64_t{100}},
  };
  const std::string out = testing::FormatText(Level::kInfo, "cold.flush", "flush done", fields);
  EXPECT_TRUE(Contains(out, "reason=pressure"));
  EXPECT_TRUE(Contains(out, "batch_size=100"));
}

}  // namespace
}  // namespace abyss::log
