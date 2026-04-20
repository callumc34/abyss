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

TEST(JsonSink, EmitsRequiredFields) {
  const std::array<LogField, 2> fields{
      LogField{"reason", std::string_view{"quiet"}},
      LogField{"batch_size", int64_t{42}},
  };
  const std::string out = testing::FormatJson(Level::kInfo, "cold.flush", "flush complete", fields);

  EXPECT_TRUE(out.starts_with("{"));
  EXPECT_TRUE(out.ends_with("}\n"));
  EXPECT_TRUE(Contains(out, R"("ts":")"));
  EXPECT_TRUE(Contains(out, R"("level":"info")"));
  EXPECT_TRUE(Contains(out, R"("component":"cold.flush")"));
  EXPECT_TRUE(Contains(out, R"("msg":"flush complete")"));
  EXPECT_TRUE(Contains(out, R"("reason":"quiet")"));
  EXPECT_TRUE(Contains(out, R"("batch_size":42)"));
}

TEST(JsonSink, EscapesControlAndQuotesInStrings) {
  const std::array<LogField, 1> fields{LogField{"raw", std::string_view{"a\"b\nc"}}};
  const std::string out = testing::FormatJson(Level::kInfo, "x", "m", fields);
  EXPECT_TRUE(Contains(out, R"("raw":"a\"b\nc")"));
}

TEST(JsonSink, RendersSupportedValueTypes) {
  const std::array<LogField, 4> fields{
      LogField{"b", true},
      LogField{"i", int64_t{-7}},
      LogField{"u", uint64_t{9}},
      LogField{"d", 1.5},
  };
  const std::string out = testing::FormatJson(Level::kInfo, "x", "m", fields);
  EXPECT_TRUE(Contains(out, R"("b":true)"));
  EXPECT_TRUE(Contains(out, R"("i":-7)"));
  EXPECT_TRUE(Contains(out, R"("u":9)"));
  EXPECT_TRUE(Contains(out, R"("d":)"));
}

TEST(JsonSink, EmitsNewlineTerminatedSingleLine) {
  const std::string out = testing::FormatJson(Level::kInfo, "x", "m", {});
  EXPECT_EQ(out.back(), '\n');
  const size_t newlines = std::count(out.begin(), out.end(), '\n');
  EXPECT_EQ(newlines, 1U);
}

}  // namespace
}  // namespace abyss::log
