#include <gtest/gtest.h>

#include <algorithm>
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

// OBS-1: the text path must escape control bytes in BOTH keys and values, the
// same way the JSON path already does, so the two formatters cannot diverge.
TEST(TextSink, FormatterEscapesControlBytes) {
  const std::string raw = std::string("a\nb\rc\td") + char{1} + "e";
  const std::string key = std::string("k\ne y=z") + char{2};
  const std::array<LogField, 1> fields{LogField{key, std::string_view{raw}}};

  const std::string out = testing::FormatText(Level::kInfo, "x", "m", fields);
  EXPECT_EQ(std::ranges::count(out, '\n'), 1);
  EXPECT_EQ(out.back(), '\n');
  EXPECT_TRUE(Contains(out, "a\\nb\\rc\\td"));
  // Keys also escape the ' ' and '=' field separators.
  EXPECT_TRUE(Contains(out, "k\\ne\\ y\\=z"));
  EXPECT_EQ(std::ranges::count(out, char{1}), 0) << "raw control byte survived the value path";
  EXPECT_EQ(std::ranges::count(out, char{2}), 0) << "raw control byte survived the key path";

  // The JSON path already escaped these; the text path must now agree.
  const std::string json = testing::FormatJson(Level::kInfo, "x", "m", fields);
  EXPECT_TRUE(Contains(json, "a\\nb\\rc\\td"));
  EXPECT_EQ(std::ranges::count(json, char{1}), 0);
}

// OBS-1: a value carrying a newline plus forged key=value pairs must stay one
// record; no extra output line is produced.
TEST(TextSink, InjectionAttemptDoesNotForgeRecord) {
  const std::array<LogField, 1> fields{
      LogField{"reason", std::string_view{"a\nlevel=critical msg=forged"}}};

  const std::string out = testing::FormatText(Level::kInfo, "x", "m", fields);
  EXPECT_EQ(std::ranges::count(out, '\n'), 1);
  EXPECT_EQ(out.back(), '\n');
  EXPECT_TRUE(Contains(out, "reason=a\\nlevel=critical msg=forged"));
}

}  // namespace
}  // namespace abyss::log
