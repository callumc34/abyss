#include <gtest/gtest.h>

#include "abyss/log/log.h"

namespace abyss::log {
namespace {

TEST(LogLevel, ParseRecognisedSpellings) {
  Level level = Level::kOff;
  EXPECT_TRUE(ParseLevel("trace", level));
  EXPECT_EQ(level, Level::kTrace);
  EXPECT_TRUE(ParseLevel("debug", level));
  EXPECT_EQ(level, Level::kDebug);
  EXPECT_TRUE(ParseLevel("info", level));
  EXPECT_EQ(level, Level::kInfo);
  EXPECT_TRUE(ParseLevel("warn", level));
  EXPECT_EQ(level, Level::kWarn);
  EXPECT_TRUE(ParseLevel("warning", level));
  EXPECT_EQ(level, Level::kWarn);
  EXPECT_TRUE(ParseLevel("error", level));
  EXPECT_EQ(level, Level::kError);
  EXPECT_TRUE(ParseLevel("err", level));
  EXPECT_EQ(level, Level::kError);
  EXPECT_TRUE(ParseLevel("critical", level));
  EXPECT_EQ(level, Level::kCritical);
  EXPECT_TRUE(ParseLevel("fatal", level));
  EXPECT_EQ(level, Level::kCritical);
  EXPECT_TRUE(ParseLevel("off", level));
  EXPECT_EQ(level, Level::kOff);
  EXPECT_TRUE(ParseLevel("none", level));
  EXPECT_EQ(level, Level::kOff);
}

TEST(LogLevel, ParseIsCaseInsensitive) {
  Level level = Level::kOff;
  EXPECT_TRUE(ParseLevel("INFO", level));
  EXPECT_EQ(level, Level::kInfo);
  EXPECT_TRUE(ParseLevel("WaRn", level));
  EXPECT_EQ(level, Level::kWarn);
}

TEST(LogLevel, ParseRejectsUnknown) {
  Level level = Level::kInfo;
  EXPECT_FALSE(ParseLevel("verbose", level));
  EXPECT_FALSE(ParseLevel("", level));
  EXPECT_FALSE(ParseLevel("nope", level));
}

TEST(LogLevel, ToStringViewMatchesCanonicalSpelling) {
  EXPECT_EQ(ToStringView(Level::kTrace), "trace");
  EXPECT_EQ(ToStringView(Level::kDebug), "debug");
  EXPECT_EQ(ToStringView(Level::kInfo), "info");
  EXPECT_EQ(ToStringView(Level::kWarn), "warn");
  EXPECT_EQ(ToStringView(Level::kError), "error");
  EXPECT_EQ(ToStringView(Level::kCritical), "critical");
  EXPECT_EQ(ToStringView(Level::kOff), "off");
}

}  // namespace
}  // namespace abyss::log
