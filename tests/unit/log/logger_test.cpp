#include <gtest/gtest.h>

#include "abyss/config/config.h"
#include "abyss/log/log.h"
#include "abyss/log/testing.h"

namespace abyss::log {
namespace {

class LoggerTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(LoggerTest, GetReturnsSameLoggerForSameName) {
  const Logger a = Get("foo");
  const Logger b = Get("foo");
  EXPECT_EQ(a.Name(), "foo");
  EXPECT_EQ(b.Name(), "foo");
}

TEST_F(LoggerTest, InitAppliesDefaultLevel) {
  config::LogConfig cfg;
  cfg.default_level = Level::kWarn;
  cfg.format = "json";
  cfg.sink = "stdout";
  Init(cfg);

  const Logger l = Get("some.component");
  EXPECT_FALSE(l.ShouldLog(Level::kInfo));
  EXPECT_TRUE(l.ShouldLog(Level::kWarn));
  EXPECT_TRUE(l.ShouldLog(Level::kError));
}

TEST_F(LoggerTest, InitAppliesComponentOverride) {
  config::LogConfig cfg;
  cfg.default_level = Level::kWarn;
  cfg.format = "json";
  cfg.sink = "stdout";
  cfg.component_levels.push_back({"noisy.component", Level::kDebug});
  Init(cfg);

  const Logger quiet = Get("quiet.component");
  const Logger noisy = Get("noisy.component");

  EXPECT_FALSE(quiet.ShouldLog(Level::kInfo));
  EXPECT_TRUE(noisy.ShouldLog(Level::kDebug));
  EXPECT_TRUE(noisy.ShouldLog(Level::kInfo));
}

TEST_F(LoggerTest, InitReplacesConfigurationOnRecall) {
  config::LogConfig cfg;
  cfg.default_level = Level::kDebug;
  cfg.format = "json";
  cfg.sink = "stdout";
  Init(cfg);
  const Logger l = Get("foo");
  EXPECT_TRUE(l.ShouldLog(Level::kDebug));

  cfg.default_level = Level::kError;
  Init(cfg);
  EXPECT_FALSE(l.ShouldLog(Level::kWarn));
  EXPECT_TRUE(l.ShouldLog(Level::kError));
}

}  // namespace
}  // namespace abyss::log
