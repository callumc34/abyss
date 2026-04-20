#include <gtest/gtest.h>

#include "abyss/config/config.h"
#include "abyss/log/log.h"
#include "abyss/log/testing.h"

namespace abyss::log {
namespace {

TEST(LogReset, ClearsInitializedState) {
  config::LogConfig cfg;
  cfg.default_level = Level::kWarn;
  cfg.format = "json";
  cfg.sink = "stdout";
  Init(cfg);
  EXPECT_TRUE(Initialized());

  testing::Reset();
  EXPECT_FALSE(Initialized());
}

TEST(LogReset, ClearsLoggerCache) {
  config::LogConfig cfg;
  cfg.default_level = Level::kWarn;
  cfg.format = "json";
  cfg.sink = "stdout";
  Init(cfg);

  const Logger a = Get("ephemeral");
  EXPECT_FALSE(a.ShouldLog(Level::kInfo));

  testing::Reset();
  // After Reset(), a fresh lookup returns a logger backed by the pre-Init
  // stderr fallback (INFO-level), not the previous WARN-level config.
  const Logger b = Get("ephemeral");
  EXPECT_TRUE(b.ShouldLog(Level::kInfo));
}

}  // namespace
}  // namespace abyss::log
