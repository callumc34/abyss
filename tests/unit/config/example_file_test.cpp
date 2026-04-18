#include <gtest/gtest.h>

#include <filesystem>

#include "abyss/config/config.h"

#ifndef ABYSS_EXAMPLE_CONFIG_PATH
#error "ABYSS_EXAMPLE_CONFIG_PATH must be defined at compile time"
#endif

namespace abyss::config {
namespace {

TEST(ConfigExampleFile, ShippedExampleLoadsCleanly) {
  const std::filesystem::path path{ABYSS_EXAMPLE_CONFIG_PATH};
  ASSERT_TRUE(std::filesystem::exists(path)) << path;

  auto cfg = Config::LoadFromFile(path);
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->profile, "embedded");
}

TEST(ConfigExampleFile, MissingFileReportsPath) {
  auto cfg = Config::LoadFromFile("/nonexistent/abyss-does-not-exist.yaml");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("/nonexistent/abyss-does-not-exist.yaml"),
            std::string::npos);
}

}  // namespace
}  // namespace abyss::config
