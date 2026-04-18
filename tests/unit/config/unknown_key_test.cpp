#include <gtest/gtest.h>

#include <string>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

TEST(ConfigUnknownKey, TopLevelUnknownRejected) {
  auto cfg = Config::ParseFromYaml("nonsense: 1\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("nonsense"), std::string::npos);
  EXPECT_NE(cfg.error().message().find("unknown"), std::string::npos);
}

TEST(ConfigUnknownKey, SectionUnknownRejected) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  max_memoryy_bytes: 1024
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("max_memoryy_bytes"), std::string::npos);
}

TEST(ConfigUnknownKey, EvictionOverrideUnknownRejected) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  eviction_overrides:
    - prefix: "x:"
      eviction_seconds: 10
      extra: 1
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("extra"), std::string::npos);
}

TEST(ConfigUnknownKey, PathIncludesSectionAndField) {
  auto cfg = Config::ParseFromYaml("resp:\n  porttt: 6380\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("resp.porttt"), std::string::npos);
}

}  // namespace
}  // namespace abyss::config
