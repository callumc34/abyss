#include <gtest/gtest.h>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

TEST(LogConfigDefaults, SensibleValues) {
  const LogConfig defaults;
  EXPECT_EQ(defaults.format, "json");
  EXPECT_EQ(defaults.sink, "stdout");
  EXPECT_TRUE(defaults.component_levels.empty());
#ifdef NDEBUG
  EXPECT_EQ(defaults.default_level, log::Level::kInfo);
#else
  EXPECT_EQ(defaults.default_level, log::Level::kDebug);
#endif
}

TEST(LogConfigParse, RoundTripsAllFields) {
  constexpr auto kYaml = R"(
log:
  level: warn
  format: text
  sink: stderr
  component_levels:
    - component: queue.segment
      level: debug
    - component: cold.flush
      level: trace
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();

  EXPECT_EQ(cfg->log.default_level, log::Level::kWarn);
  EXPECT_EQ(cfg->log.format, "text");
  EXPECT_EQ(cfg->log.sink, "stderr");
  ASSERT_EQ(cfg->log.component_levels.size(), 2U);
  EXPECT_EQ(cfg->log.component_levels[0].component, "queue.segment");
  EXPECT_EQ(cfg->log.component_levels[0].level, log::Level::kDebug);
  EXPECT_EQ(cfg->log.component_levels[1].component, "cold.flush");
  EXPECT_EQ(cfg->log.component_levels[1].level, log::Level::kTrace);
}

TEST(LogConfigParse, UnknownLevelRejected) {
  constexpr auto kYaml = R"(
log:
  level: verbose
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("log level"), std::string::npos);
}

TEST(LogConfigParse, UnknownFormatRejected) {
  constexpr auto kYaml = R"(
log:
  format: binary
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("log.format"), std::string::npos);
}

TEST(LogConfigParse, UnknownSinkRejected) {
  constexpr auto kYaml = R"(
log:
  sink: file
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("log.sink"), std::string::npos);
}

TEST(LogConfigParse, DuplicateComponentRejected) {
  constexpr auto kYaml = R"(
log:
  component_levels:
    - component: x
      level: debug
    - component: x
      level: info
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("duplicate"), std::string::npos);
}

TEST(LogConfigParse, UnknownKeyRejected) {
  constexpr auto kYaml = R"(
log:
  format: json
  sink: stdout
  typo: something
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("typo"), std::string::npos);
}

TEST(MetricsConfigParse, EnabledRoundTrips) {
  constexpr auto kYaml = R"(
metrics:
  enabled: false
  bind: 127.0.0.1
  port: 9999
)";
  auto cfg = Config::ParseFromYaml(kYaml);
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_FALSE(cfg->metrics.enabled);
  EXPECT_EQ(cfg->metrics.bind, "127.0.0.1");
  EXPECT_EQ(cfg->metrics.port, 9999);
}

TEST(MetricsConfigDefaults, EnabledTrueByDefault) {
  const MetricsConfig defaults;
  EXPECT_TRUE(defaults.enabled);
  EXPECT_EQ(defaults.port, 9090);
  EXPECT_EQ(defaults.bind, "0.0.0.0");
}

}  // namespace
}  // namespace abyss::config
