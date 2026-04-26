#include <gtest/gtest.h>

#ifdef _WIN32
#else
#include <cstdlib>
#endif

#include <string>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* prev = nullptr;
    prev = std::getenv(name);
    if (prev != nullptr) {
      had_previous_ = true;
      previous_ = prev;
    }
#ifdef _WIN32
    _putenv_s(name, value);
#else
    ::setenv(name, value, 1);
#endif
  }
  ~ScopedEnv() {
    if (had_previous_) {
#ifdef _WIN32
      _putenv_s(name_.c_str(), previous_.c_str());
#else
      ::setenv(name_.c_str(), previous_.c_str(), 1);
#endif
    } else {
#ifdef _WIN32
      _putenv_s(name_.c_str(), "");
#else
      ::unsetenv(name_.c_str());
#endif
    }
  }

  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;
  ScopedEnv(ScopedEnv&&) = delete;
  ScopedEnv& operator=(ScopedEnv&&) = delete;

 private:
  std::string name_;
  bool had_previous_ = false;
  std::string previous_;
};

TEST(ConfigEnvOverride, AbyssProfileOverridesYaml) {
  const ScopedEnv env("ABYSS_PROFILE", "external");
  auto cfg = Config::ParseFromYaml("profile: embedded\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->profile, "external");
}

TEST(ConfigEnvOverride, EmptyAbyssProfileIgnored) {
  const ScopedEnv env("ABYSS_PROFILE", "");
  auto cfg = Config::ParseFromYaml("profile: embedded\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->profile, "embedded");
}

TEST(ConfigEnvOverride, AbyssProfileValidated) {
  const ScopedEnv env("ABYSS_PROFILE", "not_a_profile");
  auto cfg = Config::ParseFromYaml("");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("profile"), std::string::npos);
}

TEST(ConfigEnvOverride, ApplyEnvironmentOverridesIsIdempotent) {
  const ScopedEnv env("ABYSS_PROFILE", "hybrid");
  Config cfg = Config::Defaults();
  cfg.ApplyEnvironmentOverrides();
  cfg.ApplyEnvironmentOverrides();
  EXPECT_EQ(cfg.profile, "hybrid");
}

TEST(ConfigEnvOverride, AbyssLogLevelOverridesYaml) {
  const ScopedEnv env("ABYSS_LOG_LEVEL", "error");
  auto cfg = Config::ParseFromYaml("log:\n  level: debug\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->log.default_level, log::Level::kError);
}

TEST(ConfigEnvOverride, AbyssLogFormatOverridesYaml) {
  const ScopedEnv env("ABYSS_LOG_FORMAT", "text");
  auto cfg = Config::ParseFromYaml("log:\n  format: json\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->log.format, "text");
}

TEST(ConfigEnvOverride, AbyssLogSinkOverridesYaml) {
  const ScopedEnv env("ABYSS_LOG_SINK", "stderr");
  auto cfg = Config::ParseFromYaml("log:\n  sink: stdout\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->log.sink, "stderr");
}

TEST(ConfigEnvOverride, AbyssMetricsEnabledOverridesYaml) {
  const ScopedEnv env("ABYSS_METRICS_ENABLED", "false");
  auto cfg = Config::ParseFromYaml("metrics:\n  enabled: true\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_FALSE(cfg->metrics.enabled);
}

TEST(ConfigEnvOverride, AbyssMetricsPortOverridesYaml) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "9100");
  auto cfg = Config::ParseFromYaml("metrics:\n  port: 9090\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->metrics.port, 9100);
}

TEST(ConfigEnvOverride, AbyssMetricsBindOverridesYaml) {
  const ScopedEnv env("ABYSS_METRICS_BIND", "127.0.0.1");
  auto cfg = Config::ParseFromYaml("metrics:\n  bind: 0.0.0.0\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->metrics.bind, "127.0.0.1");
}

}  // namespace
}  // namespace abyss::config
