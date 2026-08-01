#include <gtest/gtest.h>

#ifdef _WIN32
#else
#include <cstdlib>
#endif

#include <string>

#include "abyss/config/config.h"
#include "abyss/core/result.h"

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
  ASSERT_TRUE(cfg.ApplyEnvironmentOverrides().has_value());
  ASSERT_TRUE(cfg.ApplyEnvironmentOverrides().has_value());
  EXPECT_EQ(cfg.profile, "hybrid");
}

// An empty value is indistinguishable from an unset variable, so this also
// covers "absent variable is not an error".
TEST(ConfigEnvOverride, AbsentVariablesAreNotAnError) {
  const ScopedEnv level("ABYSS_LOG_LEVEL", "");
  const ScopedEnv enabled("ABYSS_METRICS_ENABLED", "");
  const ScopedEnv port("ABYSS_METRICS_PORT", "");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_TRUE(r.has_value()) << r.error().message();
  EXPECT_EQ(cfg.metrics.port, Config::Defaults().metrics.port);
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

// OBS-2: a present-but-unparseable override is rejected by name, never dropped
// in favour of the file/default value.
TEST(ConfigEnvOverride, EnvOverrideRejectsUnparseableLevel) {
  const ScopedEnv env("ABYSS_LOG_LEVEL", "garbage");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_NE(r.error().message().find("ABYSS_LOG_LEVEL"), std::string::npos);
  EXPECT_NE(r.error().message().find("garbage"), std::string::npos);
  EXPECT_EQ(cfg.log.default_level, Config::Defaults().log.default_level);
}

TEST(ConfigEnvOverride, EnvOverrideRejectsUnparseableMetricsEnabled) {
  const ScopedEnv env("ABYSS_METRICS_ENABLED", "maybe");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_NE(r.error().message().find("ABYSS_METRICS_ENABLED"), std::string::npos);
  EXPECT_NE(r.error().message().find("maybe"), std::string::npos);
}

TEST(ConfigEnvOverride, EnvOverrideRejectsNonNumericMetricsPort) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "foo");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_NE(r.error().message().find("ABYSS_METRICS_PORT"), std::string::npos);
  EXPECT_NE(r.error().message().find("foo"), std::string::npos);
  EXPECT_EQ(cfg.metrics.port, Config::Defaults().metrics.port);
}

TEST(ConfigEnvOverride, EnvOverrideRejectsZeroMetricsPort) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "0");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("ABYSS_METRICS_PORT"), std::string::npos);
}

TEST(ConfigEnvOverride, EnvOverrideRejectsOutOfRangeMetricsPort) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "70000");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("ABYSS_METRICS_PORT"), std::string::npos);
  EXPECT_NE(r.error().message().find("70000"), std::string::npos);
}

TEST(ConfigEnvOverride, EnvOverrideRejectsTrailingGarbageInMetricsPort) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "9090x");
  Config cfg = Config::Defaults();
  auto r = cfg.ApplyEnvironmentOverrides();
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("ABYSS_METRICS_PORT"), std::string::npos);
}

// The typo must fail startup, not run on a silently-substituted default: the
// error has to escape the config-load entry point that main() calls.
TEST(ConfigEnvOverride, BadEnvOverrideFailsConfigLoad) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "foo");
  auto cfg = Config::ParseFromYaml("metrics:\n  port: 9090\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_EQ(cfg.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_NE(cfg.error().message().find("ABYSS_METRICS_PORT"), std::string::npos);
}

TEST(ConfigEnvOverride, BadEnvOverrideFailsEmptyDocumentLoad) {
  const ScopedEnv env("ABYSS_LOG_LEVEL", "garbage");
  auto cfg = Config::ParseFromYaml("");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("ABYSS_LOG_LEVEL"), std::string::npos);
}

TEST(ConfigEnvOverride, ValidMetricsPortStillApplies) {
  const ScopedEnv env("ABYSS_METRICS_PORT", "9123");
  auto cfg = Config::ParseFromYaml("metrics:\n  port: 9090\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->metrics.port, 9123);
}

// LOG_FORMAT/LOG_SINK pass through raw; the validator owns their allowed set,
// so a bad value must still fail the load (just with the validator's message).
TEST(ConfigEnvOverride, BadLogFormatRejectedByValidator) {
  const ScopedEnv env("ABYSS_LOG_FORMAT", "yaml");
  auto cfg = Config::ParseFromYaml("");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("log.format"), std::string::npos);
}

}  // namespace
}  // namespace abyss::config
