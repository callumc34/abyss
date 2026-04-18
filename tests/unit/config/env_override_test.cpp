#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* prev = std::getenv(name); prev != nullptr) {
      had_previous_ = true;
      previous_ = prev;
    }
    ::setenv(name, value, 1);
  }
  ~ScopedEnv() {
    if (had_previous_) {
      ::setenv(name_.c_str(), previous_.c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
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

}  // namespace
}  // namespace abyss::config
