#include "abyss/branding/banner.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "abyss/version.h"

namespace abyss::branding {
namespace {

std::string CaptureBanner(const BannerOptions& opts) {
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
  std::FILE* f = std::tmpfile();
  if (f == nullptr) return {};
  PrintBanner(f, opts);
  std::fseek(f, 0, SEEK_SET);
  std::string out;
  std::array<char, 256> buf{};
  while (std::size_t n = std::fread(buf.data(), 1, buf.size(), f)) {
    out.append(buf.data(), n);
  }
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
  std::fclose(f);
  return out;
}

TEST(BannerTest, SuppressedWritesNothing) {
  const std::string out = CaptureBanner({.suppressed = true, .profile = "embedded"});
  EXPECT_TRUE(out.empty());
}

TEST(BannerTest, RenderContainsVersion) {
  const std::string out = CaptureBanner({.suppressed = false, .profile = "embedded"});
  EXPECT_NE(out.find(kVersion), std::string::npos);
}

TEST(BannerTest, RenderContainsBuildType) {
  const std::string out = CaptureBanner({.suppressed = false, .profile = "embedded"});
  EXPECT_NE(out.find(kBuildType), std::string::npos);
}

TEST(BannerTest, RenderContainsProfile) {
  const std::string out = CaptureBanner({.suppressed = false, .profile = "embedded"});
  EXPECT_NE(out.find("embedded profile"), std::string::npos);
}

TEST(BannerTest, RenderContainsTagline) {
  const std::string out = CaptureBanner({.suppressed = false, .profile = "embedded"});
  EXPECT_NE(out.find("Redis-compatible KV store"), std::string::npos);
}

TEST(BannerTest, RenderContainsShortShaWhenKnown) {
  const std::string_view sha{kBuildCommit};
  if (sha == "unknown") {
    GTEST_SKIP() << "git SHA unavailable in this build";
  }
  const std::string out = CaptureBanner({.suppressed = false, .profile = "embedded"});
  const std::string_view expected = sha.substr(0, sha.size() < 7 ? sha.size() : 7);
  EXPECT_NE(out.find(expected), std::string::npos);
}

TEST(BannerTest, VersionLineOmitsProfileWhenEmpty) {
  EXPECT_EQ(VersionLine({}).find("profile"), std::string::npos);
}

TEST(BannerTest, VersionLineIncludesProfileWhenProvided) {
  EXPECT_NE(VersionLine("embedded").find("embedded profile"), std::string::npos);
}

TEST(BannerTest, VersionLineStartsWithV) {
  // NOLINTNEXTLINE(bugprone-unused-local-non-trivial-variable)
  const std::string line = VersionLine({});
  ASSERT_FALSE(line.empty());
  EXPECT_EQ(line.front(), 'v');
}

#ifndef _WIN32
TEST(BannerTest, SuppressedByEnvTrueForTruthyValues) {
  for (const char* value : {"1", "true", "TRUE", "yes", "on", "On"}) {
    ::setenv("ABYSS_NO_BANNER", value, 1);
    EXPECT_TRUE(SuppressedByEnv()) << "value=" << value;
  }
  ::unsetenv("ABYSS_NO_BANNER");
}

TEST(BannerTest, SuppressedByEnvFalseForUnsetOrEmpty) {
  ::unsetenv("ABYSS_NO_BANNER");
  EXPECT_FALSE(SuppressedByEnv());
  ::setenv("ABYSS_NO_BANNER", "", 1);
  EXPECT_FALSE(SuppressedByEnv());
  ::unsetenv("ABYSS_NO_BANNER");
}

TEST(BannerTest, SuppressedByEnvFalseForOtherValues) {
  for (const char* value : {"0", "false", "no", "off", "garbage"}) {
    ::setenv("ABYSS_NO_BANNER", value, 1);
    EXPECT_FALSE(SuppressedByEnv()) << "value=" << value;
  }
  ::unsetenv("ABYSS_NO_BANNER");
}
#endif

}  // namespace
}  // namespace abyss::branding
