#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <string>
#include <string_view>

#include "abyss/log/log.h"
#include "abyss/log/testing.h"

ABYSS_LOG_COMPONENT("pre.init")

namespace abyss::log {
namespace {

class PreInitTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(PreInitTest, LoggerCallsBeforeInitDoNotCrash) {
  ABYSS_LOG_INFO("before init");
  ABYSS_LOG_WARN("another before init", {"k", std::string_view{"v"}});
  SUCCEED();
}

TEST_F(PreInitTest, DefaultLoggerRoutesAtInfoPreInit) {
  const Logger l = Get("pre.init");
  EXPECT_TRUE(l.ShouldLog(Level::kInfo));
  EXPECT_TRUE(l.ShouldLog(Level::kWarn));
}

TEST_F(PreInitTest, DefaultLoggerDropsDebugPreInit) {
  const Logger l = Get("pre.init");
  EXPECT_FALSE(l.ShouldLog(Level::kDebug));
}

// OBS-1: the stderr fallback shares the text escaper, so an injected newline in
// a field value cannot forge a second record there either.
TEST_F(PreInitTest, FallbackEscapesFieldsSoNoRecordIsForged) {
  const std::array<LogField, 1> fields{
      LogField{"reason", std::string_view{"a\nlevel=critical msg=forged"}}};
  const Logger fallback;  // No impl: routes to the pre-Init stderr path.

  ::testing::internal::CaptureStderr();
  fallback.Info("pre init fallback", fields);
  std::fflush(stderr);
  const std::string out = ::testing::internal::GetCapturedStderr();

  EXPECT_EQ(std::ranges::count(out, '\n'), 1);
  EXPECT_NE(out.find("reason=a\\nlevel=critical msg=forged"), std::string::npos) << out;
}

}  // namespace
}  // namespace abyss::log
