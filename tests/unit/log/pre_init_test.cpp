#include <gtest/gtest.h>

#include "abyss/log/log.h"
#include "abyss/log/testing.h"

namespace abyss::log {
namespace {

class PreInitTest : public ::testing::Test {
 protected:
  void SetUp() override { testing::Reset(); }
  void TearDown() override { testing::Reset(); }
};

TEST_F(PreInitTest, LoggerCallsBeforeInitDoNotCrash) {
  const Logger l = Get("pre.init");
  ABYSS_LOG_INFO(l, "before init");
  ABYSS_LOG_WARN(l, "another before init", {"k", std::string_view{"v"}});
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

}  // namespace
}  // namespace abyss::log
