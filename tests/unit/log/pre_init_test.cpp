#include <gtest/gtest.h>

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

}  // namespace
}  // namespace abyss::log
