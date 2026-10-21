#include "abyss/core/fatal.h"

#include <gtest/gtest.h>

#include <string>

#include "fatal_capture.h"

namespace abyss::core {
namespace {

TEST(FatalTest, RunsTheInstalledHandlerWithTheReason) {
  const testing::ScopedFatalCapture capture;
  std::string reason;
  // Nothing follows Fatal in the try: it does not return.
  try {
    Fatal("invariant broken: shard 3");
  } catch (const testing::FatalCalled& fatal) {
    reason = fatal.reason;
  }
  EXPECT_EQ(reason, "invariant broken: shard 3");
}

#ifndef NDEBUG
TEST(FatalDeathTest, AFailedDcheckIsFatal) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  int checked = 0;
  ABYSS_DCHECK(++checked == 1, "never fires");
  EXPECT_EQ(checked, 1);
  EXPECT_DEATH(ABYSS_DCHECK(checked == 2, "dcheck: checked is not 2"), "dcheck: checked is not 2");
}
#else
TEST(FatalTest, ADcheckEvaluatesNothingUnderNdebug) {
  int evaluated = 0;
  ABYSS_DCHECK(++evaluated == 5, "never evaluated");
  EXPECT_EQ(evaluated, 0);
}
#endif

}  // namespace
}  // namespace abyss::core
