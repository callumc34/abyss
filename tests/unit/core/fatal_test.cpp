#include "abyss/core/fatal.h"

#include <gtest/gtest.h>

#include <string>

#include "fatal_capture.h"

namespace abyss::core {
namespace {

TEST(FatalTest, RunsTheInstalledHandlerWithTheReason) {
  const testing::ScopedFatalCapture capture;
  try {
    Fatal("invariant broken: shard 3");
    FAIL() << "Fatal returned";
  } catch (const testing::FatalCalled& fatal) {
    EXPECT_EQ(fatal.reason, "invariant broken: shard 3");
  }
}

}  // namespace
}  // namespace abyss::core
