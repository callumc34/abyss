#include "progress_watchdog.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

namespace abyss::testing {
namespace {

// What a stall prints: the marker line, then on POSIX the state too
// (gtest's Windows regex has no alternation to span lines).
#ifdef _WIN32
constexpr const char* kPrinted = "WATCHDOG: no progress for .*s at seed 3 step 7 restart open";
#else
constexpr const char* kPrinted =
    "WATCHDOG: no progress for .*s at seed 3 step 7 restart open(.|\n)*WATCHDOG state: pool idle";
#endif

// Never beaten again, it names where it last was and the state, then
// aborts the process.
TEST(ProgressWatchdogDeathTest, AStallAbortsNamingWhereItStopped) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        ProgressWatchdog watchdog(
            "start", [] { return std::string("pool idle"); }, std::chrono::seconds{1});
        watchdog.Beat("seed 3 step 7 restart open");
        std::this_thread::sleep_for(std::chrono::hours{1});
      },
      kPrinted);
}

}  // namespace
}  // namespace abyss::testing
