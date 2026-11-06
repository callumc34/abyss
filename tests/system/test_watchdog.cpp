#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>

#include "progress_watchdog.h"

namespace abyss::system_test {
namespace {

// Three times the slowest system test seen on CI (31.8 s, on a Windows
// runner whose disk had slowed), and well inside the suite's TIMEOUT.
constexpr std::chrono::seconds kTestBudget{100};

// One test that overruns its budget aborts the suite, naming the test
// and dumping every thread's stack, rather than spending the whole
// suite's ctest TIMEOUT.
class TestWatchdog : public ::testing::EmptyTestEventListener {
 public:
  void OnTestProgramStart(const ::testing::UnitTest& /*unit*/) override {
    watchdog_.emplace("starting", ProgressWatchdog::Dump{}, kTestBudget);
  }
  void OnTestStart(const ::testing::TestInfo& test) override {
    Beat(std::string(test.test_suite_name()) + "." + test.name());
  }
  void OnTestEnd(const ::testing::TestInfo& test) override {
    Beat("after " + std::string(test.test_suite_name()) + "." + test.name());
  }
  void OnTestProgramEnd(const ::testing::UnitTest& /*unit*/) override { watchdog_.reset(); }

 private:
  using ProgressWatchdog = abyss::testing::ProgressWatchdog;

  void Beat(const std::string& where) {
    if (watchdog_.has_value()) watchdog_->Beat(where);
  }

  std::optional<ProgressWatchdog> watchdog_;
};

// Registered before main; gtest owns what it is given.
// NOLINTBEGIN(cert-err58-cpp,bugprone-throwing-static-initialization)
// NOLINTBEGIN(cppcoreguidelines-owning-memory)
[[maybe_unused]] const bool kTestWatchdog = [] {
  ::testing::UnitTest::GetInstance()->listeners().Append(new TestWatchdog);
  return true;
}();
// NOLINTEND(cppcoreguidelines-owning-memory)
// NOLINTEND(cert-err58-cpp,bugprone-throwing-static-initialization)

}  // namespace
}  // namespace abyss::system_test
