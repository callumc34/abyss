#include <gtest/gtest.h>

#include "abyss/config/config.h"
#include "abyss/log/log.h"
#include "abyss/log/testing.h"

namespace abyss::log {
namespace {

class CapturingSinkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    testing::Reset();
    config::LogConfig cfg;
    cfg.default_level = Level::kTrace;
    cfg.format = "json";
    cfg.sink = "stdout";
    Init(cfg);
  }
  void TearDown() override { testing::Reset(); }
};

TEST_F(CapturingSinkTest, ReceivesEmittedRecords) {
  testing::CapturingSink capture;
  const Logger l = Get("fixture");
  ABYSS_LOG_INFO(l, "hello world", {"count", int64_t{3}});

  const auto records = capture.Records();
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records[0].level, Level::kInfo);
  EXPECT_EQ(records[0].component, "fixture");
  EXPECT_EQ(records[0].msg, "hello world");
  ASSERT_EQ(records[0].fields.size(), 1U);
  EXPECT_EQ(records[0].fields[0].first, "count");
  EXPECT_EQ(records[0].fields[0].second, "3");
}

TEST_F(CapturingSinkTest, LevelFilteringAppliesBeforeCapture) {
  config::LogConfig cfg;
  cfg.default_level = Level::kError;
  cfg.format = "json";
  cfg.sink = "stdout";
  Init(cfg);

  testing::CapturingSink capture;
  const Logger l = Get("fixture");
  ABYSS_LOG_DEBUG(l, "debug");
  ABYSS_LOG_INFO(l, "info");
  ABYSS_LOG_ERROR(l, "error");

  const auto records = capture.Records();
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records[0].level, Level::kError);
  EXPECT_EQ(records[0].msg, "error");
}

TEST_F(CapturingSinkTest, MacroSkipsArgEvaluationBelowLevel) {
  config::LogConfig cfg;
  cfg.default_level = Level::kWarn;
  cfg.format = "json";
  cfg.sink = "stdout";
  Init(cfg);

  testing::CapturingSink capture;
  const Logger l = Get("fixture");
  bool evaluated = false;
  const auto expensive = [&]() {
    evaluated = true;
    return std::string_view{"ignored"};
  };
  ABYSS_LOG_DEBUG(l, "gated out", {"k", expensive()});
  EXPECT_FALSE(evaluated);
  EXPECT_EQ(capture.Size(), 0U);
}

TEST_F(CapturingSinkTest, ClearResetsCaptured) {
  testing::CapturingSink capture;
  const Logger l = Get("fixture");
  ABYSS_LOG_INFO(l, "a");
  ABYSS_LOG_INFO(l, "b");
  EXPECT_EQ(capture.Size(), 2U);
  capture.Clear();
  EXPECT_EQ(capture.Size(), 0U);
}

}  // namespace
}  // namespace abyss::log
