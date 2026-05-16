#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

#include "temp_dir.h"

namespace abyss::perf {
namespace {

constexpr int kProbeOk = 0;

int RunProbe(std::string_view binary, std::string_view extra_args, const std::string& output_path) {
  std::ostringstream cmd;
  cmd << binary << " --duration 1 --warmup 0 --workers 2 --key-count 100 --value-size-bytes 16 "
      << "--output " << output_path << ' ' << extra_args;
  return std::system(cmd.str().c_str());
}

YAML::Node ParseProbeJson(const std::string& path) {
  std::ifstream in{path};
  EXPECT_TRUE(in.is_open()) << "cannot open probe output: " << path;
  std::stringstream buf;
  buf << in.rdbuf();
  return YAML::Load(buf.str());
}

void AssertReportShape(const YAML::Node& report, const std::string& expected_op) {
  ASSERT_TRUE(report);
  ASSERT_TRUE(report["schema_version"]);
  EXPECT_EQ(report["schema_version"].as<int>(), 1);
  ASSERT_TRUE(report["operations"]);
  ASSERT_TRUE(report["operations"][expected_op])
      << "missing operations." << expected_op << " in report";
  const auto op_node = report["operations"][expected_op];
  EXPECT_GT(op_node["count"].as<uint64_t>(), 0U);
  ASSERT_TRUE(op_node["latency_us"]);
  EXPECT_TRUE(op_node["latency_us"]["p99"]);
  ASSERT_TRUE(report["host"]);
  EXPECT_FALSE(report["host"]["os"].as<std::string>().empty());
  ASSERT_TRUE(report["build"]);
  ASSERT_TRUE(report["classification"]);
}

TEST(ProbeSmokeTest, HotProbeRunsAndEmitsReport) {
  abyss::testing::TempDir dir{"probe_hot"};
  const auto output = dir.Sub("hot.json").string();
  const auto rc = RunProbe(ABYSS_HOT_PROBE_BINARY, "--mix \"hot_get=1.0\"", output);
  ASSERT_EQ(rc, kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "hot_get");
}

TEST(ProbeSmokeTest, BufferProbeRunsAndEmitsReport) {
  abyss::testing::TempDir dir{"probe_buffer"};
  const auto output = dir.Sub("buffer.json").string();
  const auto rc = RunProbe(ABYSS_BUFFER_PROBE_BINARY, "", output);
  ASSERT_EQ(rc, kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "buffer_read");
}

#ifdef ABYSS_HAVE_COLD_PROBE
TEST(ProbeSmokeTest, ColdProbeRunsAndEmitsReport) {
  abyss::testing::TempDir dir{"probe_cold"};
  const auto output = dir.Sub("cold.json").string();
  const auto rc = RunProbe(ABYSS_COLD_PROBE_BINARY, "", output);
  ASSERT_EQ(rc, kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "cold_get");
}
#endif

}  // namespace
}  // namespace abyss::perf
