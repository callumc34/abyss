#include <gtest/gtest.h>
#include <sys/wait.h>
#include <yaml-cpp/yaml.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
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
  // NOLINTNEXTLINE(bugprone-command-processor)
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
  const auto rc = RunProbe(ABYSS_HOT_PROBE_BINARY, "", output);
  ASSERT_EQ(rc, kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "hot_get");
  AssertReportShape(report, "hot_apply");
}

TEST(ProbeSmokeTest, HotProbeRejectsUnknownOp) {
  abyss::testing::TempDir dir{"probe_hot_unknown"};
  const auto output = dir.Sub("hot.json").string();
  EXPECT_NE(RunProbe(ABYSS_HOT_PROBE_BINARY, "--mix \"hot_set=1.0\"", output), kProbeOk);
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

#ifdef ABYSS_HAVE_WRITE_PROBE
std::string WriteProbeArgs(const abyss::testing::TempDir& dir, std::string_view extra,
                           std::string_view durability = "process_crash") {
  return "--durability " + std::string{durability} +
         " --shards 2 --flush-samples 6 --flush-concurrency 2 --wal-path " +
         dir.Sub("wal").string() + " --cold-path " + dir.Sub("cold").string() + ' ' +
         std::string{extra};
}

TEST(ProbeSmokeTest, WriteProbeClosedLoopLeavesTargetsUnevaluated) {
  abyss::testing::TempDir dir{"probe_write"};
  const auto output = dir.Sub("write.json").string();
  ASSERT_EQ(
      RunProbe(ABYSS_WRITE_PROBE_BINARY, WriteProbeArgs(dir, "--prefill-entries 300"), output),
      kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "write_ack");
  AssertReportShape(report, "device_flush");
  AssertReportShape(report, "device_flush_concurrent");
  EXPECT_EQ(report["operations"]["write_ack"]["errors"].as<uint64_t>(), 0U);
  EXPECT_EQ(report["operations"]["device_flush"]["count"].as<uint64_t>(), 6U);
  EXPECT_EQ(report["operations"]["device_flush_concurrent"]["count"].as<uint64_t>(), 6U);

  EXPECT_EQ(report["config"]["durability"].as<std::string>(), "process_crash");
  EXPECT_EQ(report["config"]["prefill_entries"].as<std::string>(), "300");
  EXPECT_EQ(report["config"]["workers"].as<std::string>(), "2");
  EXPECT_FALSE(report["driver"]["open_loop"].as<bool>());

  ASSERT_EQ(report["targets"].size(), 1U);
  EXPECT_EQ(report["targets"][0]["metric"].as<std::string>(), "operations.write_ack.p99_us");
  EXPECT_FALSE(report["targets"][0]["evaluated"].as<bool>());
  EXPECT_FALSE(report["targets_evaluated"].as<bool>());
  EXPECT_FALSE(report["pass"].as<bool>());
}

TEST(ProbeSmokeTest, WriteProbeOpenLoopEvaluatesTargets) {
  abyss::testing::TempDir dir{"probe_write_open"};
  const auto output = dir.Sub("write.json").string();
  ASSERT_EQ(
      RunProbe(ABYSS_WRITE_PROBE_BINARY, WriteProbeArgs(dir, "--target-rate-ops 200"), output),
      kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "write_ack");
  EXPECT_TRUE(report["driver"]["open_loop"].as<bool>());
  EXPECT_GT(report["driver"]["send_lag_us"]["count"].as<uint64_t>(), 0U);
  ASSERT_EQ(report["targets"].size(), 1U);
  EXPECT_TRUE(report["targets"][0]["evaluated"].as<bool>());
  EXPECT_EQ(report["targets"][0]["target"].as<double>(), 20.0);
  EXPECT_TRUE(report["targets_evaluated"].as<bool>());
}

// W2: under power_loss the target is twice the calibrated flush p99 plus
// headroom, so it always exceeds the headroom alone.
TEST(ProbeSmokeTest, WriteProbePowerLossEvaluatesTheFlushBoundTarget) {
  abyss::testing::TempDir dir{"probe_write_power"};
  const auto output = dir.Sub("write.json").string();
  ASSERT_EQ(RunProbe(ABYSS_WRITE_PROBE_BINARY,
                     WriteProbeArgs(dir, "--target-rate-ops 200", "power_loss"), output),
            kProbeOk);
  const auto report = ParseProbeJson(output);
  AssertReportShape(report, "write_ack");
  EXPECT_EQ(report["config"]["durability"].as<std::string>(), "power_loss");
  ASSERT_EQ(report["targets"].size(), 1U);
  EXPECT_TRUE(report["targets"][0]["evaluated"].as<bool>());
  EXPECT_GT(report["targets"][0]["target"].as<double>(), 50.0);
}

TEST(ProbeSmokeTest, WriteProbeRejectsAnUnknownDurability) {
  abyss::testing::TempDir dir{"probe_write_bad_durability"};
  const auto rc = RunProbe(ABYSS_WRITE_PROBE_BINARY, WriteProbeArgs(dir, "", "fsync_none"),
                           dir.Sub("w.json").string());
  ASSERT_NE(rc, kProbeOk);
  EXPECT_EQ(WEXITSTATUS(rc), 2);
}

TEST(ProbeSmokeTest, WriteProbeRejectsGateInClosedLoop) {
  abyss::testing::TempDir dir{"probe_write_gate"};
  const auto rc =
      RunProbe(ABYSS_WRITE_PROBE_BINARY, WriteProbeArgs(dir, "--gate"), dir.Sub("w.json").string());
  ASSERT_NE(rc, kProbeOk);
  EXPECT_EQ(WEXITSTATUS(rc), 2);
}

TEST(ProbeSmokeTest, WriteProbeRefusesANonEmptyWal) {
  abyss::testing::TempDir dir{"probe_write_dirty"};
  std::filesystem::create_directories(dir.Sub("wal"));
  std::ofstream{dir.Sub("wal") / "leftover"} << "x";
  const auto rc =
      RunProbe(ABYSS_WRITE_PROBE_BINARY,
               "--durability process_crash --shards 2 --wal-path " + dir.Sub("wal").string(),
               dir.Sub("write.json").string());
  EXPECT_NE(rc, kProbeOk);
}
#endif

}  // namespace
}  // namespace abyss::perf
