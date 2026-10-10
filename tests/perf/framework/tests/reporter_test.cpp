#include "reporter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <sstream>
#include <string>

#include "histogram.h"
#include "workload.h"

namespace abyss::perf {
namespace {

RunReport MakeFixtureReport() {
  RunReport r;
  r.run_id = "test-run-1";
  r.started_at = std::chrono::system_clock::time_point{std::chrono::seconds{1'700'000'000}};
  r.duration = std::chrono::seconds{30};
  r.build = {.commit = "abc1234",
             .preset = "default",
             .compiler = "test",
             .build_type = "Debug",
             .sanitizer = "none"};
  r.host = {.os = "darwin", .kernel = "25.4.0", .cpu_model = "M3 Pro", .hostname = "dev"};
  r.classification = HostClassification::kIndicative;

  r.workload.name = "test";
  r.workload.duration = std::chrono::seconds{30};
  r.workload.workers = 4;
  r.workload.key_count = 1000;
  r.workload.mix.weights["GET"] = 0.5;
  r.workload.mix.weights["SET"] = 0.5;
  r.workload.targets.per_op["GET"].p99_us = 100;
  r.workload.targets.per_op["SET"].p99_us = 50;
  r.workload.targets.throughput_ops = 100'000;
  return r;
}

TEST(ReporterTest, StatsFromHistogramComputesThroughput) {
  Histogram h;
  for (int64_t i = 1; i <= 1000; ++i) h.Record(i * 1000);
  const auto stats =
      StatsFromHistogram(h, 1000, std::chrono::nanoseconds{std::chrono::seconds{10}});
  EXPECT_EQ(stats.count, 1000U);
  EXPECT_DOUBLE_EQ(stats.throughput_ops, 100.0);
  EXPECT_GT(stats.p99_ns, stats.p50_ns);
  EXPECT_GT(stats.max_ns, 0);
  EXPECT_FALSE(stats.histogram_b64.empty());
}

TEST(ReporterTest, EvaluateTargetsMarksPassAndFail) {
  auto report = MakeFixtureReport();

  OperationStats get_stats;
  get_stats.count = 1000;
  get_stats.throughput_ops = 60'000;
  get_stats.p99_ns = 80'000;  // 80us, target 100us → pass
  report.operations["GET"] = get_stats;

  OperationStats set_stats;
  set_stats.count = 1000;
  set_stats.throughput_ops = 50'000;
  set_stats.p99_ns = 60'000;  // 60us, target 50us → fail
  report.operations["SET"] = set_stats;

  EvaluateTargets(report);
  ASSERT_FALSE(report.targets.empty());
  EXPECT_FALSE(report.pass) << "SET p99 fails, overall must fail";

  bool found_get = false;
  bool found_set = false;
  bool found_throughput = false;
  for (const auto& t : report.targets) {
    if (t.metric == "operations.GET.p99_us") {
      EXPECT_TRUE(t.pass);
      found_get = true;
    } else if (t.metric == "operations.SET.p99_us") {
      EXPECT_FALSE(t.pass);
      found_set = true;
    } else if (t.metric == "throughput_ops") {
      EXPECT_TRUE(t.pass);
      found_throughput = true;
    }
  }
  EXPECT_TRUE(found_get);
  EXPECT_TRUE(found_set);
  EXPECT_TRUE(found_throughput);
}

TEST(ReporterTest, JsonContainsSchemaVersionAndRequiredFields) {
  auto report = MakeFixtureReport();

  OperationStats get_stats;
  get_stats.count = 500;
  get_stats.throughput_ops = 10'000;
  get_stats.p50_ns = 30'000;
  get_stats.p99_ns = 80'000;
  get_stats.p999_ns = 150'000;
  get_stats.max_ns = 500'000;
  report.operations["GET"] = get_stats;
  EvaluateTargets(report);

  std::ostringstream out;
  WriteReportJson(report, out);
  const auto json = out.str();
  EXPECT_NE(json.find("\"schema_version\":1"), std::string::npos);
  EXPECT_NE(json.find("\"run_id\":\"test-run-1\""), std::string::npos);
  EXPECT_NE(json.find("\"classification\":\"indicative\""), std::string::npos);
  EXPECT_NE(json.find("\"operations\""), std::string::npos);
  EXPECT_NE(json.find("\"GET\""), std::string::npos);
  EXPECT_NE(json.find("\"p99\""), std::string::npos);
  EXPECT_NE(json.find("\"targets\""), std::string::npos);
  EXPECT_NE(json.find("\"pass\""), std::string::npos);
}

TEST(ReporterTest, JsonRecordsServerIdentityAndPipelineDepth) {
  auto report = MakeFixtureReport();
  report.workload.pipeline_depth = 16;
  report.server = ServerIdentity{.kind = "valkey", .version = "8.0.1"};

  std::ostringstream out;
  WriteReportJson(report, out);
  const auto json = out.str();
  EXPECT_NE(json.find(R"("server":{"kind":"valkey","version":"8.0.1"})"), std::string::npos);
  EXPECT_NE(json.find(R"("pipeline_depth":16)"), std::string::npos);
  EXPECT_NE(json.find(R"("schema_version":1)"), std::string::npos);
}

TEST(ReporterTest, JsonRecordsErrorsConfigAndDriver) {
  auto report = MakeFixtureReport();
  OperationStats set_stats;
  set_stats.count = 10;
  set_stats.errors = 3;
  report.operations["SET"] = set_stats;
  report.config["appendfsync"] = "always";
  Histogram lag;
  lag.Record(80'000);
  report.driver = MakeDriverStats(lag, true, std::chrono::seconds{1}, std::chrono::seconds{2},
                                  report.workload.targets);

  std::ostringstream out;
  WriteReportJson(report, out);
  const auto json = out.str();
  EXPECT_NE(json.find(R"("errors":3)"), std::string::npos);
  EXPECT_NE(json.find(R"("config":{"appendfsync":"always"})"), std::string::npos);
  EXPECT_NE(json.find(R"("lagging":true)"), std::string::npos);
  EXPECT_NE(json.find(R"("send_lag_us":{"count":1)"), std::string::npos);
}

TEST(ReporterTest, DriverLagIsJudgedAgainstTargets) {
  WorkloadTargets targets;
  targets.per_op["SET"].p99_us = 100;
  Histogram lag;
  lag.Record(20'000);  // under 50us, but over 10% of the 100us target
  const std::chrono::nanoseconds cpu{0};
  const std::chrono::nanoseconds wall{std::chrono::seconds{1}};
  EXPECT_TRUE(MakeDriverStats(lag, true, cpu, wall, targets).lagging);
  EXPECT_FALSE(MakeDriverStats(lag, true, cpu, wall, WorkloadTargets{}).lagging);
  EXPECT_FALSE(MakeDriverStats(Histogram{}, false, cpu, wall, targets).lagging);
}

// The overload threshold is the larger of one core and 10% of the host.
TEST(ReporterTest, DriverOverloadIsJudgedAgainstTheHost) {
  const std::chrono::nanoseconds wall{std::chrono::seconds{10}};
  const auto idle = MakeDriverStats(Histogram{}, true, std::chrono::seconds{1}, wall, {});
  EXPECT_DOUBLE_EQ(idle.cpu_per_wall_second, 0.1);
  EXPECT_GE(idle.overload_threshold_cores, 1.0);
  EXPECT_FALSE(idle.overloaded);

  const auto busy = MakeDriverStats(Histogram{}, true, std::chrono::seconds{2000}, wall, {});
  EXPECT_DOUBLE_EQ(busy.cpu_per_wall_second, 200.0);
  EXPECT_TRUE(busy.overloaded);
}

TEST(ReporterTest, UnevaluatedTargetsNeverPass) {
  auto report = MakeFixtureReport();
  OperationStats get_stats;
  get_stats.count = 1;
  get_stats.p99_ns = 1'000;
  report.operations["GET"] = get_stats;
  report.workload.targets.throughput_ops.reset();
  report.targets_not_evaluated = "targets not evaluated (closed-loop)";
  EvaluateTargets(report);

  ASSERT_FALSE(report.targets.empty());
  EXPECT_FALSE(report.targets[0].evaluated);
  EXPECT_FALSE(report.pass);
  std::ostringstream out;
  WriteReportJson(report, out);
  EXPECT_NE(out.str().find(R"("targets_evaluated":false)"), std::string::npos);
  EXPECT_NE(out.str().find("not evaluated (closed-loop)"), std::string::npos);
}

TEST(ReporterTest, SweepStepsAndSummaryAreReported) {
  auto report = MakeFixtureReport();
  report.config["wal-fsync-policy"] = "group_commit";
  report.sweep = {{.offered_ops = 1000,
                   .achieved_ops = 999,
                   .p99_ns = 500'000,
                   .pass = true,
                   .queue_entries = 4096},
                  {.offered_ops = 2000, .achieved_ops = 1500, .p99_ns = 9'000'000}};
  report.sweep_result_ops = 1000;

  std::ostringstream json;
  WriteReportJson(report, json);
  EXPECT_NE(json.str().find(R"("sweep_result_ops":1000)"), std::string::npos);
  EXPECT_NE(json.str().find(R"("offered_ops":2000)"), std::string::npos);
  EXPECT_NE(json.str().find(R"("queue_entries":4096)"), std::string::npos);
  EXPECT_EQ(json.str().find(R"("queue_bytes")"), std::string::npos);

  std::ostringstream summary;
  WriteSummary(report, summary);
  EXPECT_NE(summary.str().find("wal-fsync-policy=group_commit"), std::string::npos);
  EXPECT_NE(summary.str().find("sweep result: 1000"), std::string::npos);
}

TEST(ReporterTest, JsonOmitsServerForInProcessRuns) {
  const auto report = MakeFixtureReport();
  std::ostringstream out;
  WriteReportJson(report, out);
  EXPECT_EQ(out.str().find(R"("server":)"), std::string::npos);
}

TEST(ReporterTest, BuildInfoReportsCompilerAndBuildType) {
  const auto b = CurrentBuildInfo();
  EXPECT_FALSE(b.compiler.empty());
  EXPECT_FALSE(b.build_type.empty());
}

TEST(ReporterTest, HostInfoReportsOs) {
  const auto h = CurrentHostInfo();
  EXPECT_FALSE(h.os.empty());
}

TEST(ReporterTest, ClassificationDefaultsIndicativeOnMacOs) {
  const auto h = CurrentHostInfo();
  if (h.os == "darwin") {
    EXPECT_EQ(DetectClassification(), HostClassification::kIndicative)
        << "macOS must classify as indicative regardless of env";
  }
}

}  // namespace
}  // namespace abyss::perf
