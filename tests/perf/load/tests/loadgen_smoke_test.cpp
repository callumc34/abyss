#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "server_fixture.h"
#include "temp_dir.h"

namespace abyss::perf {
namespace {

constexpr int kLoadgenOk = 0;

class LoadgenFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    server_ = std::make_unique<system_test::TestServer>();
    if (!server_->Start()) {
      GTEST_SKIP() << "could not start abyss-server for loadgen test: " << server_->SkipReason();
    }
  }

  void TearDown() override {
    if (server_) server_->Stop();
  }

  std::string Endpoint() const { return "127.0.0.1:" + std::to_string(server_->Port()); }
  std::string MetricsUrl() const {
    return "http://127.0.0.1:" + std::to_string(server_->MetricsPort());
  }

  std::unique_ptr<system_test::TestServer> server_;
};

std::string WriteSmokeWorkload(const std::filesystem::path& dir) {
  const auto path = dir / "smoke.yaml";
  std::ofstream out{path};
  out << R"(name: loadgen-smoke
description: short integration run for the load generator
duration_seconds: 2
warmup_seconds: 0
workers: 1
connections_per_worker: 2
target_rate_ops: 0
key_count: 1000
key_distribution:
  kind: uniform
  seed: 1
value_size_bytes: 32
mix:
  GET: 0.5
  SET: 0.5
preload:
  enabled: true
  key_count: 1000
  value_size_bytes: 32
targets:
  per_op:
    GET:
      p99_us: 5000
    SET:
      p99_us: 5000
)";
  return path.string();
}

TEST_F(LoadgenFixture, RunsAgainstRealServerAndEmitsReport) {
  abyss::testing::TempDir dir{"loadgen_smoke"};
  const auto workload_path = WriteSmokeWorkload(dir.Path());
  const auto output_path = dir.Sub("report.json").string();

  std::ostringstream cmd;
  cmd << ABYSS_LOADGEN_BINARY << " --workload " << workload_path << " --server " << Endpoint()
      << " --metrics-url " << MetricsUrl() << " --output " << output_path;
  const int rc = std::system(cmd.str().c_str());
  ASSERT_EQ(rc, kLoadgenOk);

  std::ifstream in{output_path};
  ASSERT_TRUE(in.is_open());
  std::stringstream buf;
  buf << in.rdbuf();
  const auto report = YAML::Load(buf.str());

  ASSERT_TRUE(report);
  EXPECT_EQ(report["schema_version"].as<int>(), 1);
  ASSERT_TRUE(report["operations"]);
  ASSERT_TRUE(report["operations"]["GET"]);
  ASSERT_TRUE(report["operations"]["SET"]);
  EXPECT_GT(report["operations"]["GET"]["count"].as<uint64_t>(), 0U);
  EXPECT_GT(report["operations"]["SET"]["count"].as<uint64_t>(), 0U);
  ASSERT_TRUE(report["server_metrics"]);
  // start, mid, end snapshots — three entries when scraping is enabled.
  EXPECT_EQ(report["server_metrics"].size(), 3U);
  EXPECT_FALSE(report["host"]["os"].as<std::string>().empty());
}

}  // namespace
}  // namespace abyss::perf
