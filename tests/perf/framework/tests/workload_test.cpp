#include "workload.h"

#include <gtest/gtest.h>

#include <string>

namespace abyss::perf {
namespace {

constexpr const char* kValidWorkload = R"(
name: hot-read-heavy
description: 95/5 GET/SET zipfian
duration_seconds: 60
warmup_seconds: 10
workers: 4
connections_per_worker: 4
target_rate_ops: 100000
key_count: 1000000
key_distribution:
  kind: zipfian
  theta: 0.99
  seed: 42
value_size_bytes: 64
mix:
  GET: 0.95
  SET: 0.05
preload:
  enabled: true
  key_count: 1000000
  value_size_bytes: 64
targets:
  throughput_ops: 100000
  per_op:
    GET:
      p50_us: 50
      p99_us: 100
    SET:
      p99_us: 50
)";

TEST(WorkloadTest, ParsesValidYaml) {
  auto result = ParseWorkloadYaml(kValidWorkload);
  ASSERT_TRUE(result.has_value()) << "parse failed: " << result.error().message();
  EXPECT_EQ(result->name, "hot-read-heavy");
  EXPECT_EQ(result->duration.count(), 60);
  EXPECT_EQ(result->warmup.count(), 10);
  EXPECT_EQ(result->workers, 4);
  EXPECT_EQ(result->target_rate_ops, 100'000U);
  EXPECT_EQ(result->key_count, 1'000'000U);
  EXPECT_EQ(result->key_distribution.kind, KeyDistConfig::Kind::kZipfian);
  EXPECT_DOUBLE_EQ(result->key_distribution.theta, 0.99);
  EXPECT_EQ(result->mix.weights.at("GET"), 0.95);
  EXPECT_EQ(result->mix.weights.at("SET"), 0.05);
  EXPECT_TRUE(result->preload.enabled);
  EXPECT_TRUE(result->targets.throughput_ops.has_value());
  EXPECT_EQ(*result->targets.throughput_ops, 100'000);
  ASSERT_TRUE(result->targets.per_op.contains("GET"));
  EXPECT_EQ(*result->targets.per_op.at("GET").p99_us, 100);
}

TEST(WorkloadTest, RejectsMixWithBadWeightSum) {
  constexpr const char* kBadMix = R"(
name: bad
duration_seconds: 60
key_count: 1000
key_distribution:
  kind: uniform
mix:
  GET: 0.5
  SET: 0.3
)";
  auto result = ParseWorkloadYaml(kBadMix);
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("sum"), std::string::npos);
}

TEST(WorkloadTest, RejectsMissingName) {
  constexpr const char* kMissingName = R"(
duration_seconds: 60
key_count: 1000
key_distribution:
  kind: uniform
mix:
  GET: 1.0
)";
  auto result = ParseWorkloadYaml(kMissingName);
  ASSERT_FALSE(result.has_value());
}

TEST(WorkloadTest, RejectsZeroDuration) {
  constexpr const char* kZeroDuration = R"(
name: x
duration_seconds: 0
key_count: 1000
key_distribution:
  kind: uniform
mix:
  GET: 1.0
)";
  auto result = ParseWorkloadYaml(kZeroDuration);
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("duration_seconds"), std::string::npos);
}

TEST(WorkloadTest, RejectsTargetForUnknownOp) {
  constexpr const char* kBadTarget = R"(
name: x
duration_seconds: 1
key_count: 1
key_distribution:
  kind: uniform
mix:
  GET: 1.0
targets:
  per_op:
    SET:
      p99_us: 50
)";
  auto result = ParseWorkloadYaml(kBadTarget);
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("SET"), std::string::npos);
}

TEST(WorkloadTest, RejectsUnknownDistribution) {
  constexpr const char* kBadDist = R"(
name: x
duration_seconds: 1
key_count: 1
key_distribution:
  kind: pareto
mix:
  GET: 1.0
)";
  auto result = ParseWorkloadYaml(kBadDist);
  ASSERT_FALSE(result.has_value());
}

}  // namespace
}  // namespace abyss::perf
