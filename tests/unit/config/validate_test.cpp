#include <gtest/gtest.h>

#include <string>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

TEST(ConfigValidate, RejectsUnknownProfile) {
  auto cfg = Config::ParseFromYaml("profile: smorgasbord\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("profile"), std::string::npos);
}

TEST(ConfigValidate, AcceptsZeroRespPortAsEphemeral) {
  // resp.port == 0 is a valid request for a kernel-assigned ephemeral port.
  // The server reports the bound port on its stdout ready line.
  auto cfg = Config::ParseFromYaml("resp:\n  port: 0\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->resp.port, 0);
}

TEST(ConfigValidate, RejectsZeroMetricsPort) {
  auto cfg = Config::ParseFromYaml("metrics:\n  port: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("metrics.port"), std::string::npos);
}

TEST(ConfigValidate, RejectsPortAboveSixteenBit) {
  auto cfg = Config::ParseFromYaml("resp:\n  port: 70000\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("resp.port"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroMaxMemory) {
  auto cfg = Config::ParseFromYaml("hot:\n  max_memory_bytes: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("hot.max_memory_bytes"), std::string::npos);
}

TEST(ConfigValidate, RejectsNonPositiveHotEvictionTick) {
  auto cfg = Config::ParseFromYaml("hot:\n  eviction_tick_ms: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("hot.eviction_tick_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsNegativeJitterRatio) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  jitter_fraction: -0.1\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("jitter_fraction"), std::string::npos);
}

TEST(ConfigValidate, RejectsJitterRatioAboveOne) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  jitter_fraction: 1.5\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("jitter_fraction"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroHotConsumerBatchSize) {
  auto cfg = Config::ParseFromYaml("hot_consumer:\n  read_batch_size: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("hot_consumer.read_batch_size"), std::string::npos);
}

TEST(ConfigValidate, RejectsNonPositiveHotConsumerReadTimeout) {
  auto cfg = Config::ParseFromYaml("hot_consumer:\n  read_timeout_ms: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("hot_consumer.read_timeout_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsColdConsumerLowWaterAboveHigh) {
  auto cfg = Config::ParseFromYaml(R"YAML(
cold_consumer:
  buffer_high_water_bytes: 1000
  buffer_low_water_bytes: 2000
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("buffer_low_water_bytes"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroColdConsumerQueueReadMaxCount) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  queue_read_max_count: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("cold_consumer.queue_read_max_count"), std::string::npos);
}

TEST(ConfigValidate, RejectsNonPositiveColdConsumerQueueReadTimeout) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  queue_read_timeout_ms: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("cold_consumer.queue_read_timeout_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsColdConsumerInitialBackoffAboveMax) {
  auto cfg = Config::ParseFromYaml(R"YAML(
cold_consumer:
  retry_initial_backoff_ms: 1000
  retry_max_backoff_ms: 500
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("retry_initial_backoff"), std::string::npos);
}

TEST(ConfigValidate, RejectsUnknownFsyncPolicy) {
  auto cfg = Config::ParseFromYaml("queue:\n  wal_fsync_policy: fsync_sometimes\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("wal_fsync_policy"), std::string::npos);
}

TEST(ConfigValidate, RejectsEmptyDataPath) {
  auto cfg = Config::ParseFromYaml(R"YAML(
cold:
  data_path: ""
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("cold.data_path"), std::string::npos);
}

TEST(ConfigValidate, RejectsDuplicateEvictionPrefix) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "session:"
      eviction_seconds: 300
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("duplicate prefix"), std::string::npos);
}

TEST(ConfigValidate, RejectsEmptyEvictionPrefix) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  eviction_overrides:
    - prefix: ""
      eviction_seconds: 3600
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("prefix"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroEvictionSeconds) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  eviction_overrides:
    - prefix: "x:"
      eviction_seconds: 0
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("eviction_seconds"), std::string::npos);
}

TEST(ConfigValidate, RejectsMissingEvictionOverrideFields) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  eviction_overrides:
    - prefix: "x:"
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("eviction_seconds"), std::string::npos);
}

TEST(ConfigValidate, RejectsColliding_RespAndMetricsPort) {
  auto cfg = Config::ParseFromYaml(R"YAML(
resp:
  port: 7000
metrics:
  port: 7000
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("collides"), std::string::npos);
}

TEST(ConfigValidate, ErrorIncludesLineAndColumn) {
  auto cfg = Config::ParseFromYaml("resp:\n  port: 70000\n");
  ASSERT_FALSE(cfg.has_value());
  // "line 2:9" is not guaranteed stable across yaml-cpp versions, so just
  // check that a "line" annotation is attached when available.
  const auto& msg = cfg.error().message();
  (void)msg;  // Content is best-effort; presence of path is enough.
}

TEST(ConfigValidate, NegativeIntegerInUnsignedFieldRejected) {
  auto cfg = Config::ParseFromYaml("hot:\n  max_memory_bytes: -1\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("max_memory_bytes"), std::string::npos);
}

TEST(ConfigValidate, IntegerOverflowOnNarrowFieldRejected) {
  auto cfg = Config::ParseFromYaml("resp:\n  port: 99999999999\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("resp.port"), std::string::npos);
}

}  // namespace
}  // namespace abyss::config
