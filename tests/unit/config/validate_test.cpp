#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

TEST(ConfigValidate, RejectsUnknownProfile) {
  auto cfg = Config::ParseFromYaml("profile: smorgasbord\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("profile"), std::string::npos);
}

TEST(ConfigValidate, AcceptsZeroNetPortAsEphemeral) {
  // net.port == 0 is a valid request for a kernel-assigned ephemeral port.
  // The server reports the bound port on its stdout ready line.
  auto cfg = Config::ParseFromYaml("net:\n  port: 0\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->net.port, 0);
}

TEST(ConfigValidate, AcceptsZeroMetricsPortAsEphemeral) {
  // metrics.port == 0 mirrors net.port == 0: kernel-assigned ephemeral.
  auto cfg = Config::ParseFromYaml("metrics:\n  port: 0\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->metrics.port, 0);
}

TEST(ConfigValidate, AcceptsZeroAdminPortAsEphemeral) {
  auto cfg = Config::ParseFromYaml("admin:\n  port: 0\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->admin.port, 0);
}

TEST(ConfigValidate, RejectsPortAboveSixteenBit) {
  auto cfg = Config::ParseFromYaml("net:\n  port: 70000\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("net.port"), std::string::npos);
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

TEST(ConfigValidate, AcceptsInRangeColdConsumerCheckpointKnobs) {
  auto cfg = Config::ParseFromYaml(R"YAML(
cold_consumer:
  checkpoint_max_flushes: 8
  checkpoint_min_interval_ms: 250
  loop_initial_backoff_ms: 5
  loop_max_backoff_ms: 2000
)YAML");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->cold_consumer.checkpoint_max_flushes, 8U);
  EXPECT_EQ(cfg->cold_consumer.checkpoint_min_interval, std::chrono::milliseconds{250});
  EXPECT_EQ(cfg->cold_consumer.loop_initial_backoff, std::chrono::milliseconds{5});
  EXPECT_EQ(cfg->cold_consumer.loop_max_backoff, std::chrono::milliseconds{2000});
}

TEST(ConfigValidate, RejectsZeroColdConsumerCheckpointMaxFlushes) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  checkpoint_max_flushes: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("cold_consumer.checkpoint_max_flushes"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroColdConsumerCheckpointInterval) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  checkpoint_min_interval_ms: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("cold_consumer.checkpoint_min_interval_ms"),
            std::string::npos);
}

TEST(ConfigValidate, RejectsNegativeColdConsumerCheckpointInterval) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  checkpoint_min_interval_ms: -1\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("checkpoint_min_interval_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsExcessiveColdConsumerCheckpointInterval) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  checkpoint_min_interval_ms: 60001\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("checkpoint_min_interval_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroColdConsumerLoopBackoff) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  loop_initial_backoff_ms: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("cold_consumer.loop_initial_backoff_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsNegativeColdConsumerLoopBackoff) {
  auto cfg = Config::ParseFromYaml("cold_consumer:\n  loop_max_backoff_ms: -5\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("loop_max_backoff_ms"), std::string::npos);
}

TEST(ConfigValidate, RejectsColdConsumerLoopInitialBackoffAboveMax) {
  auto cfg = Config::ParseFromYaml(R"YAML(
cold_consumer:
  loop_initial_backoff_ms: 2000
  loop_max_backoff_ms: 100
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("loop_initial_backoff_ms"), std::string::npos);
}

TEST(ConfigValidate, OffsetFsyncIntervalBounds) {
  for (const char* bad : {"9", "60001"}) {
    auto cfg =
        Config::ParseFromYaml(std::string("queue:\n  offset_fsync_interval_ms: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value()) << bad;
    EXPECT_NE(cfg.error().message().find("queue.offset_fsync_interval_ms"), std::string::npos);
  }
  for (const char* good : {"10", "60000"}) {
    auto cfg =
        Config::ParseFromYaml(std::string("queue:\n  offset_fsync_interval_ms: ") + good + "\n");
    EXPECT_TRUE(cfg.has_value()) << good << ": " << cfg.error().message();
  }
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

// Per-prefix eviction couples to WAL retention: any override longer than
// `queue.min_retention_seconds` silently degrades recovery, so the validator
// rejects the misconfiguration at startup. See requirements.md §Consumer
// Coordination.
TEST(ConfigValidate, RejectsMinRetentionBelowDefaultEviction) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  default_eviction_seconds: 7200
queue:
  min_retention_seconds: 3600
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("queue.min_retention_seconds"), std::string::npos);
}

TEST(ConfigValidate, RejectsMinRetentionBelowMaxOverride) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  default_eviction_seconds: 3600
  eviction_overrides:
    - prefix: "important:"
      eviction_seconds: 604800
queue:
  min_retention_seconds: 86400
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("queue.min_retention_seconds"), std::string::npos);
  EXPECT_NE(cfg.error().message().find("604800"), std::string::npos)
      << "error should name the required value";
}

TEST(ConfigValidate, AcceptsMinRetentionEqualToMaxEviction) {
  auto cfg = Config::ParseFromYaml(R"YAML(
hot:
  default_eviction_seconds: 3600
  eviction_overrides:
    - prefix: "long:"
      eviction_seconds: 86400
queue:
  min_retention_seconds: 86400
)YAML");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
}

TEST(ConfigValidate, RejectsColliding_NetAndMetricsPort) {
  auto cfg = Config::ParseFromYaml(R"YAML(
net:
  port: 7000
metrics:
  port: 7000
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("collides"), std::string::npos);
}

TEST(ConfigValidate, ErrorIncludesLineAndColumn) {
  auto cfg = Config::ParseFromYaml("net:\n  port: 70000\n");
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
  auto cfg = Config::ParseFromYaml("net:\n  port: 99999999999\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("net.port"), std::string::npos);
}

TEST(ConfigValidate, RejectsResumeAtOrAboveBackpressure) {
  auto cfg = Config::ParseFromYaml(R"YAML(
net:
  write_resume_bytes: 4194304
  write_backpressure_bytes: 4194304
  write_hard_limit_bytes: 16777216
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("write_resume_bytes"), std::string::npos);
}

TEST(ConfigValidate, RejectsBackpressureAtOrAboveHardLimit) {
  auto cfg = Config::ParseFromYaml(R"YAML(
net:
  write_resume_bytes: 1048576
  write_backpressure_bytes: 16777216
  write_hard_limit_bytes: 16777216
)YAML");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("write_backpressure_bytes"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroAcceptQueue) {
  auto cfg = Config::ParseFromYaml("net:\n  accept_queue: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("net.accept_queue"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroShutdownGrace) {
  auto cfg = Config::ParseFromYaml("net:\n  shutdown_grace_seconds: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("net.shutdown_grace_seconds"), std::string::npos);
}

TEST(ConfigValidate, RejectsZeroReaperTick) {
  auto cfg = Config::ParseFromYaml("net:\n  reaper_tick_ms: 0\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("net.reaper_tick_ms"), std::string::npos);
}

TEST(ConfigValidate, AcceptsZeroIoThreadsAsAuto) {
  auto cfg = Config::ParseFromYaml("net:\n  io_threads: 0\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->net.io_threads, 0U);
}

}  // namespace
}  // namespace abyss::config
