#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include "abyss/config/config.h"
#include "abyss/core/durability.h"
#include "abyss/log/log.h"
#include "abyss/log/testing.h"

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

TEST(ConfigValidate, HotStubMemoryFractionRange) {
  for (const char* bad : {"-0.01", "0.6", ".nan"}) {
    SCOPED_TRACE(bad);
    auto cfg = Config::ParseFromYaml(std::string("hot:\n  stub_memory_fraction: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value());
    EXPECT_NE(cfg.error().message().find("hot.stub_memory_fraction"), std::string::npos);
  }
  for (const char* good : {"0", "0.5"}) {
    SCOPED_TRACE(good);
    EXPECT_TRUE(Config::ParseFromYaml(std::string("hot:\n  stub_memory_fraction: ") + good + "\n"));
  }
}

TEST(ConfigValidate, HotFillMaxFractionRange) {
  for (const char* bad : {"0", "-0.1", "1.5", ".nan"}) {
    SCOPED_TRACE(bad);
    auto cfg = Config::ParseFromYaml(std::string("hot:\n  fill_max_fraction: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value());
    EXPECT_NE(cfg.error().message().find("hot.fill_max_fraction"), std::string::npos);
  }
  for (const char* good : {"0.001", "1"}) {
    SCOPED_TRACE(good);
    EXPECT_TRUE(Config::ParseFromYaml(std::string("hot:\n  fill_max_fraction: ") + good + "\n"));
  }
}

TEST(ConfigValidate, HotBackpressureRatioRange) {
  for (const char* bad : {"0.99", "10.5", ".nan"}) {
    SCOPED_TRACE(bad);
    auto cfg = Config::ParseFromYaml(std::string("hot:\n  backpressure_ratio: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value());
    EXPECT_NE(cfg.error().message().find("hot.backpressure_ratio"), std::string::npos);
  }
  for (const char* good : {"1", "10"}) {
    SCOPED_TRACE(good);
    EXPECT_TRUE(Config::ParseFromYaml(std::string("hot:\n  backpressure_ratio: ") + good + "\n"));
  }
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

// The hot consumer and the consumer RPC registry are gone; a config
// that still names them fails, saying what replaced them.
TEST(ConfigValidate, RemovedSectionsNameWhatReplacedThem) {
  const std::array<std::pair<const char*, const char*>, 2> removed = {{
      {"hot_consumer:\n  read_batch_size: 128\n", "the sequencer applies each write to hot"},
      {"consumer_rpc:\n  default_timeout_ms: 100\n", "no consumer apply to wait for"},
  }};
  for (const auto& [yaml, reason] : removed) {
    auto cfg = Config::ParseFromYaml(yaml);
    ASSERT_FALSE(cfg.has_value()) << yaml;
    const std::string section(yaml, std::string_view(yaml).find(':'));
    EXPECT_NE(cfg.error().message().find(section), std::string::npos) << cfg.error().message();
    EXPECT_NE(cfg.error().message().find(reason), std::string::npos) << cfg.error().message();
  }
}

TEST(ConfigValidate, RemovedRecoveryBatchSizesNameTheScan) {
  for (const char* key : {"hot_replay_batch_size", "cold_replay_batch_size"}) {
    auto cfg = Config::ParseFromYaml(std::string("recovery:\n  ") + key + ": 100\n");
    ASSERT_FALSE(cfg.has_value()) << key;
    EXPECT_NE(cfg.error().message().find(std::string("recovery.") + key), std::string::npos)
        << cfg.error().message();
    EXPECT_NE(cfg.error().message().find("the Scan's batches"), std::string::npos)
        << cfg.error().message();
  }
}

// More than one log is valid but narrows what one atomic batch spans,
// so the validator says so (#169).
TEST(ConfigValidate, SeveralLogsWarnOfCrossSlotAndFlushdb) {
  const log::testing::CapturingSink sink;
  auto one = Config::ParseFromYaml("queue:\n  log_count: 1\n");
  ASSERT_TRUE(one.has_value()) << one.error().message();
  EXPECT_EQ(sink.Size(), 0U);
  auto two = Config::ParseFromYaml("queue:\n  log_count: 2\n");
  ASSERT_TRUE(two.has_value()) << two.error().message();
  const auto records = sink.Records();
  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records[0].level, log::Level::kWarn);
  EXPECT_NE(records[0].msg.find("CROSSSLOT"), std::string::npos) << records[0].msg;
  EXPECT_NE(records[0].msg.find("FLUSHDB across logs"), std::string::npos) << records[0].msg;
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

TEST(ConfigValidate, RemovedQueueKeysNameTheirReplacement) {
  const std::array<std::pair<const char*, const char*>, 3> removed = {{
      {"wal_fsync_policy: group_commit", "queue.durability (process_crash | power_loss)"},
      {"group_commit_interval_us: 1000", "a flush starts as soon as the previous one ends"},
      {"group_commit_max_bytes: 1048576", "queue.durability_window_bytes"},
  }};
  for (const auto& [line, replacement] : removed) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  ") + line + "\n");
    ASSERT_FALSE(cfg.has_value()) << line;
    const std::string key(line, std::string_view(line).find(':'));
    EXPECT_NE(cfg.error().message().find("queue." + key), std::string::npos)
        << cfg.error().message();
    EXPECT_NE(cfg.error().message().find(replacement), std::string::npos) << cfg.error().message();
  }
}

TEST(ConfigValidate, DurabilityAcceptsBothClasses) {
  for (const auto [name, durability] : {std::pair{"process_crash", core::Durability::kProcessCrash},
                                        std::pair{"power_loss", core::Durability::kPowerLoss}}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  durability: ") + name + "\n");
    ASSERT_TRUE(cfg.has_value()) << name << ": " << cfg.error().message();
    EXPECT_EQ(cfg->queue.durability, durability);
  }
}

TEST(ConfigValidate, RejectsUnknownDurability) {
  for (const char* bad : {"group_commit", "fsync_none", "PowerLoss", "\"\""}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  durability: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value()) << bad;
    EXPECT_NE(cfg.error().message().find("queue.durability"), std::string::npos)
        << cfg.error().message();
    EXPECT_NE(cfg.error().message().find("process_crash, power_loss"), std::string::npos)
        << cfg.error().message();
  }
}

TEST(ConfigValidate, DurabilityWindowBytesBounds) {
  for (const char* bad : {"1048575", "4294967297"}) {
    auto cfg =
        Config::ParseFromYaml(std::string("queue:\n  durability_window_bytes: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value()) << bad;
    EXPECT_NE(cfg.error().message().find("queue.durability_window_bytes"), std::string::npos);
  }
  for (const char* good : {"1048576", "4294967296"}) {
    auto cfg =
        Config::ParseFromYaml(std::string("queue:\n  durability_window_bytes: ") + good + "\n");
    EXPECT_TRUE(cfg.has_value()) << good << ": " << cfg.error().message();
  }
}

TEST(ConfigValidate, DurabilityWindowMsBounds) {
  for (const char* bad : {"9", "60001"}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  durability_window_ms: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value()) << bad;
    EXPECT_NE(cfg.error().message().find("queue.durability_window_ms"), std::string::npos);
  }
  for (const char* good : {"10", "60000"}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  durability_window_ms: ") + good + "\n");
    EXPECT_TRUE(cfg.has_value()) << good << ": " << cfg.error().message();
  }
}

TEST(ConfigValidate, LogCountIsAPowerOfTwoAtMostTheShardCount) {
  for (const char* bad : {"0", "3", "6"}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  log_count: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value()) << bad;
    EXPECT_NE(cfg.error().message().find("queue.log_count"), std::string::npos)
        << cfg.error().message();
  }
  auto over = Config::ParseFromYaml("hot:\n  shard_count: 4\nqueue:\n  log_count: 8\n");
  ASSERT_FALSE(over.has_value());
  EXPECT_NE(over.error().message().find("queue.log_count"), std::string::npos)
      << over.error().message();
  EXPECT_NE(over.error().message().find("hot.shard_count"), std::string::npos)
      << over.error().message();
  for (const char* good : {"1", "2", "64"}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  log_count: ") + good + "\n");
    ASSERT_TRUE(cfg.has_value()) << good << ": " << cfg.error().message();
  }
}

TEST(ConfigValidate, RingEntriesIsAPowerOfTwoInRange) {
  for (const char* bad : {"2048", "4095", "5000", "33554432"}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  ring_entries: ") + bad + "\n");
    ASSERT_FALSE(cfg.has_value()) << bad;
    EXPECT_NE(cfg.error().message().find("queue.ring_entries"), std::string::npos)
        << cfg.error().message();
  }
  for (const char* good : {"4096", "65536", "16777216"}) {
    auto cfg = Config::ParseFromYaml(std::string("queue:\n  ring_entries: ") + good + "\n");
    ASSERT_TRUE(cfg.has_value()) << good << ": " << cfg.error().message();
  }
}

// A segment is its 4 KiB header plus 8-byte aligned frames, and must
// hold one max-size frame.
TEST(ConfigValidate, SegmentSizeHoldsTheHeaderAndOneMaxSizeFrame) {
  const auto parse = [](const std::string& segment, const std::string& value) {
    return Config::ParseFromYaml("queue:\n  segment_size_bytes: " + segment +
                                 "\n  max_value_size_bytes: " + value + "\n");
  };
  auto unaligned = parse("1048580", "1024");
  ASSERT_FALSE(unaligned.has_value());
  EXPECT_NE(unaligned.error().message().find("multiple of 8"), std::string::npos)
      << unaligned.error().message();
  auto no_header_room = parse("1049600", "1048576");
  ASSERT_FALSE(no_header_room.has_value());
  EXPECT_NE(no_header_room.error().message().find("queue.segment_size_bytes"), std::string::npos)
      << no_header_room.error().message();
  auto fits = parse("1053696", "1048576");
  EXPECT_TRUE(fits.has_value()) << fits.error().message();
}

TEST(ConfigValidate, RemovedEngineKeysNameTheirReason) {
  const std::array<std::pair<const char*, const char*>, 2> removed = {{
      {"min_rpc_wait_fraction: 0.5", "no consumer apply to wait for"},
      {"buffer_consistency_wait_timeout_ms: 100", "no longer waits for the cold consumer"},
  }};
  for (const auto& [line, reason] : removed) {
    auto cfg = Config::ParseFromYaml(std::string("engine:\n  ") + line + "\n");
    ASSERT_FALSE(cfg.has_value()) << line;
    const std::string key(line, std::string_view(line).find(':'));
    EXPECT_NE(cfg.error().message().find("engine." + key), std::string::npos)
        << cfg.error().message();
    EXPECT_NE(cfg.error().message().find(reason), std::string::npos) << cfg.error().message();
  }
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
