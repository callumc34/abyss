#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

constexpr const char* kFullYaml = R"YAML(
profile: embedded

hot:
  backend: builtin_hashmap
  max_memory_bytes: 4294967296
  default_eviction_seconds: 86400
  eviction_overrides:
    - prefix: "session:"
      eviction_seconds: 3600
    - prefix: "ephemeral:"
      eviction_seconds: 300

cold:
  backend: builtin_rocksdb
  data_path: /data/cold
  write_buffer_size_bytes: 67108864

queue:
  backend: builtin_wal
  wal_path: /data/wal
  segment_size_bytes: 134217728
  min_retention_seconds: 86400
  offset_fsync_interval_ms: 250
  wal_fsync_policy: group_commit
  group_commit_interval_us: 1000
  group_commit_max_bytes: 1048576

hot_consumer:
  read_batch_size: 128
  read_timeout_ms: 50

cold_consumer:
  quiet_threshold_seconds: 30
  safety_margin_seconds: 300
  jitter_fraction: 0.25
  buffer_high_water_bytes: 536870912
  buffer_low_water_bytes: 400000000
  max_flush_batch_size: 10000
  queue_read_max_count: 2048
  queue_read_timeout_ms: 25
  retry_initial_backoff_ms: 100
  retry_max_backoff_ms: 5000
  checkpoint_max_flushes: 64
  checkpoint_min_interval_ms: 75
  loop_initial_backoff_ms: 2
  loop_max_backoff_ms: 500
  drain_grace_seconds: 20

recovery:
  replay_parallelism: 4
  hot_replay_batch_size: 10000
  cold_replay_batch_size: 50000

net:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300
  io_threads: 4
  accept_queue: 256
  max_read_buffer_bytes: 67108864
  write_backpressure_bytes: 4194304
  write_resume_bytes: 1048576
  write_hard_limit_bytes: 16777216
  shutdown_grace_seconds: 30
  reaper_tick_ms: 1000

metrics:
  bind: 0.0.0.0
  port: 9090

admin:
  enabled: false
  bind: 0.0.0.0
  port: 8080
)YAML";

TEST(ConfigParse, ParsesFullDocumentFaithfully) {
  auto cfg = Config::ParseFromYaml(kFullYaml);
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();

  EXPECT_EQ(cfg->profile, "embedded");

  EXPECT_EQ(cfg->hot.backend, "builtin_hashmap");
  EXPECT_EQ(cfg->hot.max_memory_bytes, 4294967296U);
  EXPECT_EQ(cfg->hot.default_eviction, std::chrono::seconds{86400});
  ASSERT_EQ(cfg->hot.eviction_overrides.size(), 2U);
  EXPECT_EQ(cfg->hot.eviction_overrides[0].prefix, "session:");
  EXPECT_EQ(cfg->hot.eviction_overrides[0].eviction, std::chrono::seconds{3600});
  EXPECT_EQ(cfg->hot.eviction_overrides[1].prefix, "ephemeral:");
  EXPECT_EQ(cfg->hot.eviction_overrides[1].eviction, std::chrono::seconds{300});

  EXPECT_EQ(cfg->cold.backend, "builtin_rocksdb");
  EXPECT_EQ(cfg->cold.data_path, "/data/cold");
  EXPECT_EQ(cfg->cold.write_buffer_size_bytes, 67108864U);

  EXPECT_EQ(cfg->queue.backend, "builtin_wal");
  EXPECT_EQ(cfg->queue.wal_path, "/data/wal");
  EXPECT_EQ(cfg->queue.segment_size_bytes, 134217728U);
  EXPECT_EQ(cfg->queue.min_retention, std::chrono::seconds{86400});
  EXPECT_EQ(cfg->queue.offset_fsync_interval, std::chrono::milliseconds{250});
  EXPECT_EQ(cfg->queue.fsync_policy, "group_commit");
  EXPECT_EQ(cfg->queue.group_commit_interval_us, 1000U);
  EXPECT_EQ(cfg->queue.group_commit_max_bytes, 1048576U);

  EXPECT_EQ(cfg->hot_consumer.read_batch_size, 128U);
  EXPECT_EQ(cfg->hot_consumer.read_timeout, std::chrono::milliseconds{50});

  EXPECT_EQ(cfg->cold_consumer.quiet_threshold, std::chrono::seconds{30});
  EXPECT_EQ(cfg->cold_consumer.safety_margin, std::chrono::seconds{300});
  EXPECT_DOUBLE_EQ(cfg->cold_consumer.jitter_fraction, 0.25);
  EXPECT_EQ(cfg->cold_consumer.buffer_high_water_bytes, 536870912U);
  EXPECT_EQ(cfg->cold_consumer.buffer_low_water_bytes, 400000000U);
  EXPECT_EQ(cfg->cold_consumer.max_flush_batch_size, 10000U);
  EXPECT_EQ(cfg->cold_consumer.queue_read_max_count, 2048U);
  EXPECT_EQ(cfg->cold_consumer.queue_read_timeout, std::chrono::milliseconds{25});
  EXPECT_EQ(cfg->cold_consumer.retry_initial_backoff, std::chrono::milliseconds{100});
  EXPECT_EQ(cfg->cold_consumer.retry_max_backoff, std::chrono::milliseconds{5000});
  EXPECT_EQ(cfg->cold_consumer.checkpoint_max_flushes, 64U);
  EXPECT_EQ(cfg->cold_consumer.checkpoint_min_interval, std::chrono::milliseconds{75});
  EXPECT_EQ(cfg->cold_consumer.loop_initial_backoff, std::chrono::milliseconds{2});
  EXPECT_EQ(cfg->cold_consumer.loop_max_backoff, std::chrono::milliseconds{500});
  EXPECT_EQ(cfg->cold_consumer.drain_grace, std::chrono::seconds{20});

  EXPECT_EQ(cfg->recovery.replay_parallelism, 4U);
  EXPECT_EQ(cfg->recovery.hot_replay_batch_size, 10000U);
  EXPECT_EQ(cfg->recovery.cold_replay_batch_size, 50000U);

  EXPECT_EQ(cfg->net.bind, "0.0.0.0");
  EXPECT_EQ(cfg->net.port, 6379);
  EXPECT_EQ(cfg->net.max_connections, 1024U);
  EXPECT_EQ(cfg->net.idle_timeout, std::chrono::seconds{300});
  EXPECT_EQ(cfg->net.io_threads, 4U);
  EXPECT_EQ(cfg->net.accept_queue, 256U);
  EXPECT_EQ(cfg->net.max_read_buffer_bytes, 67108864U);
  EXPECT_EQ(cfg->net.write_backpressure_bytes, 4194304U);
  EXPECT_EQ(cfg->net.write_resume_bytes, 1048576U);
  EXPECT_EQ(cfg->net.write_hard_limit_bytes, 16777216U);
  EXPECT_EQ(cfg->net.shutdown_grace, std::chrono::seconds{30});
  EXPECT_EQ(cfg->net.reaper_tick, std::chrono::milliseconds{1000});

  EXPECT_EQ(cfg->metrics.bind, "0.0.0.0");
  EXPECT_EQ(cfg->metrics.port, 9090);
  EXPECT_FALSE(cfg->admin.enabled);
  EXPECT_EQ(cfg->admin.bind, "0.0.0.0");
  EXPECT_EQ(cfg->admin.port, 8080);
}

TEST(ConfigParse, MissingSectionsUseDefaults) {
  auto cfg = Config::ParseFromYaml("profile: embedded\n");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  const Config defaults = Config::Defaults();
  EXPECT_EQ(cfg->hot.max_memory_bytes, defaults.hot.max_memory_bytes);
  EXPECT_EQ(cfg->queue.fsync_policy, defaults.queue.fsync_policy);
  EXPECT_EQ(cfg->recovery.replay_parallelism, defaults.recovery.replay_parallelism);
}

TEST(ConfigParse, PartialSectionUsesDefaultsForOmittedFields) {
  auto cfg = Config::ParseFromYaml(R"YAML(
net:
  port: 6400
)YAML");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->net.port, 6400);
  EXPECT_EQ(cfg->net.bind, Config::Defaults().net.bind);
  EXPECT_EQ(cfg->net.max_connections, Config::Defaults().net.max_connections);
}

TEST(ConfigParse, YamlParseErrorProducesActionableMessage) {
  auto cfg = Config::ParseFromYaml("hot: {unterminated");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("YAML parse error"), std::string::npos);
}

TEST(ConfigParse, NonMapTopLevelFails) {
  auto cfg = Config::ParseFromYaml("- just a list\n- of values\n");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("mapping"), std::string::npos);
}

}  // namespace
}  // namespace abyss::config
