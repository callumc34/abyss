#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>

#include "abyss/config/config.h"
#include "abyss/core/durability.h"

namespace abyss::config {
namespace {

TEST(ConfigDefaults, MatchesDefaultConstructedValues) {
  const Config defaults = Config::Defaults();
  EXPECT_EQ(defaults.profile, "embedded");

  EXPECT_EQ(defaults.hot.backend, "builtin_hashmap");
  EXPECT_GT(defaults.hot.max_memory_bytes, 0U);
  EXPECT_GT(defaults.hot.default_eviction.count(), 0);
  EXPECT_GT(defaults.hot.eviction_tick.count(), 0);
  EXPECT_TRUE(defaults.hot.eviction_overrides.empty());

  EXPECT_EQ(defaults.cold.backend, "builtin_rocksdb");
  EXPECT_FALSE(defaults.cold.data_path.empty());

  EXPECT_EQ(defaults.queue.backend, "builtin_wal");
  EXPECT_EQ(defaults.queue.durability, core::Durability::kProcessCrash);
  EXPECT_EQ(defaults.queue.durability_window_bytes, uint64_t{64} * 1024 * 1024);
  EXPECT_EQ(defaults.queue.durability_window, std::chrono::milliseconds{1000});
  EXPECT_EQ(defaults.queue.offset_fsync_interval, std::chrono::milliseconds{1000});

  EXPECT_GT(defaults.hot_consumer.read_batch_size, 0U);
  EXPECT_GT(defaults.hot_consumer.read_timeout.count(), 0);

  EXPECT_GT(defaults.cold_consumer.quiet_threshold.count(), 0);
  // ADP-004 §Flush Machinery Design Decisions §3: default jitter_fraction = 0.1.
  // Regression guard: C1 fixed a bug where the default was 0.5 (5× the
  // documented value) due to a config-field-name mismatch.
  EXPECT_DOUBLE_EQ(defaults.cold_consumer.jitter_fraction, 0.1);
  EXPECT_GT(defaults.cold_consumer.buffer_high_water_bytes, 0U);
  EXPECT_EQ(defaults.cold_consumer.buffer_low_water_bytes, 0U);  // auto
  EXPECT_GT(defaults.cold_consumer.queue_read_max_count, 0U);
  EXPECT_GT(defaults.cold_consumer.queue_read_timeout.count(), 0);
  EXPECT_GE(defaults.cold_consumer.retry_initial_backoff.count(), 0);
  EXPECT_GE(defaults.cold_consumer.retry_max_backoff.count(),
            defaults.cold_consumer.retry_initial_backoff.count());
  // Must mirror ColdConsumer::Config so an operator who omits these knobs gets
  // the same cadence the consumer struct-defaults to.
  EXPECT_EQ(defaults.cold_consumer.checkpoint_max_flushes, 32U);
  EXPECT_EQ(defaults.cold_consumer.checkpoint_min_interval, std::chrono::milliseconds{50});
  EXPECT_EQ(defaults.cold_consumer.loop_initial_backoff, std::chrono::milliseconds{1});
  EXPECT_EQ(defaults.cold_consumer.loop_max_backoff, std::chrono::milliseconds{1000});

  EXPECT_GT(defaults.recovery.replay_parallelism, 0U);
  EXPECT_GT(defaults.recovery.hot_replay_batch_size, 0U);
  EXPECT_GT(defaults.recovery.cold_replay_batch_size, 0U);

  EXPECT_EQ(defaults.net.port, 6379);
  EXPECT_EQ(defaults.metrics.port, 9090);
  EXPECT_EQ(defaults.admin.port, 8080);
}

TEST(ConfigDefaults, ValidateAcceptsDefaults) {
  auto result = Config::Defaults().Validate();
  ASSERT_TRUE(result.has_value()) << result.error().message();
}

TEST(ConfigDefaults, EmptyYamlEqualsDefaults) {
  auto cfg = Config::ParseFromYaml("");
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  const Config defaults = Config::Defaults();
  EXPECT_EQ(cfg->profile, defaults.profile);
  EXPECT_EQ(cfg->net.port, defaults.net.port);
  EXPECT_EQ(cfg->queue.durability, defaults.queue.durability);
}

}  // namespace
}  // namespace abyss::config
