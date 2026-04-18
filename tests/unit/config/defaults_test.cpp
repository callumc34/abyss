#include <gtest/gtest.h>

#include "abyss/config/config.h"

namespace abyss::config {
namespace {

TEST(ConfigDefaults, MatchesDefaultConstructedValues) {
  const Config defaults = Config::Defaults();
  EXPECT_EQ(defaults.profile, "embedded");

  EXPECT_EQ(defaults.hot.backend, "builtin_hashmap");
  EXPECT_GT(defaults.hot.max_memory_bytes, 0U);
  EXPECT_GT(defaults.hot.default_eviction.count(), 0);
  EXPECT_TRUE(defaults.hot.eviction_overrides.empty());

  EXPECT_EQ(defaults.cold.backend, "builtin_rocksdb");
  EXPECT_FALSE(defaults.cold.data_path.empty());

  EXPECT_EQ(defaults.queue.backend, "builtin_wal");
  EXPECT_EQ(defaults.queue.fsync_policy, "group_commit");
  EXPECT_GT(defaults.queue.group_commit_interval_us, 0U);

  EXPECT_GT(defaults.cold_consumer.quiet_threshold.count(), 0);
  EXPECT_GE(defaults.cold_consumer.deadline_jitter_ratio, 0.0);
  EXPECT_LE(defaults.cold_consumer.deadline_jitter_ratio, 1.0);

  EXPECT_GT(defaults.recovery.replay_parallelism, 0U);
  EXPECT_GT(defaults.recovery.hot_replay_batch_size, 0U);
  EXPECT_GT(defaults.recovery.cold_replay_batch_size, 0U);

  EXPECT_EQ(defaults.resp.port, 6379);
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
  EXPECT_EQ(cfg->resp.port, defaults.resp.port);
  EXPECT_EQ(cfg->queue.fsync_policy, defaults.queue.fsync_policy);
}

}  // namespace
}  // namespace abyss::config
