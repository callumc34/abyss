#include "abyss/consumer/existence_cache.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>

namespace abyss::consumer {
namespace {

class FakeClock {
 public:
  core::SteadyTime Now() { return now_; }
  void Advance(std::chrono::milliseconds d) { now_ += d; }
  core::SteadyClockFn Fn() {
    return [this] { return now_; };
  }

 private:
  core::SteadyTime now_{};
};

ExistenceCache::Config DefaultConfig() {
  return ExistenceCache::Config{.entry_ttl = std::chrono::seconds{60},
                                .max_entries = 1000,
                                .max_bytes = 1ULL << 20,
                                .max_string_value_bytes = 64};
}

TEST(ExistenceCacheTest, GetMissReturnsNullopt) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  EXPECT_FALSE(cache.GetKey("missing").has_value());
}

TEST(ExistenceCacheTest, UpsertAndGetKey) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.UpsertKey("k", ExistenceCache::KeyMeta{
                           .exists = true,
                           .type = ExistenceCache::KeyType::kString,
                           .abs_ttl_ms = 1000,
                           .latest_seq = 7,
                           .string_value = std::string("v"),
                       });
  auto got = cache.GetKey("k");
  ASSERT_TRUE(got.has_value());
  EXPECT_TRUE(got->exists);
  EXPECT_EQ(got->latest_seq, 7U);
  EXPECT_EQ(got->abs_ttl_ms, 1000U);
  ASSERT_TRUE(got->string_value.has_value());
  EXPECT_EQ(*got->string_value, "v");
}

TEST(ExistenceCacheTest, TombstoneKey) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.UpsertKey("k",
                  ExistenceCache::KeyMeta{
                      .exists = true, .type = ExistenceCache::KeyType::kString, .latest_seq = 1});
  cache.TombstoneKey("k", 5);
  auto got = cache.GetKey("k");
  ASSERT_TRUE(got.has_value());
  EXPECT_FALSE(got->exists);
  EXPECT_EQ(got->latest_seq, 5U);
}

TEST(ExistenceCacheTest, MemberRoundTrip) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.UpsertMember("zk", "alice", ExistenceCache::MemberMeta{.score = 12.5, .latest_seq = 3});
  auto got = cache.GetMember("zk", "alice");
  ASSERT_TRUE(got.has_value());
  EXPECT_DOUBLE_EQ(got->score, 12.5);
  EXPECT_FALSE(cache.GetMember("zk", "bob").has_value());
}

TEST(ExistenceCacheTest, FieldRoundTrip) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.UpsertField("hk", "f1",
                    ExistenceCache::FieldMeta{.value = "v1", .value_known = true, .latest_seq = 9});
  auto got = cache.GetField("hk", "f1");
  ASSERT_TRUE(got.has_value());
  EXPECT_TRUE(got->value_known);
  EXPECT_EQ(got->value, "v1");
}

TEST(ExistenceCacheTest, EnforcesEntryCap) {
  auto config = DefaultConfig();
  config.max_entries = 2;
  FakeClock clock;
  ExistenceCache cache(config, clock.Fn());
  cache.UpsertKey("a", ExistenceCache::KeyMeta{.exists = true, .latest_seq = 1});
  cache.UpsertKey("b", ExistenceCache::KeyMeta{.exists = true, .latest_seq = 2});
  cache.UpsertKey("c", ExistenceCache::KeyMeta{.exists = true, .latest_seq = 3});
  // 'a' was the LRU and should have been evicted.
  EXPECT_FALSE(cache.GetKey("a").has_value());
  EXPECT_TRUE(cache.GetKey("b").has_value());
  EXPECT_TRUE(cache.GetKey("c").has_value());
  EXPECT_EQ(cache.Size(), 2U);
  EXPECT_GE(cache.EvictionsCapacity(), 1U);
}

TEST(ExistenceCacheTest, SweepsExpired) {
  auto config = DefaultConfig();
  config.entry_ttl = std::chrono::seconds{1};
  FakeClock clock;
  ExistenceCache cache(config, clock.Fn());
  cache.UpsertKey("old", ExistenceCache::KeyMeta{.exists = true, .latest_seq = 1});
  clock.Advance(std::chrono::milliseconds{2000});
  cache.UpsertKey("fresh", ExistenceCache::KeyMeta{.exists = true, .latest_seq = 2});

  const auto swept = cache.SweepExpired();
  EXPECT_GE(swept, 1U);
  EXPECT_FALSE(cache.GetKey("old").has_value());
  EXPECT_TRUE(cache.GetKey("fresh").has_value());
  EXPECT_GE(cache.EvictionsTtl(), 1U);
}

TEST(ExistenceCacheTest, LargeStringValueDropsSnapshot) {
  auto config = DefaultConfig();
  config.max_string_value_bytes = 4;
  FakeClock clock;
  ExistenceCache cache(config, clock.Fn());
  cache.UpsertKey("k", ExistenceCache::KeyMeta{.exists = true,
                                               .type = ExistenceCache::KeyType::kString,
                                               .latest_seq = 1,
                                               .string_value = std::string("hello-too-long")});
  auto got = cache.GetKey("k");
  ASSERT_TRUE(got.has_value());
  EXPECT_TRUE(got->exists);
  EXPECT_FALSE(got->string_value.has_value());
}

TEST(ExistenceCacheTest, ClearDropsAllEntriesAcrossKinds) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.UpsertKey("k1", ExistenceCache::KeyMeta{.exists = true,
                                                .type = ExistenceCache::KeyType::kString,
                                                .latest_seq = 1,
                                                .string_value = std::string("v")});
  cache.UpsertMember("zset", "m", ExistenceCache::MemberMeta{.score = 1.5, .latest_seq = 2});
  cache.UpsertField(
      "hash", "f",
      ExistenceCache::FieldMeta{.value = std::string("v"), .value_known = true, .latest_seq = 3});
  ASSERT_GT(cache.Size(), 0U);
  ASSERT_GT(cache.BytesEstimate(), 0U);

  cache.Clear();

  EXPECT_EQ(cache.Size(), 0U);
  EXPECT_EQ(cache.BytesEstimate(), 0U);
  EXPECT_FALSE(cache.GetKey("k1").has_value());
  EXPECT_FALSE(cache.GetMember("zset", "m").has_value());
  EXPECT_FALSE(cache.GetField("hash", "f").has_value());
}

TEST(ExistenceCacheTest, ClearOnEmptyIsNoOp) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.Clear();
  EXPECT_EQ(cache.Size(), 0U);
}

}  // namespace
}  // namespace abyss::consumer
