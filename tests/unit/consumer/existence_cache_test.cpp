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

// HOTC-4: keys/members are binary-safe and may contain 0x1F. The old single
// 0x1F separator made (key="a", member="b\x1Fc") collide with
// (key="a\x1Fb", member="c"). Length-prefixed encoding must keep them distinct.
TEST(ExistenceCacheTest, MemberKeyNoCollisionWithSeparatorInData) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  const std::string member_with_sep = std::string("b\x1F") + "c";
  const std::string key_with_sep = std::string("a\x1F") + "b";

  cache.UpsertMember("a", member_with_sep,
                     ExistenceCache::MemberMeta{.score = 1.0, .latest_seq = 1});

  // The colliding-under-old-scheme (key,member) pair must be a miss.
  EXPECT_FALSE(cache.GetMember(key_with_sep, "c").has_value());
  // The exact pair we inserted resolves correctly.
  auto got = cache.GetMember("a", member_with_sep);
  ASSERT_TRUE(got.has_value());
  EXPECT_DOUBLE_EQ(got->score, 1.0);

  // Same disambiguation for fields.
  const std::string field_with_sep = std::string("f\x1F") + "g";
  const std::string key_with_f = std::string("h\x1F") + "f";
  cache.UpsertField("h", field_with_sep,
                    ExistenceCache::FieldMeta{.value = "v", .value_known = true, .latest_seq = 2});
  EXPECT_FALSE(cache.GetField(key_with_f, "g").has_value());
  EXPECT_TRUE(cache.GetField("h", field_with_sep).has_value());
}

// HOTC-3 ⟷ HOTC-4: RemoveMember must address exactly the (key,member) it was
// given and never the byte-prefix-sharing neighbour.
TEST(ExistenceCacheTest, RemoveMemberAddressesExactEntryUnderBinarySafeKeys) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  const std::string member_with_sep = std::string("b\x1F") + "c";
  const std::string key_with_sep = std::string("a\x1F") + "b";

  cache.UpsertMember("a", member_with_sep,
                     ExistenceCache::MemberMeta{.score = 1.0, .latest_seq = 1});
  cache.UpsertMember(key_with_sep, "c", ExistenceCache::MemberMeta{.score = 2.0, .latest_seq = 2});

  // Removing the first must leave the second untouched.
  cache.RemoveMember("a", member_with_sep);
  EXPECT_FALSE(cache.GetMember("a", member_with_sep).has_value());
  auto other = cache.GetMember(key_with_sep, "c");
  ASSERT_TRUE(other.has_value());
  EXPECT_DOUBLE_EQ(other->score, 2.0);
}

// HOTC-3: deleting a key purges its members and fields but not its peers'.
TEST(ExistenceCacheTest, RemoveMembersAndFieldsScopedToKey) {
  FakeClock clock;
  ExistenceCache cache(DefaultConfig(), clock.Fn());
  cache.UpsertMember("s", "m1", ExistenceCache::MemberMeta{.score = 1.0, .latest_seq = 1});
  cache.UpsertMember("s", "m2", ExistenceCache::MemberMeta{.score = 2.0, .latest_seq = 2});
  cache.UpsertField("s", "f1",
                    ExistenceCache::FieldMeta{.value = "v", .value_known = true, .latest_seq = 3});
  cache.UpsertMember("other", "m1", ExistenceCache::MemberMeta{.score = 9.0, .latest_seq = 4});

  cache.RemoveMembersAndFields("s");

  EXPECT_FALSE(cache.GetMember("s", "m1").has_value());
  EXPECT_FALSE(cache.GetMember("s", "m2").has_value());
  EXPECT_FALSE(cache.GetField("s", "f1").has_value());
  EXPECT_TRUE(cache.GetMember("other", "m1").has_value());
}

}  // namespace
}  // namespace abyss::consumer
