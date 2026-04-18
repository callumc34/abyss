#include "abyss/core/eviction_policy.h"

#include <gtest/gtest.h>

#include <chrono>

namespace abyss::core {
namespace {

using namespace std::chrono_literals;

TEST(EvictionPolicyTest, DefaultConstructedHasBuiltinDefault) {
  const EvictionPolicy policy;
  EXPECT_EQ(policy.Resolve("anything"), EvictionTTL{86400});
}

TEST(EvictionPolicyTest, DefaultEvictionAppliesWhenNoOverrides) {
  EvictionPolicy policy{EvictionTTL{3600}};
  EXPECT_EQ(policy.Resolve("user:42"), 3600s);
}

TEST(EvictionPolicyTest, PrefixOverrideApplies) {
  EvictionPolicy policy{
      EvictionTTL{3600},
      {{.prefix = "session:", .eviction = EvictionTTL{300}}},
  };
  EXPECT_EQ(policy.Resolve("session:abc"), 300s);
  EXPECT_EQ(policy.Resolve("user:abc"), 3600s);
}

TEST(EvictionPolicyTest, LongestPrefixWins) {
  EvictionPolicy policy{
      EvictionTTL{3600},
      {
          {.prefix = "cache:", .eviction = EvictionTTL{600}},
          {.prefix = "cache:hot:", .eviction = EvictionTTL{60}},
      },
  };
  EXPECT_EQ(policy.Resolve("cache:hot:xyz"), 60s);
  EXPECT_EQ(policy.Resolve("cache:cold:xyz"), 600s);
  EXPECT_EQ(policy.Resolve("other:xyz"), 3600s);
}

TEST(EvictionPolicyTest, EmptyKeyResolvesToDefault) {
  EvictionPolicy policy{EvictionTTL{3600}};
  EXPECT_EQ(policy.Resolve(""), 3600s);
}

TEST(EvictionPolicyTest, KeyEqualsPrefixMatches) {
  EvictionPolicy policy{
      EvictionTTL{3600},
      {{.prefix = "session", .eviction = EvictionTTL{300}}},
  };
  EXPECT_EQ(policy.Resolve("session"), 300s);
}

TEST(EvictionPolicyTest, OrderOfInsertionDoesNotMatter) {
  EvictionPolicy a{
      EvictionTTL{3600},
      {
          {.prefix = "a:", .eviction = EvictionTTL{10}},
          {.prefix = "a:b:", .eviction = EvictionTTL{20}},
      },
  };
  EvictionPolicy b{
      EvictionTTL{3600},
      {
          {.prefix = "a:b:", .eviction = EvictionTTL{20}},
          {.prefix = "a:", .eviction = EvictionTTL{10}},
      },
  };
  EXPECT_EQ(a.Resolve("a:b:x"), b.Resolve("a:b:x"));
  EXPECT_EQ(a.Resolve("a:x"), b.Resolve("a:x"));
}

}  // namespace
}  // namespace abyss::core
