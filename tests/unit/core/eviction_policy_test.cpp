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

TEST(EvictionPolicyValidate, AcceptsWellFormedInput) {
  std::vector<EvictionRule> overrides{
      {.prefix = "session:", .eviction = EvictionTTL{300}},
      {.prefix = "ephemeral:", .eviction = EvictionTTL{60}},
  };
  auto r = EvictionPolicy::Validate(EvictionTTL{3600}, overrides, "hot");
  EXPECT_TRUE(r.has_value()) << (r.has_value() ? "" : r.error().message());
}

TEST(EvictionPolicyValidate, RejectsNonPositiveDefault) {
  auto r = EvictionPolicy::Validate(EvictionTTL{0}, {}, "hot");
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("hot.default_eviction_seconds"), std::string::npos);
}

TEST(EvictionPolicyValidate, RejectsEmptyPrefix) {
  std::vector<EvictionRule> overrides{{.prefix = "", .eviction = EvictionTTL{60}}};
  auto r = EvictionPolicy::Validate(EvictionTTL{3600}, overrides, "hot");
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("prefix"), std::string::npos);
}

TEST(EvictionPolicyValidate, RejectsNonPositiveOverrideEviction) {
  std::vector<EvictionRule> overrides{{.prefix = "x:", .eviction = EvictionTTL{0}}};
  auto r = EvictionPolicy::Validate(EvictionTTL{3600}, overrides, "hot");
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("eviction_seconds"), std::string::npos);
}

TEST(EvictionPolicyValidate, RejectsDuplicatePrefix) {
  std::vector<EvictionRule> overrides{
      {.prefix = "session:", .eviction = EvictionTTL{60}},
      {.prefix = "session:", .eviction = EvictionTTL{120}},
  };
  auto r = EvictionPolicy::Validate(EvictionTTL{3600}, overrides, "hot");
  ASSERT_FALSE(r.has_value());
  EXPECT_NE(r.error().message().find("duplicate prefix"), std::string::npos);
}

TEST(EvictionPolicyValidate, MaxConfiguredTtlReportsMax) {
  std::vector<EvictionRule> overrides{
      {.prefix = "short:", .eviction = EvictionTTL{60}},
      {.prefix = "long:", .eviction = EvictionTTL{604800}},
  };
  EXPECT_EQ(EvictionPolicy::MaxConfiguredTtl(EvictionTTL{3600}, overrides), EvictionTTL{604800});
}

TEST(EvictionPolicyValidate, MaxConfiguredTtlReturnsDefaultWhenNoOverrides) {
  EXPECT_EQ(EvictionPolicy::MaxConfiguredTtl(EvictionTTL{42}, {}), EvictionTTL{42});
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
