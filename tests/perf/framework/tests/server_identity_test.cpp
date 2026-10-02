#include "server_identity.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>

namespace abyss::perf {
namespace {

constexpr const char* kRedisInfo =
    "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\nos:Linux\r\n";
constexpr const char* kValkeyInfo =
    "# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:8.0.1\r\n";
constexpr const char* kDragonflyInfo =
    "# Server\r\nredis_version:7.4.0\r\ndragonfly_version:df-v1.21.2\r\n";

TEST(ServerIdentityTest, InfoFieldMatchesWholeKeyOnly) {
  const std::string info = "redis_version_extra:x\r\nredis_version:7.2.4\r\n";
  EXPECT_EQ(InfoField(info, "redis_version"), "7.2.4");
  EXPECT_EQ(InfoField("valkey_version:8.0.1", "valkey_version"), "8.0.1");
  EXPECT_FALSE(InfoField(info, "valkey_version").has_value());
  EXPECT_FALSE(InfoField("", "redis_version").has_value());
}

TEST(ServerIdentityTest, HelloNamingTheServerIsTrusted) {
  const std::optional<HelloFields> hello = HelloFields{.server = "abyss", .version = "0.1.0"};
  EXPECT_FALSE(NeedsInfo(hello));
  const auto id = ResolveServerIdentity(hello, "");
  EXPECT_EQ(id.kind, "abyss");
  EXPECT_EQ(id.version, "0.1.0");
}

TEST(ServerIdentityTest, RedisHelloIsDisambiguatedByInfo) {
  const std::optional<HelloFields> hello = HelloFields{.server = "redis", .version = "7.2.4"};
  ASSERT_TRUE(NeedsInfo(hello));

  const auto redis = ResolveServerIdentity(hello, kRedisInfo);
  EXPECT_EQ(redis.kind, "redis");
  EXPECT_EQ(redis.version, "7.2.4");

  const auto valkey = ResolveServerIdentity(hello, kValkeyInfo);
  EXPECT_EQ(valkey.kind, "valkey");
  EXPECT_EQ(valkey.version, "8.0.1");

  const auto dragonfly = ResolveServerIdentity(hello, kDragonflyInfo);
  EXPECT_EQ(dragonfly.kind, "dragonfly");
  EXPECT_EQ(dragonfly.version, "df-v1.21.2");
}

TEST(ServerIdentityTest, MissingHelloFallsBackToInfo) {
  ASSERT_TRUE(NeedsInfo(std::nullopt));
  const auto id = ResolveServerIdentity(std::nullopt, kRedisInfo);
  EXPECT_EQ(id.kind, "redis");
  EXPECT_EQ(id.version, "7.2.4");
}

TEST(ServerIdentityTest, NoEvidenceIsUnknown) {
  const auto id = ResolveServerIdentity(std::nullopt, "");
  EXPECT_EQ(id.kind, "unknown");
  EXPECT_TRUE(id.version.empty());
}

}  // namespace
}  // namespace abyss::perf
